"""
shared/utils/recommendations.py

Trade ideas from what the platform has measured: the cross-sectional model's
ranks at each horizon for stocks, and for each stock idea a defined option
structure chosen from the live chain.

Pure: inputs in, ideas out. The service (services/agents/recommendation_engine.py)
gathers the inputs and logs every idea so the ideas themselves can be graded.

What decides what:
- Direction and horizon come from the model's ranks. A horizon whose model has
  not passed the validation gate still produces ideas, marked unvalidated.
- The option structure comes from implied against realised volatility. Options
  priced below the stock's own recent movement are bought outright; fairly
  priced, a debit spread caps the cost; rich, a defined-risk credit spread
  sells the premium instead. Goyal & Saretto (2009): sorting stocks on realised
  minus at-the-money implied volatility, options where implied is far above
  realised underperform and those far below outperform, robust across market
  conditions and option liquidity -- which is why rich premium is sold rather
  than bought.
- Contracts are real: expiry inside the horizon's window, strikes by delta,
  priced at the quote midpoint, refused when the quote is too wide to trade.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from shared.utils.bar_feed import DAILY_BAR_VOLUME_SHARE

# Days to expiry by horizon: long enough that the horizon fits inside it.
EXPIRY_WINDOWS: Dict[str, Tuple[int, int]] = {
    "daily": (5, 16),
    "weekly": (12, 35),
    "monthly": (30, 70),
}
IV_CHEAP = 1.05          # implied / realised below this: options cheap against movement
IV_RICH = 1.35           # above this: premium rich
MAX_REL_SPREAD = 0.25    # (ask - bid) / mid above this is not a tradable quote
STOP_ATR = 2.0
MIN_STOP_PCT = 0.02
# The stop widens with the hold (22 Oct 2026). Two daily ATRs is about 2.8 daily
# standard deviations; over 21 sessions a stock's ordinary range is sqrt(21) =
# 4.6 of them, so a monthly idea's stop sat inside one month's normal swing and
# a random walk touches it about half the time -- the ledger stopped 113 of 167
# monthly ideas (68%) and 98 of 221 weekly ones, cutting the model's edge off
# before its horizon. HORIZON_STOP_K x ATR x sqrt(days) sets the touch chance
# near one in ten for a driftless walk (ATR ~ 1.4 daily standard deviations,
# 1.645 sigma over the hold); daily ideas keep their two ATRs.
HORIZON_STOP_K = 1.2


@dataclass(frozen=True)
class Leg:
    symbol: str
    right: str            # "C" | "P"
    side: str             # "buy" | "sell"
    strike: float
    expiry: str
    delta: float
    iv: Optional[float]
    mid: float


def realized_vol(closes: Sequence[float], window: int = 21) -> Optional[float]:
    """Annualised close-to-close volatility over the last `window` returns."""
    c = [float(x) for x in closes if x and x > 0]
    if len(c) < window + 1:
        return None
    rets = [math.log(c[i] / c[i - 1]) for i in range(len(c) - window, len(c))]
    mu = sum(rets) / len(rets)
    var = sum((r - mu) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var * 252)


def atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], n: int = 14) -> Optional[float]:
    if len(closes) < n + 1:
        return None
    trs = []
    for i in range(len(closes) - n, len(closes)):
        h, lo, pc = float(highs[i]), float(lows[i]), float(closes[i - 1])
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    return sum(trs) / n


def iv_regime(atm_iv: Optional[float], rv: Optional[float]) -> str:
    if not atm_iv or not rv or rv <= 0:
        return "unknown"
    ratio = atm_iv / rv
    if ratio < IV_CHEAP:
        return "cheap"
    if ratio > IV_RICH:
        return "rich"
    return "fair"


def choose_structure(direction: int, regime: str) -> str:
    """Bullish +1 / bearish -1 and the vol regime -> the structure to use."""
    bull = direction > 0
    if regime == "cheap":
        return "long_call" if bull else "long_put"
    if regime == "rich":
        return "bull_put_credit" if bull else "bear_call_credit"
    return "bull_call_debit" if bull else "bear_put_debit"


# Target deltas per structure: (right, side, |delta|) for each leg.
_LEG_PLAN: Dict[str, List[Tuple[str, str, float]]] = {
    "long_call": [("C", "buy", 0.45)],
    "long_put": [("P", "buy", 0.45)],
    "bull_call_debit": [("C", "buy", 0.45), ("C", "sell", 0.25)],
    "bear_put_debit": [("P", "buy", 0.45), ("P", "sell", 0.25)],
    "bull_put_credit": [("P", "sell", 0.30), ("P", "buy", 0.15)],
    "bear_call_credit": [("C", "sell", 0.30), ("C", "buy", 0.15)],
}


def _num(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def tradable_chain(chain: Iterable[Tuple[Dict[str, Any], Dict[str, Any]]], today: date,
                   window: Tuple[int, int]) -> Dict[str, List[Leg]]:
    """(parsed OCC, snapshot) -> {expiry: [quoted legs with delta]} inside the window."""
    out: Dict[str, List[Leg]] = {}
    for parsed, snap in chain:
        if not parsed or not snap:
            continue
        exp = str(parsed.get("expiry") or "")
        try:
            dte = (date.fromisoformat(exp) - today).days
        except ValueError:
            continue
        if not window[0] <= dte <= window[1]:
            continue
        q = snap.get("latestQuote") or {}
        bid, ask = _num(q.get("bp")), _num(q.get("ap"))
        delta = _num((snap.get("greeks") or {}).get("delta"))
        strike = _num(parsed.get("strike"))
        if not bid or not ask or bid <= 0 or ask < bid or delta is None or strike is None:
            continue
        mid = (bid + ask) / 2
        if (ask - bid) / mid > MAX_REL_SPREAD:
            continue
        right = "P" if str(parsed.get("option_type", "")).upper().startswith("P") else "C"
        out.setdefault(exp, []).append(Leg(
            symbol=str(parsed.get("symbol") or ""), right=right, side="", strike=strike, expiry=exp,
            delta=delta, iv=_num(snap.get("impliedVolatility")), mid=round(mid, 4),
        ))
    return out


def quotes_by_expiry(chain: Iterable[Tuple[Dict[str, Any], Dict[str, Any]]], today: date,
                     window: Tuple[int, int]) -> Tuple[Dict[str, List[Any]], Dict[str, int]]:
    """({expiry: [option_ev.Quote]}, {expiry: days to expiry}) for tradable quotes
    inside the window: both sides quoted, spread no wider than MAX_REL_SPREAD."""
    from shared.utils.option_ev import Quote

    out: Dict[str, List[Any]] = {}
    dte: Dict[str, int] = {}
    for parsed, snap in chain:
        if not parsed or not snap:
            continue
        exp = str(parsed.get("expiry") or "")
        try:
            days = (date.fromisoformat(exp) - today).days
        except ValueError:
            continue
        if not window[0] <= days <= window[1]:
            continue
        q = snap.get("latestQuote") or {}
        bid, ask, strike = _num(q.get("bp")), _num(q.get("ap")), _num(parsed.get("strike"))
        if not bid or not ask or bid <= 0 or ask < bid or strike is None:
            continue
        if (ask - bid) / ((bid + ask) / 2) > MAX_REL_SPREAD:
            continue
        right = "P" if str(parsed.get("option_type", "")).upper().startswith("P") else "C"
        out.setdefault(exp, []).append(Quote(
            symbol=str(parsed.get("symbol") or ""), right=right, strike=strike, expiry=exp, bid=bid, ask=ask,
            delta=_num((snap.get("greeks") or {}).get("delta")), iv=_num(snap.get("impliedVolatility"))))
        dte[exp] = days
    return out, dte


def ev_option_idea(pick: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """An option_ev pick in the shape the trade-ideas page reads, with the
    forecast's expected value, probability and size beside it."""
    if not pick:
        return None
    legs = [{**lg, "mid": lg["price"], "delta": round(lg["delta"], 3) if lg.get("delta") is not None else None,
             "iv": round(lg["iv"], 4) if lg.get("iv") else None} for lg in pick["legs"]]
    return {"structure": pick["structure"], "expiry": pick["expiry"], "dte": pick["dte"], "legs": legs,
            "net": pick["net"], "max_loss": pick["max_loss"], "max_gain": pick["max_gain"],
            "breakeven": pick["breakeven"], "prob_profit_by_delta": pick["prob_profit"],
            "prob_profit": pick["prob_profit"], "ev": pick["ev"], "ev_per_risk": pick["ev_per_risk"],
            "kelly_share": pick["kelly_share"], "considered": pick.get("considered"), "priced_by": "forecast"}


def _closest(legs: List[Leg], right: str, target: float) -> Optional[Leg]:
    cands = [lg for lg in legs if lg.right == right]
    if not cands:
        return None
    return min(cands, key=lambda lg: abs(abs(lg.delta) - target))


def build_option_idea(structure: str, by_expiry: Dict[str, List[Leg]], today: date,
                      window: Tuple[int, int]) -> Optional[Dict[str, Any]]:
    """Real legs for the structure at the expiry nearest the window's middle,
    with cost, maximum loss and gain, breakeven, and probability by delta."""
    if not by_expiry:
        return None
    target_dte = (window[0] + window[1]) / 2
    for exp in sorted(by_expiry, key=lambda e: abs((date.fromisoformat(e) - today).days - target_dte)):
        legs: List[Leg] = []
        for right, side, d in _LEG_PLAN[structure]:
            lg = _closest(by_expiry[exp], right, d)
            if lg is None:
                break
            legs.append(Leg(**{**lg.__dict__, "side": side}))
        if len(legs) != len(_LEG_PLAN[structure]):
            continue
        if len(legs) == 2 and legs[0].strike == legs[1].strike:
            continue
        econ = _economics(structure, legs)
        if econ is None:
            continue
        return {
            "structure": structure,
            "expiry": exp,
            "dte": (date.fromisoformat(exp) - today).days,
            "legs": [{"symbol": lg.symbol, "right": lg.right, "side": lg.side, "strike": lg.strike,
                      "delta": round(lg.delta, 3), "iv": round(lg.iv, 4) if lg.iv else None, "mid": lg.mid}
                     for lg in legs],
            **econ,
        }
    return None


def _economics(structure: str, legs: List[Leg]) -> Optional[Dict[str, Any]]:
    """Per one contract (100 shares). None when the prices make no sense."""
    a = legs[0]
    if structure in ("long_call", "long_put"):
        debit = a.mid
        be = a.strike + debit if a.right == "C" else a.strike - debit
        return {"net": round(-debit * 100, 2), "max_loss": round(debit * 100, 2),
                "max_gain": None if a.right == "C" else round((a.strike - debit) * 100, 2),
                "breakeven": round(be, 2), "prob_profit_by_delta": round(max(0.0, abs(a.delta) - 0.05), 2)}
    b = legs[1]
    width = abs(a.strike - b.strike)
    if structure in ("bull_call_debit", "bear_put_debit"):
        debit = a.mid - b.mid
        if debit <= 0 or debit >= width:
            return None
        be = a.strike + debit if a.right == "C" else a.strike - debit
        return {"net": round(-debit * 100, 2), "max_loss": round(debit * 100, 2),
                "max_gain": round((width - debit) * 100, 2), "breakeven": round(be, 2),
                "prob_profit_by_delta": round(abs(a.delta) * 0.85, 2)}
    credit = a.mid - b.mid
    if credit <= 0 or credit >= width:
        return None
    be = a.strike - credit if a.right == "P" else a.strike + credit
    return {"net": round(credit * 100, 2), "max_loss": round((width - credit) * 100, 2),
            "max_gain": round(credit * 100, 2), "breakeven": round(be, 2),
            "prob_profit_by_delta": round(1 - abs(a.delta), 2)}


def stock_ideas(scores: Dict[str, Dict[str, Any]], n: int = 10,
                exclude: Iterable[str] = ("SPY", "QQQ", "IWM", "DIA"),
                industries: Optional[Dict[str, Optional[str]]] = None) -> List[Dict[str, Any]]:
    """The n strongest and n weakest ranks as long and short ideas.

    With `industries`, ranked within each industry as the headline book is
    (model_portfolio.industry_relative): every industry's best name comes
    before any industry's second, and names of the same tier by their rank
    across all names -- so no one sector fills a side. `industry_percentile` is
    the rank within the industry; `percentile` stays the rank across all names,
    which is what the forecast is calibrated on."""
    from shared.utils.model_portfolio import MIN_INDUSTRY_GROUP, industry_relative

    skip = {t.upper() for t in exclude}
    pool = {t: v for t, v in (scores or {}).items() if t.upper() not in skip}
    universe = {t: float(v.get("percentile", 0.5)) for t, v in pool.items()}

    def idea(t: str, v: Dict[str, Any], direction: int) -> Dict[str, Any]:
        out = {"ticker": t, "direction": direction, "percentile": v.get("percentile"), "quintile": v.get("quintile"),
               "drivers": v.get("drivers")}
        if industries is not None:
            out["industry_percentile"] = rel[t]
        return out

    if industries is None:
        rel: Dict[str, float] = {}
        ranked = sorted(pool, key=lambda t: universe[t], reverse=True)
        return [idea(t, pool[t], 1) for t in ranked[:n]] + [idea(t, pool[t], -1) for t in ranked[::-1][:n]]
    rel = industry_relative(universe, industries)
    # The groups industry_relative ranked within: small industries pooled.
    sizes: Dict[str, int] = {}
    for t in pool:
        sizes[industries.get(t) or ""] = sizes.get(industries.get(t) or "", 0) + 1
    group = {t: (industries.get(t) or "") if (industries.get(t) and sizes[industries.get(t) or ""] >= MIN_INDUSTRY_GROUP)
             else "" for t in pool}
    members: Dict[str, List[str]] = {}
    for t in pool:
        members.setdefault(group[t], []).append(t)
    top: Dict[str, int] = {}
    bottom: Dict[str, int] = {}
    for names in members.values():
        order = sorted(names, key=lambda t: (rel[t], universe[t]), reverse=True)
        for k, t in enumerate(order):
            top[t], bottom[t] = k, len(order) - 1 - k
    longs = sorted(pool, key=lambda t: (top[t], -universe[t]))[:n]
    shorts = sorted(pool, key=lambda t: (bottom[t], universe[t]))[:n]
    return [idea(t, pool[t], 1) for t in longs] + [idea(t, pool[t], -1) for t in shorts]


# Ideas on one side that move together are one bet. On 19 Oct 2026 the six
# monthly longs, six different SIC industries (semiconductors, plastics, printed
# circuit boards, instruments), were one theme: a mean pairwise correlation of
# 0.62 over 63 sessions, two independent bets of six, the theme that fell 4.1%
# in a day on 14 Oct. Each idea must stay under this correlation with every idea
# already kept on its side.
MAX_PAIR_CORRELATION = 0.6
CORRELATION_WINDOW = 63


def log_returns(closes: Sequence[float], window: int = CORRELATION_WINDOW) -> List[float]:
    c = [float(x) for x in closes[-(window + 1):] if x and x > 0]
    return [math.log(b / a) for a, b in zip(c, c[1:])]


def correlation(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    """Pearson correlation of two return series, aligned at their ends."""
    n = min(len(a), len(b))
    if n < 20:
        return None
    x, y = a[-n:], b[-n:]
    mx, my = sum(x) / n, sum(y) / n
    sxy = sum((p - mx) * (q - my) for p, q in zip(x, y))
    sx = math.sqrt(sum((p - mx) ** 2 for p in x))
    sy = math.sqrt(sum((q - my) ** 2 for q in y))
    return sxy / (sx * sy) if sx > 0 and sy > 0 else None


def effective_bets(series: Sequence[Sequence[float]]) -> Optional[float]:
    """How many independent bets a set of positions is: the participation
    ratio of their return correlation matrix's eigenvalues, (sum)^2 / sum of
    squares. n uncorrelated names give n; n names that move as one give 1."""
    import numpy as np

    rows = [list(s) for s in series if s and len(s) >= 20]
    if len(rows) < 2:
        return float(len(rows)) if rows else None
    n = min(len(r) for r in rows)
    x = np.array([r[-n:] for r in rows], dtype=float)
    keep = x.std(axis=1) > 0
    if keep.sum() < 2:
        return float(keep.sum())
    ev = np.linalg.eigvalsh(np.corrcoef(x[keep]))
    ev = np.clip(ev, 0.0, None)
    return round(float(ev.sum() ** 2 / (ev ** 2).sum()), 1)


def too_close(returns: Sequence[float], kept: Sequence[Sequence[float]],
              limit: float = MAX_PAIR_CORRELATION) -> bool:
    """Whether a candidate moves too much like any idea already kept."""
    for r in kept:
        c = correlation(returns, r)
        if c is not None and c > limit:
            return True
    return False


# Ideas must be tradable by a person, not only rankable by a model: the model's
# floor ($3, ~$3M a day) keeps the cross-section broad; an idea needs more.
IDEA_MIN_PRICE = 10.0
IDEA_MIN_CONSOLIDATED_DOLLAR_VOLUME = 20e6  # a day, every exchange
IDEA_MIN_FEED_DOLLAR_VOLUME = IDEA_MIN_CONSOLIDATED_DOLLAR_VOLUME * DAILY_BAR_VOLUME_SHARE
IDEA_MAX_REALIZED_VOL = 1.20


def idea_screen(direction: int, meta: Optional[Sequence[Any]], price: Optional[float],
                feed_dollar_volume: Optional[float], rv: Optional[float]) -> Optional[str]:
    """None when the idea is tradable; otherwise the reason it is not.
    meta: [kind, shortable, easy_to_borrow, name] from shared.utils.asset_universe."""
    if meta is not None and meta[0] != "stock":
        return f"not a common stock ({meta[0]})"
    if price is None or price < IDEA_MIN_PRICE:
        return f"price under ${IDEA_MIN_PRICE:.0f}"
    if feed_dollar_volume is None or feed_dollar_volume < IDEA_MIN_FEED_DOLLAR_VOLUME:
        return "too thinly traded"
    if rv is not None and rv > IDEA_MAX_REALIZED_VOL:
        return f"realised volatility {rv:.0%}"
    if direction < 0 and meta is not None and not (meta[1] and meta[2]):
        return "not easy to borrow for a short"
    return None


def stock_plan(direction: int, price: float, atr_value: Optional[float], horizon_days: int,
               expected_excess_bps: Optional[float]) -> Dict[str, Any]:
    """Entry, a stop that widens with the hold, exit at the horizon."""
    stop = None
    if atr_value and atr_value > 0:
        # At least MIN_STOP_PCT away: a pinned name (a pending acquisition)
        # has a daily range near zero, and a stop inside the spread is noise.
        dist = max(STOP_ATR * atr_value, HORIZON_STOP_K * atr_value * math.sqrt(max(1, horizon_days)),
                   MIN_STOP_PCT * price)
        stop = round(price - direction * dist, 2)
    return {
        "action": "BUY" if direction > 0 else "SELL SHORT",
        "entry": round(price, 2),
        "stop": stop,
        "exit": f"after {horizon_days} trading day{'s' if horizon_days != 1 else ''}, or at the stop",
        "expected_excess_bps": round(expected_excess_bps, 1) if expected_excess_bps is not None else None,
    }


# The model is one voice. Insider buying, unusual buy flow, filings and proven
# event families on the Signal Board can outweigh it in either direction, and on
# their own can carry an idea. Weights are the board's evidence tiers; a family
# measured here with no edge, or not measured and without a documented
# direction, counts nothing.
EVIDENCE_WEIGHTS = {"proven": 3.0, "supported": 2.0, "published": 1.0}
MODEL_WEIGHTS = {"validated": 3.0, "supported": 2.0}
EVENT_IDEA_MIN_WEIGHT = 3.0          # one-sided evidence that can carry an idea alone
EVENT_IDEA_MAX_AGAINST = 1.0
# Events only: insider filings, anomalous trading and flow families, corporate
# filings. Price, fundamental and options-positioning signals are the model's
# own inputs (counting them again would vote twice), and several were measured
# here with no edge or the wrong sign.
EVENT_GROUPS = frozenset({"insider", "events", "filings"})


def evidence_balance(signals: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Event evidence on a name (EVENT_GROUPS): weighted bull and bear totals, with reasons."""
    bull = bear = 0.0
    reasons: Dict[int, List[Tuple[float, str]]] = {1: [], -1: []}
    for sg in signals or []:
        if sg.get("group") not in EVENT_GROUPS:
            continue
        w = EVIDENCE_WEIGHTS.get(str(sg.get("status")), 0.0)
        d = int(sg.get("direction") or 0)
        if not w or not d:
            continue
        # Evidence pouring in counts for more than a single print: a family
        # with five events in its window weighs half again, ten or more double.
        n = sg.get("value") if sg.get("group") in ("events", "insider") else None
        try:
            n = float(n) if n is not None else 0.0
        except (TypeError, ValueError):
            n = 0.0
        w *= 2.0 if n >= 10 else 1.5 if n >= 5 else 1.0
        if d > 0:
            bull += w
        else:
            bear += w
        tier = {"proven": "proven here", "supported": "model-backed", "published": "research-backed"}[str(sg["status"])]
        reasons[1 if d > 0 else -1].append((w, f"{sg.get('name')}: {sg.get('display')} ({tier})"))
    top = {k: [r for _w, r in sorted(v, key=lambda x: -x[0])[:3]] for k, v in reasons.items()}
    return {"bull": bull, "bear": bear, "bull_reasons": top[1], "bear_reasons": top[-1]}


def overruled(direction: int, model_support: Optional[str], balance: Dict[str, Any]) -> Optional[str]:
    """Why the evidence against a model idea outweighs the model, or None."""
    weight = MODEL_WEIGHTS.get(model_support or "", 0.0)
    against = balance["bear"] if direction > 0 else balance["bull"]
    behind = balance["bull"] if direction > 0 else balance["bear"]
    # Outweigh by a full vote: a tie (one block print and an earnings reaction
    # against a supported model, 8 Oct) is not evidence pouring in.
    if against >= weight + 1.0 and against > behind:
        why = "; ".join(balance["bear_reasons"] if direction > 0 else balance["bull_reasons"])
        side = "short" if direction < 0 else "long"
        return f"evidence against the model's {side} ({against:g} vs the model's {weight:g}): {why}"
    return None


def event_led(balance: Dict[str, Any]) -> int:
    """+1 / -1 where one-sided evidence is strong enough to carry an idea without the model."""
    if balance["bull"] >= EVENT_IDEA_MIN_WEIGHT and balance["bear"] <= EVENT_IDEA_MAX_AGAINST:
        return 1
    if balance["bear"] >= EVENT_IDEA_MIN_WEIGHT and balance["bull"] <= EVENT_IDEA_MAX_AGAINST:
        return -1
    return 0


EARNINGS_CYCLE_DAYS = 91


def next_earnings(last_release: Optional[date], today: date) -> Optional[date]:
    """The next quarterly results, estimated from the last 8-K item 2.02: a
    quarter after it, rolled forward past today. An estimate, and said so."""
    if not last_release:
        return None
    nxt = last_release
    while nxt < today:
        nxt = date.fromordinal(nxt.toordinal() + EARNINGS_CYCLE_DAYS)
    return nxt


def earnings_inside(next_release: Optional[date], today: date, horizon_days: int) -> bool:
    """Whether the estimated release falls inside the holding window (trading
    days read as calendar days x 7/5, plus a day for the release's timing)."""
    if not next_release:
        return False
    return 0 <= (next_release - today).days <= int(horizon_days * 7 / 5) + 1


def risk_terms(direction: int, entry: Optional[float], stop: Optional[float], adv_usd: Optional[float],
               annual_vol: Optional[float], position_usd: float = 10_000.0,
               target_daily_risk: float = 0.01) -> Dict[str, Optional[float]]:
    """What a position risks, in dollars: the loss at the stop on `position_usd`,
    the cost to trade it each way, and the size whose typical daily move is
    target_daily_risk of that amount (so a volatile name gets less)."""
    from shared.utils.paper_ledger import cost_bps

    out: Dict[str, Optional[float]] = {"loss_at_stop_usd": None, "cost_bps": None, "vol_sized_usd": None}
    if entry and stop:
        out["loss_at_stop_usd"] = round(position_usd * abs(entry - stop) / entry, 0)
    daily = annual_vol / math.sqrt(252) if annual_vol else None
    if adv_usd:
        out["cost_bps"] = round(cost_bps(position_usd, adv_usd, daily), 1)
    if daily:
        out["vol_sized_usd"] = round(min(position_usd, position_usd * target_daily_risk / daily), -1)
    return out


__all__ = [
    "EARNINGS_CYCLE_DAYS", "earnings_inside", "next_earnings", "risk_terms",
    "EVENT_GROUPS", "EVIDENCE_WEIGHTS", "EXPIRY_WINDOWS", "Leg", "atr", "evidence_balance", "event_led", "idea_screen",
    "build_option_idea", "choose_structure", "iv_regime", "overruled", "realized_vol", "stock_ideas",
    "stock_plan", "tradable_chain",
]
