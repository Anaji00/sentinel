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
                exclude: Iterable[str] = ("SPY", "QQQ", "IWM", "DIA")) -> List[Dict[str, Any]]:
    """The n strongest and n weakest ranks as long and short ideas."""
    skip = {t.upper() for t in exclude}
    ranked = sorted(((t, v) for t, v in (scores or {}).items() if t.upper() not in skip),
                    key=lambda kv: kv[1].get("percentile", 0.5), reverse=True)
    longs = [{"ticker": t, "direction": 1, "percentile": v.get("percentile"), "quintile": v.get("quintile")}
             for t, v in ranked[:n]]
    shorts = [{"ticker": t, "direction": -1, "percentile": v.get("percentile"), "quintile": v.get("quintile")}
              for t, v in ranked[::-1][:n]]
    return longs + shorts


# Ideas must be tradable by a person, not only rankable by a model: the model's
# floor ($3, ~$3M a day) keeps the cross-section broad; an idea needs more.
IDEA_MIN_PRICE = 10.0
IDEA_MIN_FEED_DOLLAR_VOLUME = 500_000.0     # IEX share; ~ $20M a day consolidated
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
    """Entry, stop at STOP_ATR x ATR, exit at the horizon."""
    stop = None
    if atr_value and atr_value > 0:
        # At least MIN_STOP_PCT away: a pinned name (a pending acquisition)
        # has a daily range near zero, and a stop inside the spread is noise.
        dist = max(STOP_ATR * atr_value, MIN_STOP_PCT * price)
        stop = round(price - direction * dist, 2)
    return {
        "action": "BUY" if direction > 0 else "SELL SHORT",
        "entry": round(price, 2),
        "stop": stop,
        "exit": f"after {horizon_days} trading day{'s' if horizon_days != 1 else ''}, or at the stop",
        "expected_excess_bps": round(expected_excess_bps, 1) if expected_excess_bps is not None else None,
    }


__all__ = [
    "EXPIRY_WINDOWS", "Leg", "atr", "idea_screen", "build_option_idea", "choose_structure", "iv_regime",
    "realized_vol", "stock_ideas", "stock_plan", "tradable_chain",
]
