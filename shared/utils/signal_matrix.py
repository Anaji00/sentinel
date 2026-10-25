"""
shared/utils/signal_matrix.py

Every signal the platform can compute for one ticker, in one place, each with
what it is worth.

A signal here is a measurement with a direction and an evidence status:

  proven       an event family this platform's reaction study has proven
  supported    the cross-sectional model's rank where its horizon clears every
               out-of-sample bar but the deflated Sharpe
  published    a documented anomaly from the literature, not yet measured here
  no_edge      measured here and found empty -- shown, never counted
  context      descriptive (trend, RSI, gaps, volume, news tone): what a trader
               reads first, with no claim to predict

The tally counts each tier separately, so "two published and one proven" is
never blurred into a single confident-looking number. Pure: arrays and rows
in, signals out; services/agents/dossier_engine.py gathers the inputs.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from shared.utils.bar_feed import DAILY_BAR_VOLUME_SHARE

STATUSES = ("proven", "supported", "published", "no_edge", "context")
TALLY_WEIGHTS = {"proven": 3.0, "supported": 2.0, "published": 1.0}


@dataclass
class Signal:
    group: str
    name: str
    value: Optional[float]
    display: str
    direction: int = 0              # +1 bullish, -1 bearish, 0 none
    status: str = "context"
    source: str = ""
    note: str = ""
    as_of: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Matrix:
    ticker: str
    signals: List[Signal] = field(default_factory=list)

    def add(self, s: Optional[Signal]) -> None:
        if s is not None:
            self.signals.append(s)

    def tally(self) -> Dict[str, Any]:
        t: Dict[str, Dict[str, int]] = {s: {"bull": 0, "bear": 0} for s in STATUSES}
        for s in self.signals:
            if s.direction > 0:
                t[s.status]["bull"] += 1
            elif s.direction < 0:
                t[s.status]["bear"] += 1
        num = sum(TALLY_WEIGHTS[k] * (v["bull"] - v["bear"]) for k, v in t.items() if k in TALLY_WEIGHTS)
        den = sum(TALLY_WEIGHTS[k] * (v["bull"] + v["bear"]) for k, v in t.items() if k in TALLY_WEIGHTS)
        # lean is a ratio and reads +1.00 on a single vote; net is the weighted
        # count behind it, and the board sorts on net.
        return {"by_status": t, "lean": round(num / den, 3) if den else 0.0, "net": num, "weighted_votes": den}

    def as_dict(self) -> Dict[str, Any]:
        return {"ticker": self.ticker, "signals": [s.as_dict() for s in self.signals], "tally": self.tally()}


def _f(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _pct(v: Optional[float], digits: int = 1) -> str:
    return "n/a" if v is None else f"{v * 100:+.{digits}f}%"


def _ret(c: Sequence[float], k: int) -> Optional[float]:
    if len(c) <= k or not c[-1 - k]:
        return None
    return c[-1] / c[-1 - k] - 1.0


def _sma(c: Sequence[float], n: int) -> Optional[float]:
    return sum(c[-n:]) / n if len(c) >= n else None


def _std(xs: Sequence[float]) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def _rets(c: Sequence[float]) -> List[float]:
    return [c[i] / c[i - 1] - 1.0 for i in range(1, len(c)) if c[i - 1]]


def rsi(c: Sequence[float], n: int = 14) -> Optional[float]:
    if len(c) < n + 1:
        return None
    gains = losses = 0.0
    for i in range(len(c) - n, len(c)):
        d = c[i] - c[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - 100 / (1 + rs)


def price_signals(m: Matrix, closes: Sequence[float], highs: Sequence[float], lows: Sequence[float],
                  volumes: Sequence[float], spy: Sequence[float], as_of: Optional[str]) -> Dict[str, Optional[float]]:
    """Returns, trend, momentum, reversal, 52-week position, volatility, beta."""
    c = [float(x) for x in closes]
    if len(c) < 22:
        return {}
    r = _rets(c)
    rv21 = (_std(r[-21:]) or 0.0) * math.sqrt(252) if len(r) >= 21 else None
    rv63 = (_std(r[-63:]) or 0.0) * math.sqrt(252) if len(r) >= 63 else None
    g = "price"
    for k, label in ((1, "1 day"), (5, "1 week"), (21, "1 month"), (63, "3 months")):
        v = _ret(c, k)
        m.add(Signal(g, f"return {label}", v, _pct(v), 0, "context", "daily bars", as_of=as_of))
    if len(spy) == len(c) and len(c) > 22:
        for k, label in ((5, "1 week"), (21, "1 month")):
            a, b = _ret(c, k), _ret(list(spy), k)
            if a is not None and b is not None:
                m.add(Signal(g, f"excess vs SPY {label}", a - b, _pct(a - b), 0, "context", "daily bars", as_of=as_of))
    # 12-1 month momentum (Jegadeesh & Titman 1993): winners keep winning.
    if len(c) >= 253:
        mom = c[-22] / c[-253] - 1.0
        m.add(Signal(g, "12-1 month momentum", mom, _pct(mom), 1 if mom > 0.10 else -1 if mom < -0.10 else 0,
                     "published", "daily bars", "Jegadeesh & Titman 1993", as_of))
    # One-month reversal (Jegadeesh 1990; Lehmann 1990): last month's extremes revert.
    r21 = _ret(c, 21)
    if r21 is not None:
        m.add(Signal(g, "1-month reversal", r21, _pct(r21), -1 if r21 > 0.15 else 1 if r21 < -0.15 else 0,
                     "published", "daily bars", "Jegadeesh 1990: extreme one-month moves partly revert", as_of))
    # Nearness to the 52-week high (George & Hwang 2004).
    if len(highs) >= 252:
        hi = max(float(x) for x in highs[-252:])
        lo = min(float(x) for x in lows[-252:])
        near = c[-1] / hi if hi else None
        if near is not None:
            m.add(Signal(g, "price / 52-week high", near, f"{near:.0%}", 1 if near >= 0.95 else -1 if near <= 0.60 else 0,
                         "published", "daily bars", "George & Hwang 2004", as_of))
            m.add(Signal(g, "52-week range position", (c[-1] - lo) / (hi - lo) if hi > lo else None,
                         f"{(c[-1] - lo) / (hi - lo):.0%}" if hi > lo else "n/a", 0, "context", "daily bars", as_of=as_of))
    # Lottery demand: the month's largest daily gain (Bali, Cakici & Whitelaw 2011).
    if len(r) >= 21:
        mx = max(r[-21:])
        m.add(Signal(g, "max daily gain, 1 month", mx, _pct(mx), -1 if mx > 0.15 else 0,
                     "published", "daily bars", "Bali, Cakici & Whitelaw 2011: lottery-like names underperform", as_of))
    for n in (20, 50, 200):
        s = _sma(c, n)
        if s:
            m.add(Signal(g, f"vs {n}-day average", c[-1] / s - 1.0, _pct(c[-1] / s - 1.0), 0, "context",
                         "daily bars", "trend context; moving-average votes measured a 46% record here", as_of))
    rs = rsi(c)
    if rs is not None:
        m.add(Signal(g, "RSI 14", rs, f"{rs:.0f}", 0, "context", "daily bars", as_of=as_of))
    if rv21 is not None:
        m.add(Signal(g, "realised volatility 21d", rv21, f"{rv21:.0%}", 0, "context", "daily bars", as_of=as_of))
    beta = None
    if len(spy) == len(c) and len(c) >= 127:
        rs_, rm = r[-126:], _rets(list(spy))[-126:]
        mm, ms = sum(rm) / len(rm), sum(rs_) / len(rs_)
        var = sum((x - mm) ** 2 for x in rm)
        if var > 0:
            beta = sum((a - ms) * (b - mm) for a, b in zip(rs_, rm)) / var
            m.add(Signal(g, "beta to SPY, 6 months", beta, f"{beta:.2f}", 0, "context", "daily bars", as_of=as_of))
    vol = [float(x or 0) for x in volumes]
    if len(vol) >= 21 and sum(vol[-21:-1]) > 0:
        base = sum(vol[-21:-1]) / 20
        surge = vol[-1] / base if base else None
        if surge is not None:
            m.add(Signal(g, "volume vs 20-day average", surge, f"{surge:.1f}x", 0, "context", "daily bars", as_of=as_of))
    return {"rv21": rv21, "rv63": rv63, "beta": beta, "price": c[-1]}


def model_signals(m: Matrix, ranks: Dict[str, Optional[Dict[str, Any]]], support: Dict[str, Optional[str]],
                  as_of: Optional[str], legs: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
    """The cross-sectional model's rank at each horizon, counted only where
    supported and only on a side whose leg beat SPY out of sample."""
    from shared.utils.trade_plan import leg_earns
    for h in ("daily", "weekly", "monthly"):
        e = ranks.get(h)
        if not e:
            continue
        q, pct = e.get("quintile"), _f(e.get("percentile"))
        st = support.get(h)
        status = "supported" if st in ("supported", "validated") else "context"
        direction = (1 if q == 5 else -1 if q == 1 else 0) if status == "supported" else 0
        if direction and legs is not None and not leg_earns(legs.get(h) or {}, direction):
            direction = 0
        m.add(Signal("model", f"{h} rank", pct, f"p{pct * 100:.0f} (quintile {q})" if pct is not None else "n/a",
                     direction, status, "cross-sectional model",
                     f"horizon {st or 'below the out-of-sample bar'}", as_of))


def cross_section_pct(values: Dict[str, Optional[float]]) -> Dict[str, float]:
    """Each name's percentile (0..1) among today's names with a value."""
    known = sorted((v, t) for t, v in values.items() if v is not None)
    n = len(known)
    return {t: (k / (n - 1) if n > 1 else 0.5) for k, (_v, t) in enumerate(known)}


# The papers sort stocks against each other; a fixed level means a different
# thing in a calm market and a frightened one. Extreme fifths vote.
XS_EXTREME = 0.2
XS_MIN_NAMES = 100


def options_signals(m: Matrix, live: Dict[str, Any], daily: Sequence[Dict[str, Any]], sweeps: Dict[str, float],
                    rv21: Optional[float], as_of: Optional[str],
                    xs: Optional[Dict[str, Optional[float]]] = None) -> None:
    """xs: this name's percentile today for "iv_skew" and "cp_iv_spread" among
    every name with a measure (shared by the caller); absent, fixed levels."""
    xs = xs or {}
    g = "options"
    pcr = _f(live.get("put_call_ratio"))
    hist = [_f(d.get("put_call_ratio")) for d in daily[1:21]]
    hist = [x for x in hist if x is not None]
    if pcr is not None:
        z = None
        if len(hist) >= 5 and (_std(hist) or 0) > 0:
            z = (pcr - sum(hist) / len(hist)) / _std(hist)
        m.add(Signal(g, "put/call volume", pcr, f"{pcr:.2f}" + (f" (z {z:+.1f})" if z is not None else ""),
                     -1 if (z is not None and z > 1.5) else 1 if (z is not None and z < -1.5) else 0,
                     "published", "option chain",
                     "Pan & Poteshman 2006: high put/call volume preceded lower returns", as_of))
    skew = _f(live.get("iv_skew"))
    if skew is not None:
        pct = xs.get("iv_skew")
        if pct is not None:
            d, disp = (-1 if pct >= 1 - XS_EXTREME else 1 if pct <= XS_EXTREME else 0),                 f"{skew * 100:+.1f} vol pts, steeper than {pct:.0%} of stocks today"
        else:
            d, disp = (-1 if skew > 0.08 else 1 if skew < 0.02 else 0), f"{skew * 100:+.1f} vol pts"
        m.add(Signal(g, "IV skew (OTM put - ATM call)", skew, disp, d, "published", "option chain",
                     "Xing, Zhang & Zhao 2010: steep skew preceded underperformance", as_of))
    cps = _f(live.get("cp_iv_spread"))
    if cps is not None:
        pct = xs.get("cp_iv_spread")
        if pct is not None:
            d, disp = (1 if pct >= 1 - XS_EXTREME else -1 if pct <= XS_EXTREME else 0),                 f"{cps * 100:+.1f} vol pts, higher than {pct:.0%} of stocks today"
        else:
            d, disp = (1 if cps > 0.01 else -1 if cps < -0.02 else 0), f"{cps * 100:+.1f} vol pts"
        m.add(Signal(g, "call - put IV spread", cps, disp, d, "published", "option chain",
                     "Cremers & Weinbaum 2010: calls dearer than puts preceded outperformance", as_of))
    atm = _f(live.get("atm_iv"))
    if atm is not None and rv21:
        ratio = atm / rv21
        m.add(Signal(g, "implied / realised vol", ratio, f"{ratio:.2f} ({atm:.0%} vs {rv21:.0%})", 0, "context",
                     "option chain", "Goyal & Saretto 2009: drives the option structure, not the stock's direction", as_of))
    net = sweeps.get("call", 0.0) - sweeps.get("put", 0.0)
    if sweeps:
        m.add(Signal(g, "sweep premium, calls - puts, 5d", net, f"${net / 1e6:+.2f}M",
                     1 if net > 0 else -1 if net < 0 else 0, "no_edge", "option sweeps",
                     "measured here on 5 October: sweeps carried no edge", as_of))


def short_signals(m: Matrix, si: Sequence[Dict[str, Any]], as_of: Optional[str]) -> None:
    if not si:
        return
    last = si[0]
    dtc = _f(last.get("days_to_cover"))
    chg = _f(last.get("change_pct"))
    if dtc is not None:
        m.add(Signal("short", "days to cover", dtc, f"{dtc:.1f} days ({last.get('settlement_date')})",
                     -1 if dtc > 7 else 0, "published", "FINRA short interest",
                     "Hong, Li, Ni, Scheinkman & Yan 2015; no edge at one week here, documented monthly",
                     str(last.get("settlement_date"))))
    if chg is not None:
        m.add(Signal("short", "short interest change", chg / 100, f"{chg:+.1f}%", 0, "context",
                     "FINRA short interest", as_of=str(last.get("settlement_date"))))


def insider_signals(m: Matrix, buys_usd: float, sells_usd: float, buyers: int, sellers: int,
                    clusters: int, as_of: Optional[str]) -> None:
    if not (buys_usd or sells_usd or clusters):
        return
    net = buys_usd - sells_usd
    m.add(Signal("insider", "open-market net, 90d (plans excluded)", net, f"${net / 1e6:+.2f}M "
                 f"({buyers} buying, {sellers} selling)",
                 1 if buys_usd > 0 and net > 0 and buyers >= 1 else 0, "published", "SEC Form 4",
                 "Cohen, Malloy & Pomorski 2012: opportunistic insider buying predicts returns; routine "
                 "and 10b5-1 trades do not", as_of))
    if clusters:
        m.add(Signal("insider", "insider clusters, 90d", float(clusters), str(clusters), 1, "published",
                     "SEC Form 4", "several insiders buying together", as_of))


def filing_signals(m: Matrix, items: Dict[str, Dict[str, Any]], ear: Optional[float], ear_date: Optional[str]) -> None:
    for fam, info in items.items():
        st = info.get("status", "published")
        m.add(Signal("filings", fam.replace("8k_", "8-K ").replace("_", " "), float(info.get("count", 1)),
                     f"{info.get('count', 1)} in 30d, last {info.get('last')}", info.get("direction", 0),
                     st, "SEC EDGAR", info.get("note", ""), info.get("last")))
    if ear is not None:
        # Measured here over 37,394 reactions (8 Oct 2026): jumps reversed rather
        # than drifted (-37 bps the next week), against the literature's prior.
        # Shown, not counted, until a pre-registered test of the reversal holds.
        m.add(Signal("filings", "earnings reaction (last 2.02)", ear, _pct(ear), 0, "context",
                     "SEC EDGAR + daily bars",
                     "measured here: reactions of 2%+ reversed the next week (-37 bps); no vote", ear_date))


def news_signals(m: Matrix, count: int, mean_sent: Optional[float], as_of: Optional[str]) -> None:
    if not count:
        return
    m.add(Signal("news", "headlines naming it, 72h", float(count),
                 f"{count}" + (f", tone {mean_sent:+.2f}" if mean_sent is not None else ""),
                 0, "context", "news feeds", "company-news families measured no edge here", as_of))


# Event families with a documented record in the literature: 8-K items (event
# studies), insider clusters (Cohen, Malloy & Pomorski), institutional flows,
# activist 13Ds (Brav et al.), short-volume spikes (Diether, Lee & Werner),
# earnings surprises (Bernard & Thomas).
LITERATURE_FAMILY_PREFIXES = (
    "8k_", "insider_cluster", "institutional_", "activist_13d", "short_volume_spike", "earnings_surprise",
    "earnings_reaction", "block_buy", "block_sell",
)


def event_signals(m: Matrix, dossier: Optional[Dict[str, Any]]) -> None:
    """Every event family on the name over the window, with the study's verdict.

    proven counts; no_edge is shown and never counted; a family not yet
    measured is published research at best, and counted only at that weight
    when its prior has a direction."""
    for f in (dossier or {}).get("factors", []):
        fam = str(f.get("family", ""))
        # Unmeasured here counts as research-backed only where the literature
        # documents the pattern; news-tone families do not qualify.
        default = "published" if fam.startswith(LITERATURE_FAMILY_PREFIXES) else "context"
        st = {"proven": "proven", "no_edge": "no_edge"}.get(f.get("status"), default)
        rec = f.get("study") or {}
        note = (f"study: {rec.get('verdict')}, {rec.get('units')} episodes, hit {rec.get('hit_rate')}, "
                f"{rec.get('mean_bps')} bps" if rec else "not yet measured here")
        direction = int(f.get("direction") or 0)
        if st == "published" and direction == 0:
            st = "context"
        m.add(Signal("events", str(f.get("family", "")).replace("_", " "), float(f.get("events") or 0),
                     f"{f.get('events')} event(s), last {str(f.get('last_seen', ''))[:16]}",
                     direction, st, str(f.get("source", "")), note, str(f.get("last_seen", ""))))


# Fundamentals, from the cross-sectional model's own feature ranks (-0.5..+0.5,
# point in time): (label, citation, direction when the rank is high).
FUNDAMENTAL_FEATURES = {
    "bm": ("value (book-to-market)", "Fama & French 1992: cheaper stocks have earned more", 1),
    "ey": ("earnings yield", "Basu 1977: high earnings yield has preceded higher returns", 1),
    "roa": ("profitability (ROA)", "Fama & French 2015; Novy-Marx 2013: profitable firms have earned more", 1),
    "asset_growth": ("asset growth", "Cooper, Gulen & Schill 2008: fast-growing balance sheets have lagged", -1),
    "accruals": ("accruals", "Sloan 1996: earnings not backed by cash have preceded weaker returns", -1),
    "gpa": ("gross profitability", "Novy-Marx 2013: high gross profit to assets has earned more", 1),
    "value_ind": ("value vs its industry",
                  "Asness, Porter & Stevens 2000: cheapness measured within an industry predicts better", 1),
    "quality_ind": ("quality vs its industry",
                    "Asness, Frazzini & Pedersen 2019: profitable, cash-backed firms have earned more", 1),
}


def fundamental_signals(m: Matrix, feature_ranks: Optional[Dict[str, float]], as_of: Optional[str]) -> None:
    for key, (label, cite, high_dir) in FUNDAMENTAL_FEATURES.items():
        r = _f((feature_ranks or {}).get(key))
        if r is None:
            continue
        pct = r + 0.5
        direction = high_dir if r > 0.3 else -high_dir if r < -0.3 else 0
        m.add(Signal("fundamentals", label, pct, f"higher than {pct:.0%} of stocks", direction, "published",
                     "SEC XBRL filings", cite, as_of))


def execution_signals(m: Matrix, price: Optional[float], feed_dollar_volume: Optional[float], rv: Optional[float],
                      meta: Optional[Sequence[Any]]) -> Dict[str, Optional[float]]:
    """What it costs to trade: consolidated volume, square-root impact for $1M, borrow."""
    out: Dict[str, Optional[float]] = {}
    if feed_dollar_volume:
        adv = feed_dollar_volume / DAILY_BAR_VOLUME_SHARE
        out["adv_usd"] = adv
        m.add(Signal("execution", "daily dollar volume (est.)", adv, f"${adv / 1e6:,.0f}M", 0, "context",
                     "consolidated daily volume" if DAILY_BAR_VOLUME_SHARE == 1.0 else "IEX volume / 2.5% share",
                     as_of=None))
        sigma = (rv or 0.3) / math.sqrt(252)
        half_spread = 1.0 if adv > 5e8 else 2.5 if adv > 5e7 else 5.0 if adv > 1e7 else 12.5
        impact = 1e4 * sigma * math.sqrt(1e6 / adv)
        cost = half_spread + impact
        out["cost_bps_1m"] = cost
        m.add(Signal("execution", "cost to trade $1M (est.)", cost, f"{cost:.0f} bps ({impact:.0f} impact)", 0,
                     "context", "square-root impact model", "half-spread by liquidity + sigma x sqrt(Q/ADV)", None))
        m.add(Signal("execution", "$1M as share of volume", 1e6 / adv, f"{1e6 / adv:.2%}", 0, "context", "", "", None))
    if meta:
        m.add(Signal("execution", "borrow", None,
                     "easy to borrow" if meta[1] and meta[2] else "shortable, hard to borrow" if meta[1] else "not shortable",
                     0, "context", "Alpaca asset list", "", None))
    return out


__all__ = [
    "FUNDAMENTAL_FEATURES", "Matrix", "cross_section_pct", "STATUSES", "Signal", "event_signals", "execution_signals", "fundamental_signals", "filing_signals", "insider_signals", "model_signals",
    "news_signals", "options_signals", "price_signals", "rsi", "short_signals",
]
