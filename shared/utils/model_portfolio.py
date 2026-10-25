"""
shared/utils/model_portfolio.py

An actively managed model portfolio that tries to beat SPY with the factor
model's buy and sell signals: SPY as the core, plus a market-neutral overlay --
long the monthly model's top fifth, short its bottom fifth among names the
broker will lend -- at OVERLAY_GROSS of the portfolio on each side. Rebalanced
on the model's own cadence with its hold buffer; between rebalances a name that
leaves the ranked universe is sold, and a short that can no longer be borrowed
is covered.

Why this shape (out-of-sample trials on 9 Oct 2026, consolidated bars, net of
costs): the monthly long/short book had beta ~0 to SPY and +191 bps a month of
alpha (t 2.3), nearly all of it from the short leg (-248 bps a month, t -3.9).
The long picks alone carry beta ~1.5 and no alpha after it, so a book of only
the buys beats SPY only by holding more market; equal-weight top-100/200
baskets of the picks did that and lost to SPY once beta was paid for, and so
did the same picks hedged to beta one. Thirty-two months, one of many trials:
evidence to test forward, which is what this portfolio does.

Pure: books, orders and the day's return from prices handed in. The engine
(services/agents/portfolio_engine.py) does the I/O.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from shared.utils.factor_model import COST_BPS_PER_SIDE, HOLD_BUFFER, HORIZON_SPECS

PORTFOLIO = "spy_plus_overlay"
HORIZON = "monthly"
REBALANCE_SESSIONS = HORIZON_SPECS[HORIZON].test_every
# Each leg's size as a share of the portfolio. At the trial's net 171 bps a
# month and ~16% annual volatility of the long/short book, 0.30 a side aims at
# roughly 5-6% a year over SPY for about 5% tracking error.
OVERLAY_GROSS = 0.30
# Easy-to-borrow names lend near the general-collateral rate.
BORROW_FEE_ANNUAL = 0.005
SESSIONS_PER_YEAR = 252

BUY, SELL, SHORT, COVER = "BUY", "SELL", "SHORT", "COVER"


@dataclass
class Order:
    ticker: str
    action: str
    reason: str
    percentile: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def due(sessions_since: Optional[int]) -> bool:
    """A full rebalance: the first one, then every REBALANCE_SESSIONS sessions."""
    return sessions_since is None or sessions_since >= REBALANCE_SESSIONS


def book(percentiles: Mapping[str, float], held: Iterable[str], side: int,
         allowed: Optional[Set[str]] = None) -> List[str]:
    """The model's buffered book (factor_model._buffered_book on live ranks):
    a fifth of every ranked name, from the top (side 1) or the bottom (-1). A
    held name stays while it is still within the best HOLD_BUFFER on its side;
    `allowed` limits a side to names it can trade (borrowable, for shorts)."""
    q = max(1, len(percentiles) // 5)
    edge = {t: (p if side > 0 else 1.0 - p) for t, p in percentiles.items() if allowed is None or t in allowed}
    keep = sorted((t for t in set(held) if t in edge and edge[t] >= 1.0 - HOLD_BUFFER),
                  key=lambda t: (-edge[t], t))[:q]
    chosen = set(keep)
    for t in sorted(edge, key=lambda t: (-edge[t], t)):
        if len(keep) >= q:
            break
        if t not in chosen:
            keep.append(t)
            chosen.add(t)
    return sorted(keep)


# The shadow variant: the same scores, ranked within each industry.
#
# The live book's first week (9-15 Oct 2026) was long 28 chip makers and 21
# precious-metal and mining names and short 18 REITs, 24 energy names and 14
# utilities: as much a sector bet as stock picking, and the 14 Oct chip selloff
# took the long leg down 4.1% against SPY's 1.1%. Ranked within industry, the
# top and bottom fifth of every industry go long and short, so no sector can
# dominate a side. Run in shadow beside the live book -- changing the live
# construction mid-test would void the test.
PORTFOLIO_INDUSTRY_NEUTRAL = "spy_plus_overlay_industry_neutral"
# Adopted 16 Oct 2026 on history instead of a month of paper trading
# (shared/utils/portfolio_lab.py): over the monthly model's 32 out-of-sample
# months the industry-neutral book earned less (184 vs 249 bps a month net) but
# more per unit of risk (Sharpe 1.70 vs 1.53, t 2.78 vs 2.50), with industry
# bets explaining 0.5% of its variance against 29%. It is now the headline
# book; the universe-wide one keeps running beside it as the shadow.
HEADLINE_PORTFOLIO = PORTFOLIO_INDUSTRY_NEUTRAL
SHADOW_PORTFOLIOS: Tuple[Tuple[str, str], ...] = (("universe_wide", PORTFOLIO),)
MIN_INDUSTRY_GROUP = 5          # smaller industries are ranked together as one group


def industry_relative(percentiles: Mapping[str, float], industries: Mapping[str, Optional[str]],
                      min_group: int = MIN_INDUSTRY_GROUP) -> Dict[str, float]:
    """Each name's percentile among its own industry (0 worst .. 1 best), ties by
    name. Industries with fewer than `min_group` ranked names, and names with no
    industry, are pooled into one group so they are still ranked."""
    groups: Dict[str, List[str]] = {}
    for t in percentiles:
        groups.setdefault(industries.get(t) or "", []).append(t)
    pooled = [t for g, names in groups.items() if not g or len(names) < min_group for t in names]
    ranked = [names for g, names in groups.items() if g and len(names) >= min_group]
    if pooled:
        ranked.append(pooled)
    out: Dict[str, float] = {}
    for names in ranked:
        order = sorted(names, key=lambda t: (percentiles[t], t))
        n = len(order)
        for i, t in enumerate(order):
            out[t] = (i + 0.5) / n
    return out


def maintain(held: Iterable[str], percentiles: Mapping[str, float], allowed: Optional[Set[str]] = None) -> List[str]:
    """Between rebalances: the names still ranked (and still tradable on this side)."""
    return sorted(t for t in held if t in percentiles and (allowed is None or t in allowed))


def _pct(p: Optional[float]) -> str:
    return f"{p * 100:.0f}" if p is not None else "?"


def _share(x: Optional[float]) -> str:
    """A tail share as whole percent, never rounded down to nothing: 0.003 -> 1."""
    return f"{max(1, math.ceil(x * 100 - 1e-9))}" if x is not None else "?"


def orders(prev: Mapping[int, Iterable[str]], new: Mapping[int, Iterable[str]],
           percentiles: Mapping[str, float], borrowable: Optional[Set[str]] = None,
           why_flat: Optional[str] = None) -> List[Order]:
    """What changes between two books, each with its reason in plain words.

    Percentile 1.0 is the model's favourite, 0.0 its least favourite."""
    keep_line = _pct(1.0 - HOLD_BUFFER)
    out: List[Order] = []
    old_l, new_l = set(prev.get(1, ())), set(new.get(1, ()))
    old_s, new_s = set(prev.get(-1, ())), set(new.get(-1, ()))
    for t in sorted(new_l - old_l):
        p = percentiles.get(t)
        out.append(Order(t, BUY, f"Entered the model's top fifth: among its best {_share(1.0 - p if p is not None else None)}% "
                                 f"of stocks.", p))
    for t in sorted(old_l - new_l):
        p = percentiles.get(t)
        if why_flat:
            why = why_flat
        elif p is None:
            why = "No longer ranked: left the liquid universe or stopped trading."
        else:
            why = (f"Slipped to the {_pct(p)}th percentile, below the {keep_line}th "
                   f"where held names are kept.")
        out.append(Order(t, SELL, why, p))
    for t in sorted(new_s - old_s):
        p = percentiles.get(t)
        out.append(Order(t, SHORT, f"In the model's bottom fifth: among its worst {_share(p)}% of stocks, "
                                   f"and easy to borrow. If you own it, this says sell.", p))
    for t in sorted(old_s - new_s):
        p = percentiles.get(t)
        if why_flat:
            why = why_flat
        elif p is None:
            why = "No longer ranked: left the liquid universe or stopped trading."
        elif borrowable is not None and t not in borrowable:
            why = "No longer easy to borrow: the broker may recall the shares."
        else:
            why = (f"Rose to the {_pct(p)}th percentile, out of the bottom "
                   f"{_pct(HOLD_BUFFER)}% where shorts are kept.")
        out.append(Order(t, COVER, why, p))
    return out


Bar = Tuple[Optional[float], Optional[float]]          # (open, close) for one session


def leg_return(old: Set[str], new: Set[str], prev_close: Mapping[str, float],
               today: Mapping[str, Bar], weights: Optional[Mapping[str, float]] = None) -> Tuple[float, List[str]]:
    """One session's return of a leg that changes at the open, equal-weight or
    by `weights` (a name without one weighs 1; meta-labelled sizing, applied
    when the lab confirms it).

    Names leaving are sold at the open (they earn the overnight move), names
    arriving are bought there (open to close), and an unchanged leg earns close
    to close. A name without the bar it needs counts flat and is returned, so a
    gap in the data shows instead of moving the result."""
    missing: List[str] = []

    def part(names: Set[str], fn) -> float:
        if not names:
            return 0.0
        total = wsum = 0.0
        for t in names:
            w = float(weights.get(t, 1.0)) if weights else 1.0
            wsum += w
            r = fn(t)
            if r is None:
                missing.append(t)
            else:
                total += w * r
        return total / wsum if wsum > 0 else 0.0

    def close_to_close(t: str) -> Optional[float]:
        pc, c = prev_close.get(t), today.get(t, (None, None))[1]
        return c / pc - 1.0 if pc and c else None

    def overnight(t: str) -> Optional[float]:
        pc, o = prev_close.get(t), today.get(t, (None, None))[0]
        return o / pc - 1.0 if pc and o else None

    def intraday(t: str) -> Optional[float]:
        o, c = today.get(t, (None, None))
        return c / o - 1.0 if o and c else None

    if old == new:
        return part(old, close_to_close), sorted(set(missing))
    return part(old, overnight) + part(new, intraday), sorted(set(missing))


def traded_fraction(old: Set[str], new: Set[str]) -> float:
    """Share of one leg's value traded moving from `old` to `new`, both sides."""
    out = len(old - new) / len(old) if old else 0.0
    inn = len(new - old) / len(new) if new else 0.0
    return out + inn


def day_step(nav: float, spy_nav: float, spy_ret: float, long_ret: float, short_ret: float,
             traded_long: float = 0.0, traded_short: float = 0.0, shorts_held: bool = True,
             gross: float = OVERLAY_GROSS, borrow_fee_annual: float = BORROW_FEE_ANNUAL,
             core: float = 1.0, short_gross: Optional[float] = None) -> Dict[str, float]:
    """One session: the SPY core (`core` of the portfolio, the beta hedge)
    plus the overlay's long minus short, less trading cost on what changed
    hands and the short book's borrow fee (the book's mean quoted fee when
    known, BORROW_FEE_ANNUAL otherwise). `short_gross`, when given, sizes the
    short leg apart from the long (the sentiment tilt, when the lab adopts it)."""
    sg = gross if short_gross is None else short_gross
    cost = (gross * traded_long + sg * traded_short) * COST_BPS_PER_SIDE / 1e4
    borrow = sg * borrow_fee_annual / SESSIONS_PER_YEAR if shorts_held else 0.0
    overlay = gross * long_ret - sg * short_ret - cost - borrow
    ret = core * spy_ret + overlay
    return {"nav": nav * (1.0 + ret), "spy_nav": spy_nav * (1.0 + spy_ret), "ret": ret,
            "overlay": overlay, "cost": cost, "borrow": borrow}


# Sizing the overlay (16 Oct 2026). The overlay was a fixed 30% a side with a
# full SPY core, so its size ignored how volatile the book had become and its
# net beta (0.27-0.38 in the portfolio lab) rode on top of the core's.
#   volatility target  scale the overlay by TARGET_LS_VOL over the book's own
#                      ex-ante volatility (its holdings' last quarter): the
#                      size of the book's swings, not its name count, stays put
#                      (Moreira & Muir 2017; Barroso & Santa-Clara 2015)
#   regime multiplier  shrink it when the equity-regime gauge reads stressed
#                      or panic (shared/utils/equity_regime.py)
#   beta hedge         set the SPY core to 1 minus the overlay's net beta, so
#                      the whole portfolio carries the market's beta and the
#                      overlay only its stock picking
TARGET_LS_VOL = 0.16            # the long-short book's annual volatility in the walk-forward
SCALE_BOUNDS = (0.4, 1.25)
CORE_BOUNDS = (0.6, 1.4)
VOL_LOOKBACK = 63
BETA_LOOKBACK = 126
SIZING_KEPT = 10


def _leg_series(rets: Mapping[str, Mapping[date, float]], names: Iterable[str], days: Sequence[date]) -> List[Optional[float]]:
    names = list(names)
    out: List[Optional[float]] = []
    for d in days:
        xs = [rets[t][d] for t in names if t in rets and d in rets[t]]
        out.append(sum(xs) / len(xs) if len(xs) >= max(1, len(names) // 2) else None)
    return out


def _beta(leg: Sequence[Optional[float]], mkt: Sequence[float]) -> Optional[float]:
    pairs = [(a, b) for a, b in zip(leg, mkt) if a is not None]
    if len(pairs) < 30:
        return None
    la, ma = [p[0] for p in pairs], [p[1] for p in pairs]
    mm, lm = sum(ma) / len(ma), sum(la) / len(la)
    var = sum((m - mm) ** 2 for m in ma)
    return sum((a - lm) * (m - mm) for a, m in zip(la, ma)) / var if var > 0 else None


def overlay_sizing(rets: Mapping[str, Mapping[date, float]], spy: Mapping[date, float], long: Iterable[str],
                   short: Iterable[str], multiplier: float = 1.0, gross: float = OVERLAY_GROSS) -> Dict[str, Any]:
    """The overlay's gross per side and the SPY core weight for the books
    `long` and `short`, from each name's daily returns `rets` and SPY's.

    Falls back to the plain design (gross, full core) where the history is
    too thin to estimate, so a new or empty book is never sized on noise."""
    long, short = sorted(set(long)), sorted(set(short))
    days = sorted(spy)[-BETA_LOOKBACK:]
    out: Dict[str, Any] = {"gross": round(gross * multiplier, 4), "core": 1.0, "vol_scale": 1.0,
                           "regime_multiplier": multiplier, "ex_ante_vol": None, "beta_long": None,
                           "beta_short": None}
    if not long or not short or len(days) < 40:
        return out
    leg_l, leg_s = _leg_series(rets, long, days), _leg_series(rets, short, days)
    recent = [a - b for a, b in zip(leg_l[-VOL_LOOKBACK:], leg_s[-VOL_LOOKBACK:]) if a is not None and b is not None]
    if len(recent) >= 20:
        m = sum(recent) / len(recent)
        vol = math.sqrt(sum((x - m) ** 2 for x in recent) / (len(recent) - 1) * SESSIONS_PER_YEAR)
        if vol > 0:
            scale = min(SCALE_BOUNDS[1], max(SCALE_BOUNDS[0], TARGET_LS_VOL / vol))
            out.update(ex_ante_vol=round(vol, 4), vol_scale=round(scale, 3))
    g = gross * out["vol_scale"] * multiplier
    mkt = [spy[d] for d in days]
    b_l, b_s = _beta(leg_l, mkt), _beta(leg_s, mkt)
    core = 1.0
    if b_l is not None and b_s is not None:
        core = min(CORE_BOUNDS[1], max(CORE_BOUNDS[0], 1.0 - g * (b_l - b_s)))
        out.update(beta_long=round(b_l, 3), beta_short=round(b_s, 3))
    out.update(gross=round(g, 4), core=round(core, 4))
    return out


def sizing_record(history: Sequence[Mapping[str, Any]], day: date) -> Mapping[str, Any]:
    """The sizing in force on `day`: the latest decided before it, or {}."""
    best: Optional[Mapping[str, Any]] = None
    for s in history or []:
        try:
            decided = date.fromisoformat(str(s["decided_on"])[:10])
        except (KeyError, ValueError):
            continue
        if decided < day and (best is None or decided >= date.fromisoformat(str(best["decided_on"])[:10])):
            best = s
    return best or {}


def sizing_for(history: Sequence[Mapping[str, Any]], day: date) -> Tuple[float, float]:
    """(gross, core) in force on `day`: the latest sizing decided before it,
    or the plain design when none was."""
    best: Optional[Mapping[str, Any]] = None
    for s in history or []:
        try:
            decided = date.fromisoformat(str(s["decided_on"])[:10])
        except (KeyError, ValueError):
            continue
        if decided < day and (best is None or decided >= date.fromisoformat(str(best["decided_on"])[:10])):
            best = s
    if best is None:
        return OVERLAY_GROSS, 1.0
    return float(best.get("gross", OVERLAY_GROSS)), float(best.get("core", 1.0))


def book_fee(names: Iterable[str], fees: Mapping[str, float]) -> float:
    """The short book's mean annual borrow fee as a fraction, from quoted fees
    in percent; names without a quote are charged BORROW_FEE_ANNUAL."""
    names = list(names)
    if not names:
        return BORROW_FEE_ANNUAL
    return sum(fees[t] / 100.0 if t in fees else BORROW_FEE_ANNUAL for t in names) / len(names)


def summary(rows: Sequence[Mapping[str, float]]) -> Dict[str, Optional[float]]:
    """Since inception, from the marked sessions (each with ret, spy_ret, nav,
    spy_nav): the return and SPY's, in percent; the annualised tracking error
    and information ratio of the daily difference; the share of sessions ahead
    of SPY; and the worst fall of the portfolio relative to SPY."""
    if not rows:
        return {"sessions": 0}
    diff = [float(r["ret"]) - float(r["spy_ret"]) for r in rows]
    n = len(diff)
    mean = sum(diff) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in diff) / (n - 1)) if n > 1 else None
    peak, worst = 0.0, 0.0
    for r in rows:
        rel = float(r["nav"]) / float(r["spy_nav"])
        peak = max(peak, rel)
        worst = min(worst, rel / peak - 1.0)
    last = rows[-1]
    return {
        "sessions": n,
        "return_pct": round((float(last["nav"]) - 1.0) * 100, 2),
        "spy_return_pct": round((float(last["spy_nav"]) - 1.0) * 100, 2),
        "excess_pct": round((float(last["nav"]) - float(last["spy_nav"])) * 100, 2),
        "tracking_error_pct": round(sd * math.sqrt(SESSIONS_PER_YEAR) * 100, 2) if sd else None,
        "info_ratio": round(mean / sd * math.sqrt(SESSIONS_PER_YEAR), 2) if sd else None,
        "ahead_share": round(sum(1 for x in diff if x > 0) / n, 3),
        "worst_vs_spy_pct": round(worst * 100, 2),
    }


__all__ = [
    "BORROW_FEE_ANNUAL", "BUY", "COVER", "HORIZON", "OVERLAY_GROSS", "Order", "PORTFOLIO",
    "REBALANCE_SESSIONS", "SELL", "SHORT", "book", "book_fee", "day_step", "due", "leg_return", "maintain",
    "orders", "summary", "traded_fraction",
]
