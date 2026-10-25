"""
shared/utils/paper_ledger.py

Every trade idea, traded on paper as a desk would have traded it.

The track record (track_record.py) grades an idea from the price it was given
at, which no one could have traded: the idea is logged during the session and
the price is already history. Here each idea is filled at the next session's
open after it was logged, pays half the spread and square-root market impact
on the way in and out, exits at its own stop if a session's range crosses it,
and otherwise at the close of its last session. Returns are net of SPY over the
same open-to-close window.

Pure: bars in, fills out. services/agents/grader.py does the I/O.
"""
from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Dict, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

HORIZON_SESSIONS = {"daily": 1, "weekly": 5, "monthly": 21}
NOTIONAL_USD = 250_000.0
# Volatility-managed sizing (Moreira & Muir 2017): exposure scaled by the
# market's long-run variance over its recent variance, bounded. Recorded beside
# the fixed-size result, so the ledger itself tests whether scaling helps.
SCALE_RECENT, SCALE_LONG = 21, 252
SCALE_BOUNDS = (0.25, 2.0)
_NY = ZoneInfo("America/New_York")
_OPEN = time(9, 30)


@dataclass(frozen=True)
class Bar:
    day: date
    open: float
    high: float
    low: float
    close: float
    volume: float


def half_spread_bps(adv_usd: float) -> float:
    """By liquidity tier, as the signal board's execution estimate."""
    return 1.0 if adv_usd > 5e8 else 2.5 if adv_usd > 5e7 else 5.0 if adv_usd > 1e7 else 12.5


def cost_bps(notional: float, adv_usd: Optional[float], daily_vol: Optional[float]) -> float:
    """One side: half the spread plus square-root impact, sigma x sqrt(Q / ADV)."""
    if not adv_usd or adv_usd <= 0:
        return 50.0                                  # unknown liquidity: priced as illiquid
    sigma = daily_vol if daily_vol and daily_vol > 0 else 0.02
    return half_spread_bps(adv_usd) + 1e4 * sigma * math.sqrt(notional / adv_usd)


def entry_session(days: Sequence[date], idea_at: datetime) -> Optional[int]:
    """Index of the first session whose open is after the idea was logged."""
    if isinstance(idea_at, str):                     # some readers hand back ISO text
        idea_at = datetime.fromisoformat(idea_at.replace("Z", "+00:00"))
    ny = idea_at.astimezone(_NY)
    i = bisect.bisect_left(list(days), ny.date())
    if i < len(days) and days[i] == ny.date() and ny.time() >= _OPEN:
        i += 1                                       # logged after today's open: tomorrow's
    return i if i < len(days) else None


def liquidity(bars: Sequence[Bar], upto: int, window: int = 21) -> Tuple[Optional[float], Optional[float]]:
    """(mean dollar volume, daily volatility) over the sessions before `upto`."""
    lo = max(0, upto - window)
    seg = bars[lo:upto]
    if len(seg) < 5:
        return None, None
    adv = sum(b.close * b.volume for b in seg) / len(seg)
    rets = [math.log(b.close / a.close) for a, b in zip(seg, seg[1:]) if a.close > 0 and b.close > 0]
    if len(rets) < 4:
        return adv, None
    m = sum(rets) / len(rets)
    return adv, math.sqrt(sum((r - m) ** 2 for r in rets) / (len(rets) - 1))


def vol_scale(spy_closes: Sequence[float]) -> float:
    """long-run / recent variance of daily market returns, from closes before entry."""
    rets = [math.log(b / a) for a, b in zip(spy_closes, spy_closes[1:]) if a > 0 and b > 0]
    if len(rets) < SCALE_RECENT + 20:
        return 1.0

    def var(xs: Sequence[float]) -> float:
        m = sum(xs) / len(xs)
        return sum((x - m) ** 2 for x in xs) / (len(xs) - 1)

    recent, long = var(rets[-SCALE_RECENT:]), var(rets[-SCALE_LONG:])
    if recent <= 0:
        return SCALE_BOUNDS[1]
    return max(SCALE_BOUNDS[0], min(SCALE_BOUNDS[1], long / recent))


@dataclass
class Fill:
    entry_day: date
    entry_price: float
    spy_entry: float
    cost_bps: float
    adv_usd: Optional[float]
    exit_day: Optional[date] = None
    exit_price: Optional[float] = None
    spy_exit: Optional[float] = None
    exit_reason: Optional[str] = None
    gross_excess: Optional[float] = None
    net_excess: Optional[float] = None
    pnl_usd: Optional[float] = None
    scale: float = 1.0
    pnl_scaled_usd: Optional[float] = None


def simulate(bars: Sequence[Bar], spy: Dict[date, Bar], idea_at: datetime, direction: int, horizon: str,
             stop: Optional[float] = None, notional: float = NOTIONAL_USD) -> Optional[Fill]:
    """The trade an idea would have been: None until its entry session has traded."""
    days = [b.day for b in bars]
    i = entry_session(days, idea_at)
    if i is None or days[i] not in spy:
        return None
    e = bars[i]
    adv, vol = liquidity(bars, i)
    side_cost = cost_bps(notional, adv, vol)
    f = Fill(e.day, e.open, spy[e.day].open, side_cost, adv)
    f.scale = vol_scale([spy[d].close for d in sorted(spy) if d < e.day][-(SCALE_LONG + 1):])
    n = HORIZON_SESSIONS.get(horizon, 5)
    last = i + n - 1
    for k in range(i, min(last, len(bars) - 1) + 1):
        b = bars[k]
        if stop and stop > 0:
            hit = (direction > 0 and b.low <= stop) or (direction < 0 and b.high >= stop)
            gap = (direction > 0 and b.open <= stop) or (direction < 0 and b.open >= stop)
            # On the entry session the open is the fill: a stop already crossed
            # by it is set after entry, not hit. Later, a gap through the stop
            # fills at the open, not at the stop.
            if hit and not (k == i and gap):
                px = b.open if (gap and k > i) else stop
                return _close(f, b.day, px, spy.get(b.day), "stop", direction, notional)
        if k == last:
            return _close(f, b.day, b.close, spy.get(b.day), "horizon", direction, notional)
    return f                                          # still open


def _close(f: Fill, day: date, px: float, spy_bar: Optional[Bar], reason: str, direction: int,
           notional: float) -> Fill:
    if spy_bar is None:
        return f
    f.exit_day, f.exit_price, f.spy_exit, f.exit_reason = day, px, spy_bar.close, reason
    r = px / f.entry_price - 1.0
    m = f.spy_exit / f.spy_entry - 1.0
    f.gross_excess = (r - m) * (1 if direction > 0 else -1)
    f.net_excess = f.gross_excess - 2 * f.cost_bps / 1e4
    f.pnl_usd = notional * f.net_excess
    f.pnl_scaled_usd = f.pnl_usd * f.scale
    return f


def open_mark(direction: int, entry: float, spy_entry: float, close: float, spy_close: float,
              cost_bps_each_way: float) -> Optional[float]:
    """An open trade's net excess over SPY at the latest close, both sides of
    costs charged as if it were closed there. Without it a horizon longer than
    the ledger's history can only show the trades its stops closed."""
    if not (entry and spy_entry and close and spy_close) or entry <= 0 or spy_entry <= 0:
        return None
    gross = ((close / entry - 1.0) - (spy_close / spy_entry - 1.0)) * (1 if direction > 0 else -1)
    return gross - 2 * float(cost_bps_each_way or 0) / 1e4


__all__ = ["Bar", "Fill", "HORIZON_SESSIONS", "NOTIONAL_USD", "cost_bps", "entry_session", "half_spread_bps",
           "liquidity", "open_mark", "simulate", "vol_scale"]
