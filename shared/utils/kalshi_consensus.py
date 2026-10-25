"""
shared/utils/kalshi_consensus.py

A market consensus for a macro release, from Kalshi's strike ladder. Pure; the
macro collector fetches the markets and keeps the last snapshot before each
release.

FRED publishes values, not forecasts, so every release was measured against the
prior reading and tagged `no_consensus`. Kalshi lists a ladder of markets on
the same release -- "initial claims at or above 210,000?", "CPI above 3.1%?" --
and each price is the market's probability. Where that probability falls
through one half is the market's median expectation: a consensus with money
behind it, free.

Only strikes with a real two-sided quote are used (a 1-cent bid against a
$1.00 ask is no opinion), the probabilities are forced to fall as the strike
rises (thin books cross), and no median is given unless the ladder brackets
one half.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

MAX_SPREAD = 0.15               # a wider bid-ask is not a price
MIN_STRIKES = 3

# Indicator (economic_calendar.INDICATOR_PROFILES) -> Kalshi series and the
# factor taking a strike into the release's units.
SERIES: Dict[str, Tuple[str, float]] = {
    "JOBLESS_CLAIMS": ("KXJOBLESSCLAIMS", 1.0),     # persons, as FRED's ICSA
    "NFP": ("KXPAYROLLS", 0.001),                   # persons -> thousands (PAYEMS change)
    "CPI": ("KXCPIYOY", 1.0),                       # percent year on year
    "UNEMPLOYMENT": ("KXU3", 1.0),                  # percent
}


def _f(x: Any) -> Optional[float]:
    try:
        return float(x) if x not in (None, "") else None
    except (TypeError, ValueError):
        return None


def strike_probabilities(markets: Sequence[Mapping[str, Any]]) -> List[Tuple[float, float]]:
    """(strike, P(outcome above the strike)) for an event's 'above' markets with
    a tight two-sided quote, strike ascending, made non-increasing."""
    pts: List[Tuple[float, float]] = []
    for m in markets:
        if m.get("strike_type") not in ("greater", "greater_or_equal"):
            continue
        k = _f(m.get("floor_strike"))
        bid, ask = _f(m.get("yes_bid_dollars")), _f(m.get("yes_ask_dollars"))
        if k is None or bid is None or ask is None or ask <= 0 or bid < 0 or ask - bid > MAX_SPREAD:
            continue
        pts.append((k, (bid + ask) / 2))
    pts.sort()
    out: List[Tuple[float, float]] = []
    running = 1.0
    for k, p in pts:
        running = min(running, p)
        out.append((k, running))
    return out


def implied_median(points: Sequence[Tuple[float, float]]) -> Optional[float]:
    """Where P(above) crosses one half, interpolated between strikes."""
    if len(points) < MIN_STRIKES or points[0][1] < 0.5 or points[-1][1] > 0.5:
        return None
    for (k0, p0), (k1, p1) in zip(points, points[1:]):
        if p0 >= 0.5 >= p1:
            return k0 if p0 == p1 else k0 + (p0 - 0.5) / (p0 - p1) * (k1 - k0)
    return None


def nearest_event(markets: Sequence[Mapping[str, Any]], now: datetime) -> Optional[str]:
    """The event (release) whose markets close soonest after now."""
    best: Optional[Tuple[str, str]] = None
    for m in markets:
        close = str(m.get("close_time") or "")
        ev = m.get("event_ticker")
        if not ev or not close or close < now.isoformat()[:19]:
            continue
        if best is None or close < best[0]:
            best = (close, ev)
    return best[1] if best else None


def consensus(markets: Sequence[Mapping[str, Any]], indicator: str, now: datetime) -> Optional[Dict[str, Any]]:
    """The market's median for the next release of an indicator, in its units."""
    if indicator not in SERIES:
        return None
    event = nearest_event(markets, now)
    if not event:
        return None
    ladder = [m for m in markets if m.get("event_ticker") == event]
    points = strike_probabilities(ladder)
    median = implied_median(points)
    if median is None:
        return None
    close = min(str(m.get("close_time")) for m in ladder if m.get("close_time"))
    return {"indicator": indicator, "event": event, "close_time": close,
            "median": round(median * SERIES[indicator][1], 4), "strikes": len(points)}


__all__ = ["MAX_SPREAD", "SERIES", "consensus", "implied_median", "nearest_event", "strike_probabilities"]
