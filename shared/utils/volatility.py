"""
shared/utils/volatility.py

Realised volatility, measured from bars this platform already stores.

The radar's threshold function documents itself as scaling with VIX:

    Calculates dynamic alpha and Z-Score threshold based on VIX state.
    Scales Z_threshold dynamically with VIX: Z_th = 3.0 * (VIX / 20.0)

and then reads `sentinel:macro:vix`, which nothing has ever written. VIX appears
in no collector -- not the macro tier, not the tradfi subscription, nowhere. The
read falls through to a hardcoded 20.0, so the threshold resolves to
3.0 * (20/20) = 3.0 and alpha to 0.10, on every call, in every market. A
function whose entire purpose is to adapt has never once adapted.

That is the same shape as the invented Hawkes excitation table and the macro
yield defaults: a chosen number wearing the clothes of a measurement.

Rather than add a feed for an index the free tiers do not carry, this measures
what the platform observes directly. Realised volatility of the index it already
records is not the same quantity as VIX -- VIX is implied, forward-looking, and
option-derived, while this is backward-looking and computed from prints -- but
it moves with the thing the threshold actually cares about, which is whether
today is a calm session or a violent one.

Annualised so the numbers land on the scale the existing thresholds were written
against, and refused outright when the sample cannot support it, because a
volatility estimate from four bars is how the earnings scorer came to compare an
observation against itself.
"""

import logging
import math
from typing import List, Optional, Sequence

logger = logging.getLogger("shared.volatility")

# Bars required before an estimate is offered. Below this the standard
# deviation is mostly a statement about which few prints happened to arrive.
MIN_VOLATILITY_BARS = 30

# Trading periods in a year, by bar size. Used to annualise, so the output sits
# on the same scale as the VIX numbers the thresholds were tuned against.
PERIODS_PER_YEAR = {
    "1m": 252 * 390,
    "5m": 252 * 78,
    "1h": 252 * 7,
    "1d": 252,
}

# Where the measurement is published. Deliberately not `sentinel:macro:vix`:
# this is realised volatility and calling it VIX would be the mislabelling this
# module exists to correct.
REALISED_VOL_KEY = "sentinel:macro:realised_vol"

# What a reader should assume when nothing has been measured.
#
# Kept only because the alternative is refusing to score anything at all, and
# stated here rather than buried at a call site so it is visible as the
# assumption it is.
ASSUMED_VOL_WHEN_UNMEASURED = 20.0


def simple_returns(closes: Sequence[float]) -> List[float]:
    """Bar-over-bar returns, skipping non-positive prices."""
    out = []
    for previous, current in zip(closes, closes[1:]):
        if previous and previous > 0 and current and current > 0:
            out.append((current - previous) / previous)
    return out


def realised_volatility(closes: Sequence[float], bar: str = "1m") -> Optional[float]:
    """Annualised realised volatility as a percentage, or None.

    None when the sample is too thin or the series does not move. A flat series
    has no volatility to report and returning zero would read as
    extraordinary calm rather than as an absence of data -- the distinction that
    the frozen macro quotes turned into six spurious correlations.
    """
    values = []
    for close in closes or []:
        try:
            v = float(close)
        except (TypeError, ValueError):
            continue
        if v == v and math.isfinite(v) and v > 0:
            values.append(v)

    if len(values) < MIN_VOLATILITY_BARS:
        return None

    returns = simple_returns(values)
    if len(returns) < MIN_VOLATILITY_BARS - 1:
        return None

    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    if variance <= 0:
        return None

    periods = PERIODS_PER_YEAR.get(bar, PERIODS_PER_YEAR["1m"])
    return round(math.sqrt(variance) * math.sqrt(periods) * 100.0, 3)


def threshold_for(volatility_pct: Optional[float]) -> tuple:
    """(alpha, z_threshold) for a volatility level.

    The mapping is the one the radar already used; what changes is that the
    input is now measured. The bounds stay because an unbounded threshold turns
    one violent session into a detector that reports nothing for a week.
    """
    vol = volatility_pct if volatility_pct is not None else ASSUMED_VOL_WHEN_UNMEASURED
    z_threshold = max(2.5, min(5.0, round(3.0 * (vol / 20.0), 2)))
    if vol < 12.0:
        alpha = 0.05
    elif vol > 25.0:
        alpha = 0.20
    else:
        alpha = 0.10
    return alpha, z_threshold
