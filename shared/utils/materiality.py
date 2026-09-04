"""
shared/utils/materiality.py

Whether a thing is big enough to matter, as distinct from how it ranks.

Every detector in this platform scores by empirical percentile. That was a
deliberate repair: fixed sigmoids were exhausted by real traffic and piled
events onto the ceiling, and a percentile is self-calibrating and spreads across
the range whatever the units. It answers "how does this rank against its
neighbours".

It cannot answer "is this worth anyone's attention", and the two questions come
apart badly when the whole neighbourhood is noise. Measured live on 4 September:

    AVAXUSDT 30-min moved -0.22% on $0.1M volume        scored 0.995
    AAPL options sweep, $118,700 premium                 scored 0.059

Both are correct percentiles. The first is the most extreme thing its detector
has lately seen, and its detector has lately seen nothing. The second is a real
institutional order in a stream that contains larger ones. Neither number is
about the market.

This supplies the missing half: an absolute sense of size, per domain, which is
*multiplied into* the percentile rather than replacing it. The ranking that
fixed the original ceiling-clustering survives intact; what is added is that a
detector ranking noise can no longer report certainty about it.

    final = percentile * materiality

A percentile of 0.995 on a $100k crypto candle becomes 0.995 * 0.29 = 0.29.
A percentile of 0.60 on a $40M block becomes 0.60 * 1.0 = 0.60. The ordering
within a domain is preserved wherever size is comparable, and collapses only
where the underlying observation was too small to have earned its rank.

Deliberately not a threshold
----------------------------
A floor that dropped events below a size would lose the small-but-strange, and
the platform's own history argues against it: the OFAC path is a floor rather
than a lift precisely because a sanctions match on a $50 transfer is still a
sanctions match. So this attenuates rather than filters, and callers that have a
categorical reason to override it -- sanctions, watchlists, emergency squawks --
apply their floor after it, as they already do.
"""

import logging
import math
from typing import Optional

logger = logging.getLogger("shared.materiality")

# What counts as fully material, per domain, in USD.
#
# Set at the size above which a professional would not need convincing that the
# observation is worth reading. Below it, attenuation is gradual and logarithmic
# -- there is no cliff, and an event at a tenth of the reference still scores a
# substantial share.
#
# These are reference points, not thresholds. They are written down because they
# are judgements about markets rather than facts the platform can measure: the
# system has no way to learn what a person considers a large options trade.
MATERIAL_NOTIONAL_USD = {
    "crypto_candle": 25_000_000.0,   # a 30-min bar worth reading about
    "crypto_trade": 1_000_000.0,     # the existing whale threshold
    "crypto_transfer": 1_000_000.0,
    "crypto_liquidation": 5_000_000.0,
    "options_flow": 500_000.0,       # premium, not contract notional
    "equity_block": 10_000_000.0,
    "dark_pool": 10_000_000.0,
    "market_anomaly": 25_000_000.0,
    "price_anomaly": 25_000_000.0,
    "insider_trade": 1_000_000.0,
}

# Where attenuation stops. An observation far below the reference is not
# nothing -- it is small, and the detector may still be right that it is odd --
# so the floor preserves ordering rather than zeroing it.
MIN_MATERIALITY = 0.15

# Size at or below which the reference-relative curve is treated as zero-signal.
# Below a thousand dollars, notional carries no information about intent in any
# of these markets.
NEGLIGIBLE_USD = 1_000.0


def materiality(notional_usd: Optional[float], event_type: str) -> float:
    """How much of full materiality this size represents, in [MIN_MATERIALITY, 1.0].

    Logarithmic, because these markets span many orders of magnitude and a
    linear ratio would make everything below the reference indistinguishable.
    At the reference the factor is 1.0; a tenth of it scores about 0.6; a
    hundredth about 0.3.

    Returns 1.0 when the size is unknown. An absent notional must never
    attenuate a score -- that would silently penalise every event type that does
    not carry one, which is most of the non-financial platform.
    """
    reference = MATERIAL_NOTIONAL_USD.get(str(event_type or "").lower())
    if reference is None or reference <= 0:
        return 1.0

    try:
        value = float(notional_usd) if notional_usd is not None else None
    except (TypeError, ValueError):
        return 1.0
    if value is None:
        return 1.0

    value = abs(value)
    if value >= reference:
        return 1.0
    if value <= NEGLIGIBLE_USD:
        return MIN_MATERIALITY

    # Position on a log scale between negligible and fully material.
    span = math.log10(reference) - math.log10(NEGLIGIBLE_USD)
    if span <= 0:
        return 1.0
    position = (math.log10(value) - math.log10(NEGLIGIBLE_USD)) / span
    position = max(0.0, min(1.0, position))

    # Squared, so the curve is convex rather than linear in log space.
    #
    # A linear position puts $100k at 54% of full materiality against a $25M
    # reference, which is too generous by inspection -- a hundred thousand
    # dollars of AVAX is not half of a notable move. Squaring keeps both
    # endpoints exactly where they were and bends the middle down, so the
    # attenuation bites where the observations that motivated this actually sit.
    return round(MIN_MATERIALITY + (1.0 - MIN_MATERIALITY) * (position ** 2), 4)


def apply_materiality(
    score: float,
    notional_usd: Optional[float],
    event_type: str,
) -> float:
    """A percentile score, weighted by whether the observation was big enough to earn it.

    The percentile keeps the ranking; this supplies the sense of size it cannot
    have. Applied last, before any categorical floor -- a sanctions match or an
    emergency squawk sets its minimum after this, because those are facts about
    the subject rather than judgements about magnitude.
    """
    try:
        base = float(score)
    except (TypeError, ValueError):
        return score
    if not (0.0 <= base <= 1.0):
        return score
    return round(base * materiality(notional_usd, event_type), 4)


# How far a price has to travel, per timeframe, before the move itself is worth
# reading. Materiality above asks whether the *bar* was big enough; this asks
# whether the *move* was, and the two are independent.
#
# After materiality was applied, BTCUSDT 60-min bars moving 0.47% still scored
# 0.832 -- because bitcoin's hourly volume is always material, and nothing
# examined the 0.47%. A percentile ranks a move against its neighbours and every
# neighbour was equally small.
#
# Scaled with the square root of the timeframe, as volatility is: a 4-hour bar
# has to travel twice as far as a 1-hour bar to be equally remarkable.
MATERIAL_MOVE_PCT_1H = 1.5

# Below this the move carries no information about direction at all -- it is
# inside the spread for most of what this platform watches.
NEGLIGIBLE_MOVE_PCT = 0.05

MIN_MOVE_MATERIALITY = 0.15


def move_materiality(move_pct: Optional[float], timeframe_minutes: Optional[float]) -> float:
    """How much of a full move this price travel represents, in [MIN, 1.0].

    `move_pct` is a percentage, signed or unsigned -- direction is not the
    question here, distance is. Returns 1.0 when either input is unknown, so a
    detector that does not carry a timeframe is never silently penalised.
    """
    try:
        move = abs(float(move_pct)) if move_pct is not None else None
        minutes = float(timeframe_minutes) if timeframe_minutes is not None else None
    except (TypeError, ValueError):
        return 1.0
    if move is None or minutes is None or minutes <= 0:
        return 1.0

    reference = MATERIAL_MOVE_PCT_1H * math.sqrt(minutes / 60.0)
    if move >= reference:
        return 1.0
    if move <= NEGLIGIBLE_MOVE_PCT:
        return MIN_MOVE_MATERIALITY

    span = math.log10(reference) - math.log10(NEGLIGIBLE_MOVE_PCT)
    if span <= 0:
        return 1.0
    position = max(0.0, min(1.0, (math.log10(move) - math.log10(NEGLIGIBLE_MOVE_PCT)) / span))
    return round(MIN_MOVE_MATERIALITY + (1.0 - MIN_MOVE_MATERIALITY) * (position ** 2), 4)
