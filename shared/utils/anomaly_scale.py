"""The map between a z-score and a unit anomaly score, in one place.

The enrichers compress an unbounded statistic into [0, 1] with
`1 - exp(-z / SCALE)`: five sigma clears the 0.6 the downstream thresholds use,
ten and twenty sigma stay distinguishable from it and from each other, and the
curve approaches 1.0 without arriving, which is the honest shape when there is
always a larger spike.

Going back the other way was `score * 4.5` in the gateway route and
`anomaly_score * 4.5` in three places in the radar panel -- a linear rescale,
presented as a z-score, using a different constant from the compression it was
meant to invert. It is not the inverse, and the constant disagreeing made it
worse in a specific way: **the displayed figure was bounded above by 4.5**, so
five, ten and twenty sigma all converged on the ceiling and the field named for
the quantity that separates them could not.

  true z =  5  -> score 0.632 -> shown 2.84
  true z = 10  -> score 0.865 -> shown 3.89
  true z = 20  -> score 0.982 -> shown 4.42

One constant, one forward function, one inverse.
"""
from __future__ import annotations

import math
from typing import Optional

# Sigma at which a volume spike is already clearly significant: the point where
# `1 - exp(-z/scale)` reaches ~63% of its range.
Z_SCORE_SCALE = 5.0

# Above this the inverse is reporting the reciprocal of a rounding error. A
# stored 0.9999 is "off the scale", not 46 sigma, and a number that large in a
# field called z_score reads as a measurement rather than as saturation.
MAX_REPORTABLE_Z = 25.0


def z_to_score(z_score: float) -> float:
    """An unbounded sigma into [0, 1). Negative z is not more anomalous."""
    return 1.0 - math.exp(-max(0.0, float(z_score)) / Z_SCORE_SCALE)


def score_to_z(score: Optional[float]) -> Optional[float]:
    """The unit score back to the sigma that would have produced it.

    None for a score that is not a number. Saturates at MAX_REPORTABLE_Z rather
    than running to infinity as the score approaches 1.0.

    This is only meaningful for scores produced by `z_to_score`. Not every
    anomaly score on this platform is -- other enrichers build theirs their own
    way -- so a caller displaying the result should say it is a z-equivalent
    rather than a measured sigma.
    """
    if score is None:
        return None
    try:
        value = float(score)
    except (TypeError, ValueError):
        return None
    if value != value or value <= 0.0:
        return 0.0
    if value >= 1.0:
        return MAX_REPORTABLE_Z
    z = -Z_SCORE_SCALE * math.log(1.0 - value)
    return round(min(z, MAX_REPORTABLE_Z), 3)


__all__ = ["Z_SCORE_SCALE", "MAX_REPORTABLE_Z", "z_to_score", "score_to_z"]
