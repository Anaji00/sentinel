"""Anomaly score for a possible ship-to-ship transfer, from the geometry that defines it.

The detector scored every STS event 0.780 (4,073 events in 7 days, one distinct value), so the score ranked nothing: a 30-minute
brush between two tugs and a 14-hour rendezvous of a sanctioned tanker read the same. The score now rises with the evidence that
makes a transfer likely, and stays close to the old constant for an ordinary case so downstream thresholds keep their meaning.

Pure; no I/O.
"""
from __future__ import annotations

import math
from typing import Sequence

BASE = 0.55
MAX_SCORE = 0.97


def sts_score(dwell_hours: float, separation_nm: float, speeds_knots: Sequence[float], sanctioned: bool, in_named_region: bool) -> float:
    """Score in [BASE, MAX_SCORE]. Longer dwell, closer hulls, slower hulls, a sanctioned vessel and a named region each add."""
    dwell = max(0.0, float(dwell_hours or 0.0))
    sep = max(0.0, float(separation_nm if separation_nm is not None else 0.5))
    speed = max((float(s) for s in (speeds_knots or [3.0])), default=3.0)
    score = BASE
    score += 0.20 * (1.0 - math.exp(-dwell / 6.0))              # hours alongside: 0.11 at 4.6 h, 0.18 at 12 h
    score += 0.10 * max(0.0, 1.0 - sep / 0.5)                    # hulls within half a nautical mile
    score += 0.05 * max(0.0, 1.0 - speed / 3.0)                  # both stopped rather than creeping
    score += 0.12 if sanctioned else 0.0
    score += 0.05 if in_named_region else 0.0
    return round(min(MAX_SCORE, score), 3)


__all__ = ["BASE", "MAX_SCORE", "sts_score"]
