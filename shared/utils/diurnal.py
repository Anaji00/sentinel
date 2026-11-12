"""Same-time-of-day baselines for activity scores, with a robust (median and MAD) z that never includes the observation being scored.

Intraday volume follows a U-shaped daily profile, so a reading is compared with readings from the same session bucket, not with the whole
day. Buckets are the registered ones (Upgrade pre-registrations - 12 Nov, phase 3). Shadow tool: nothing here changes a live score.
Pure; no I/O.
"""
from __future__ import annotations

import collections
import statistics
from datetime import datetime
from typing import Deque, Dict, Iterable, Optional, Tuple
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")
# (name, first minute of the Eastern day, first minute after the bucket)
BUCKETS = (("before the open", 0, 570), ("opening", 570, 600), ("morning", 600, 690), ("midday", 690, 840),
           ("afternoon", 840, 930), ("closing", 930, 960), ("after the close", 960, 1440))
REGULAR = tuple(b[0] for b in BUCKETS[1:6])
MAD_TO_SIGMA = 1.4826
MIN_BASELINE = 30


def bucket_of(when: datetime) -> str:
    """The session bucket of a timestamp (naive times are taken as Eastern)."""
    et = when.astimezone(_ET) if when.tzinfo else when.replace(tzinfo=_ET)
    minute = et.hour * 60 + et.minute
    for name, lo, hi in BUCKETS:
        if lo <= minute < hi:
            return name
    return BUCKETS[-1][0]


def robust_z(x: float, baseline: Iterable[float], floor_rel: float = 0.05, min_obs: int = MIN_BASELINE) -> Optional[float]:
    """(x - median) / (1.4826 x MAD) of the baseline; None when it is too short. The scale is floored at `floor_rel` of the median
    (and at 1e-9) so a flat baseline does not turn a small move into a huge score. `x` must not be part of `baseline`."""
    base = list(baseline)
    if len(base) < min_obs:
        return None
    med = statistics.median(base)
    mad = statistics.median(abs(v - med) for v in base)
    scale = MAD_TO_SIGMA * max(mad, floor_rel * abs(med), 1e-9)
    return (x - med) / scale


class SlotBaseline:
    """Rolling same-bucket history per (key, bucket); scoring happens BEFORE the observation is added, so it never sees itself."""

    def __init__(self, maxlen: int = 400) -> None:
        self._maxlen = maxlen
        self._hist: Dict[Tuple[str, str], Deque[float]] = collections.defaultdict(lambda: collections.deque(maxlen=self._maxlen))

    def score_then_add(self, key: str, when: datetime, value: float) -> Optional[float]:
        bucket = bucket_of(when)
        hist = self._hist[(key, bucket)]
        z = robust_z(value, hist)
        hist.append(value)
        return z

    def size(self, key: str, when: datetime) -> int:
        return len(self._hist[(key, bucket_of(when))])


__all__ = ["BUCKETS", "MAD_TO_SIGMA", "MIN_BASELINE", "REGULAR", "SlotBaseline", "bucket_of", "robust_z"]
