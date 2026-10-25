"""
shared/utils/track_record.py

Grading what the platform said against what prices did.

Two things are graded, both point in time and net of the market:
  - every trade idea (recommendation_log), over its own horizon from the price
    it was given at;
  - a daily snapshot of every signal-board row (signal_matrix_log), over one
    and five trading days, so the board's net vote can be tested as a
    predictor rather than trusted as one.

Pure: closes in, grades out.
"""
from __future__ import annotations

import bisect
import math
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def forward_close(days: Sequence[date], closes: Sequence[float], start: date, k: int) -> Optional[Tuple[date, float]]:
    """The close k trading days after `start` (k=1: the next session's close)."""
    i = bisect.bisect_right(list(days), start) - 1 + k
    if i < 0 or i >= len(days):
        return None
    return days[i], closes[i]


def grade(direction: int, entry: float, exit_: float, spy_entry: Optional[float],
          spy_exit: Optional[float]) -> Dict[str, Optional[float]]:
    """Signed excess return: positive when the call was right after the market."""
    if not entry or entry <= 0 or not exit_:
        return {"ret": None, "spy_ret": None, "excess": None, "signed_excess": None, "hit": None}
    r = exit_ / entry - 1.0
    m = (spy_exit / spy_entry - 1.0) if spy_entry and spy_exit else 0.0
    ex = r - m
    signed = ex * (1 if direction >= 0 else -1)
    return {"ret": r, "spy_ret": m, "excess": ex, "signed_excess": signed,
            "hit": None if direction == 0 else float(signed > 0)}


def summarize(signed: Iterable[float]) -> Dict[str, Any]:
    """n, hit rate, mean signed excess in bps, and its t-statistic."""
    xs = [x for x in signed if x is not None and math.isfinite(x)]
    n = len(xs)
    if n == 0:
        return {"n": 0, "hit_rate": None, "mean_bps": None, "t": None}
    mean = sum(xs) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in xs) / (n - 1)) if n > 1 else 0.0
    return {
        "n": n,
        "hit_rate": round(sum(1 for x in xs if x > 0) / n, 3),
        "mean_bps": round(mean * 1e4, 1),
        "t": round(mean / (sd / math.sqrt(n)), 2) if sd > 0 and n > 1 else None,
    }


def day_clustered_t(dated: Iterable[Tuple[date, float]], overlap: int = 1) -> Dict[str, Any]:
    """The t-statistic with one observation per day, not per row.

    The board logs every stock every day, so one market move lands on hundreds
    of rows at once, and a five-day return logged daily shares four of its days
    with the next one. Counting rows as independent inflates t by far more than
    the sample earns. Rows are averaged within each day, and the daily means'
    standard error is Newey-West with `overlap - 1` lags. Fewer than four
    windows' worth of days is too few to estimate that error, and reads None."""
    by_day: Dict[date, List[float]] = {}
    for d, x in dated:
        if x is not None and math.isfinite(x):
            by_day.setdefault(d, []).append(x)
    ys = [sum(v) / len(v) for _, v in sorted(by_day.items())]
    n = len(ys)
    if n < max(3, 4 * overlap):
        return {"days": n, "t_days": None}
    mean = sum(ys) / n
    e = [y - mean for y in ys]
    var = sum(x * x for x in e) / n
    for lag in range(1, min(max(overlap, 1), n)):
        w = 1 - lag / max(overlap, 1)
        var += 2 * w * sum(e[i] * e[i - lag] for i in range(lag, n)) / n
    if var <= 0:
        return {"days": n, "t_days": None}
    return {"days": n, "t_days": round(mean / math.sqrt(var / n), 2)}


NET_BUCKETS: List[Tuple[str, float, float]] = [
    ("strong bear (net <= -3)", -math.inf, -3),
    ("bear (-2..-1)", -3, 0),
    ("none (0)", 0, 0),
    ("bull (1..2)", 0, 3),
    ("strong bull (net >= 3)", 3, math.inf),
]


def bucket_of(net: float) -> str:
    if net <= -3:
        return "strong bear (net <= -3)"
    if net < 0:
        return "bear (-2..-1)"
    if net == 0:
        return "none (0)"
    if net < 3:
        return "bull (1..2)"
    return "strong bull (net >= 3)"


__all__ = ["NET_BUCKETS", "bucket_of", "day_clustered_t", "forward_close", "grade", "summarize"]
