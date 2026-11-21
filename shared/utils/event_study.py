"""Pure statistics for the free-dataset event studies (CFTC COT, Treasury auctions, Reg SHO), pre-registered in
reports/Upgrade pre-registrations - 12 Nov.md ("Datasets with no live collector").

Unit: an event's log excess return (instrument minus SPY) over the five sessions that start at the close of the first session strictly after the
publication date, so nothing is read before it was public. Events on one date are one cluster: the t-statistic is over cluster means, so a hundred
names listed on the same day are not a hundred observations. Bars per cell: at least 100 events, |t| >= 2, the same sign in the first and second
half of the events by date, and a Benjamini-Hochberg q below 0.10 across every cell tested. Also pure: no database, no clock.
"""
from __future__ import annotations

import math
from bisect import bisect_right
from collections import defaultdict
from datetime import date
from statistics import NormalDist, mean, stdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

HORIZON = 5
MIN_EVENTS = 100
MIN_ABS_T = 2.0
MAX_Q = 0.10

Series = Tuple[List[date], List[float]]          # ascending session dates and closes
EventRet = Tuple[str, date, float]               # cell, cluster date (publication date), log excess return


def forward_excess(inst: Series, bench: Dict[date, float], after: date, horizon: int = HORIZON) -> Optional[float]:
    """Log return of the instrument minus the benchmark from the close of the first session after `after` to `horizon` sessions later.
    None when the series does not reach, or the benchmark lacks either date."""
    days, closes = inst
    i = bisect_right(days, after)
    j = i + horizon
    if j >= len(days) or closes[i] <= 0 or closes[j] <= 0:
        return None
    b0, b1 = bench.get(days[i]), bench.get(days[j])
    if not b0 or not b1 or b0 <= 0 or b1 <= 0:
        return None
    return math.log(closes[j] / closes[i]) - math.log(b1 / b0)


def benjamini_hochberg(pvals: Sequence[float]) -> List[float]:
    """BH step-up q-values in the input order."""
    m = len(pvals)
    order = sorted(range(m), key=lambda k: pvals[k])
    q = [1.0] * m
    prev = 1.0
    for rank in range(m, 0, -1):
        k = order[rank - 1]
        prev = min(prev, pvals[k] * m / rank)
        q[k] = prev
    return q


def _cell_stats(rets: List[Tuple[date, float]]) -> Dict[str, Any]:
    rets = sorted(rets, key=lambda x: x[0])
    n = len(rets)
    by_day: Dict[date, List[float]] = defaultdict(list)
    for d, r in rets:
        by_day[d].append(r)
    cm = [mean(v) for v in by_day.values()]
    k = len(cm)
    t = 0.0
    if k >= 3:
        sd = stdev(cm)
        t = mean(cm) / (sd / math.sqrt(k)) if sd > 0 else 0.0
    p = 2.0 * (1.0 - NormalDist().cdf(abs(t))) if k >= 3 else 1.0
    half = n // 2
    h1 = mean(r for _, r in rets[:half]) if half else 0.0
    h2 = mean(r for _, r in rets[half:]) if n - half else 0.0
    return {"n": n, "clusters": k, "mean_bps": round(mean(r for _, r in rets) * 1e4, 2) if n else 0.0, "t": round(t, 3), "p": p,
            "half1_bps": round(h1 * 1e4, 2), "half2_bps": round(h2 * 1e4, 2), "same_sign": bool(n >= 2 and h1 * h2 > 0)}


def evaluate(events: Sequence[EventRet]) -> List[Dict[str, Any]]:
    """One result per cell, with BH q across all cells present, and `passes` when every registered bar is met."""
    cells: Dict[str, List[Tuple[date, float]]] = defaultdict(list)
    for cell, d, r in events:
        if r is not None and math.isfinite(r):
            cells[cell].append((d, r))
    names = sorted(cells)
    rows = [{"cell": c, **_cell_stats(cells[c])} for c in names]
    qs = benjamini_hochberg([r["p"] for r in rows])
    for r, q in zip(rows, qs):
        r["q"] = round(q, 4)
        r["passes"] = bool(r["n"] >= MIN_EVENTS and abs(r["t"]) >= MIN_ABS_T and r["same_sign"] and q < MAX_Q)
    return rows


def combine(result_sets: Sequence[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Re-run BH across the cells of several studies at once (the registered family is every cell of every dataset)."""
    rows = [dict(r) for rs in result_sets for r in rs]
    qs = benjamini_hochberg([r["p"] for r in rows])
    for r, q in zip(rows, qs):
        r["q"] = round(q, 4)
        r["passes"] = bool(r["n"] >= MIN_EVENTS and abs(r["t"]) >= MIN_ABS_T and r["same_sign"] and q < MAX_Q)
    return rows


def trailing_percentile(values: Sequence[float], window: int, min_history: int) -> List[Optional[float]]:
    """For each position, the share of the previous `window` values (excluding the current) that are below it; None until `min_history` exist."""
    out: List[Optional[float]] = []
    for i, v in enumerate(values):
        hist = values[max(0, i - window):i]
        out.append(sum(1 for h in hist if h < v) / len(hist) if len(hist) >= min_history else None)
    return out


def trailing_z(values: Sequence[float], window: int, min_history: int) -> List[Optional[float]]:
    """For each position, (value - mean) / stdev of the previous `window` values; None until `min_history` exist or when they are constant."""
    out: List[Optional[float]] = []
    for i, v in enumerate(values):
        hist = values[max(0, i - window):i]
        if len(hist) < min_history:
            out.append(None)
            continue
        sd = stdev(hist)
        out.append((v - mean(hist)) / sd if sd > 0 else None)
    return out


__all__ = ["HORIZON", "benjamini_hochberg", "combine", "evaluate", "forward_excess", "trailing_percentile", "trailing_z"]
