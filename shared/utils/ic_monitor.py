"""
shared/utils/ic_monitor.py

Is the factor model still working? Each night's scores are kept
(factor_scores_daily); once a horizon has passed, the scores are graded by
their rank correlation (IC) with what the stocks then did against SPY, from the
official consolidated daily bars. The live IC is set beside the IC the
walk-forward promised, so a model that stops working shows here before the
paper ledger or the portfolio makes it obvious. Pure; the factor engine grades
and stores, the gateway serves.
"""
from __future__ import annotations

import math
from statistics import mean, pstdev
from typing import Any, Dict, List, Mapping, Optional, Sequence

# Trading days each horizon looks ahead (shared/utils/factor_model.HORIZON_SPECS).
HORIZON_DAYS = {"daily": 1, "weekly": 5, "monthly": 21,
                # Trial F in shadow: the weekly model on beta-adjusted returns.
                "weekly_trial_f": 5}
MIN_NAMES = 50                  # fewer graded names than this is not a cross-section
WATCH_PERIODS = 20              # the recent window the alarm reads


def _ranks(values: Sequence[float]) -> List[float]:
    """Average ranks (ties share their mean rank)."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        for k in range(i, j + 1):
            ranks[order[k]] = (i + j) / 2.0
        i = j + 1
    return ranks


def spearman(x: Sequence[float], y: Sequence[float]) -> Optional[float]:
    if len(x) != len(y) or len(x) < 3:
        return None
    rx, ry = _ranks(x), _ranks(y)
    mx, my = mean(rx), mean(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    den = math.sqrt(sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry))
    return num / den if den > 0 else None


def grade(percentiles: Mapping[str, float], start: Mapping[str, float], end: Mapping[str, float],
          spy_start: float, spy_end: float) -> Dict[str, Any]:
    """IC of one night's scores: rank correlation of each stock's percentile with
    its return over the horizon net of SPY's. Names without both prices are
    left out, not scored as zero."""
    if not (spy_start and spy_end):
        return {"ic": None, "n": 0}
    spy = math.log(spy_end / spy_start)
    xs, ys = [], []
    for t, p in percentiles.items():
        a, b = start.get(t), end.get(t)
        if a and b and a > 0 and b > 0:
            xs.append(float(p))
            ys.append(math.log(b / a) - spy)
    if len(xs) < MIN_NAMES:
        return {"ic": None, "n": len(xs)}
    ic = spearman(xs, ys)
    return {"ic": round(ic, 5) if ic is not None else None, "n": len(xs)}


def health(ics: Sequence[Optional[float]], expected_ic: Optional[float],
           horizon_days: int) -> Dict[str, Any]:
    """The recent live IC against the walk-forward's.

    Daily and weekly grades overlap (a weekly score graded every night shares
    four of five days with the next), so the t-statistic uses one in every
    `horizon_days` grades -- the overlap would otherwise inflate it."""
    recent = [x for x in list(ics)[-WATCH_PERIODS * horizon_days:] if x is not None]
    independent = recent[::max(1, horizon_days)]
    if len(independent) < 3:
        return {"periods": len(recent), "mean_ic": round(mean(recent), 4) if recent else None,
                "t": None, "expected_ic": expected_ic, "status": "warming_up"}
    m = mean(independent)
    sd = pstdev(independent)
    t = m / (sd / math.sqrt(len(independent))) if sd > 0 else None
    if t is not None and m < 0 and t <= -2:
        status = "broken"          # reliably pointing the wrong way
    elif expected_ic is not None and m < expected_ic / 2:
        status = "weak"            # working at under half of what was promised
    else:
        status = "ok"
    return {"periods": len(recent), "independent": len(independent), "mean_ic": round(m, 4),
            "t": round(t, 2) if t is not None else None, "expected_ic": expected_ic,
            "share_positive": round(sum(1 for x in independent if x > 0) / len(independent), 3),
            "status": status}


__all__ = ["HORIZON_DAYS", "MIN_NAMES", "grade", "health", "spearman"]
