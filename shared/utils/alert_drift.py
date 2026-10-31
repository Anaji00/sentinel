"""
shared/utils/alert_drift.py

A trial feature from the alert direction grades (producer_grades.follow_table):
after an alert from a family graded to continue the prior session's move over
the next five, the sign of that prior move. On 30 Oct 2026 Semantic
Convergence continued +57 bp over 5 sessions (n 6,644, t 8.5) and Equity Block
& Options +64 bp (n 1,809, t 4.3), against +12 bp (t 1.7) for headline days.

The families are fixed here, before the trial, because they were chosen on the
same alerts the trial would otherwise be scored on; the feature is recorded
forward only for the same reason. Pure.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

FEATURE = "ev_alert_drift_5"
FAMILIES = frozenset({"Cross-Domain Semantic Convergence", "Equity Block & Options Convergence"})
FIRST_DAY = date(2026, 10, 30)


def drift_rows(alerts: Iterable[Tuple[str, date, str]], prior: Dict[Tuple[str, date], Optional[float]],
               since: date = FIRST_DAY) -> List[Tuple[str, date, float]]:
    """(ticker, day, +1 or -1) for each stock and day with an alert from FAMILIES:
    the sign of the session before the alert's entry, net of SPY. A missing or
    flat prior gives no row; a day with alerts of both signs keeps the larger move."""
    best: Dict[Tuple[str, date], float] = {}
    for ticker, day, family in alerts:
        if family not in FAMILIES or day < since:
            continue
        p = prior.get((ticker, day))
        if p is None or p == 0 or not math.isfinite(p):
            continue
        k = (ticker, day)
        if k not in best or abs(p) > abs(best[k]):
            best[k] = p
    return [(t, d, 1.0 if p > 0 else -1.0) for (t, d), p in sorted(best.items())]


__all__ = ["FAMILIES", "FEATURE", "FIRST_DAY", "drift_rows"]
