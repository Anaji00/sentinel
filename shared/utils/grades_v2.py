"""Clustered, controlled grades for correlation rule families (grades v2): one unit per (family, ticker, detection date), each compared with the mean forward
move of random stocks on the same date, with the t taken over DATES.

The stored grades (shared/utils/producer_grades.py) treat every alert as independent and compare its forward move with 1.0. Alerts cluster on a few days (13 distinct days behind 2,939
peer-decoupling alerts) and a stock that fires an alert has often just moved, so those t values are far too large (research, 13 Nov 2026: of 19,749 units only peer cointegration survived
at one session; several families sit significantly BELOW the control). These numbers are additional keys only: nothing here retires or gates a rule (that needs its registered test).
Pure; no I/O. The caller supplies bars and units.
"""
from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from shared.utils.producer_grades import move_ratio

KS = (1, 5)
CONTROL = 120
MIN_DATES = 8
MIN_CONTROL = 30
ONE_SIDED_T = 1.65

Bars = Mapping[str, Tuple[Sequence[date], Sequence[float], Sequence[float]]]      # ticker -> (days, opens, closes)
Unit = Tuple[str, str, date]                                                      # (family, ticker, detection date)


def tstat(xs: Sequence[float]) -> Optional[float]:
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return None if sd == 0 else m / (sd / math.sqrt(n))


def verdict(t: Optional[float], excess: float, n_dates: int) -> str:
    if n_dates < MIN_DATES or t is None:
        return "too few dates"
    if t >= ONE_SIDED_T and excess > 0:
        return "moves more than the same-day control"
    if t <= -ONE_SIDED_T and excess < 0:
        return "moves less than the same-day control"
    return "no difference from the same-day control"


def grade_units(units: Mapping[Unit, datetime], bars: Bars, spy_by_day: Mapping[date, Tuple[float, float]], seed: int = 20261113,
                control: int = CONTROL, ks: Sequence[int] = KS, ratio: Callable[..., Any] = move_ratio) -> List[Dict[str, Any]]:
    """One row per (family, horizon k) with at least MIN_DATES detection dates. `units` maps (family, ticker, date) to the detection time."""
    rnd = random.Random(seed)
    pool = sorted(t for t in bars if t != "SPY" and len(bars[t][0]) > 90)

    def one(t: str, at: datetime, k: int) -> Optional[float]:
        b = bars.get(t)
        got = ratio(b[0], b[1], b[2], spy_by_day, at, k) if b else None
        return got[0] if got else None

    dates = sorted({u[2] for u in units})
    ctrl: Dict[Tuple[date, int], float] = {}
    for d in dates:
        at = datetime(d.year, d.month, d.day, 15, 0, tzinfo=timezone.utc)
        for k in ks:
            vals = [v for t in rnd.sample(pool, min(control, len(pool))) if (v := one(t, at, k)) is not None and v]
            if len(vals) >= MIN_CONTROL:
                ctrl[(d, k)] = sum(vals) / len(vals)
    diffs: Dict[Tuple[str, int], Dict[date, List[float]]] = defaultdict(lambda: defaultdict(list))
    raws: Dict[Tuple[str, int], Dict[date, List[float]]] = defaultdict(lambda: defaultdict(list))
    for (fam, t, d), at in units.items():
        for k in ks:
            v, c = one(t, at, k), ctrl.get((d, k))
            if v is not None and c is not None:           # a 0.0 move is a real observation (review L7)
                diffs[(fam, k)][d].append(v - c)
                raws[(fam, k)][d].append(v)
    out: List[Dict[str, Any]] = []
    for (fam, k), by in diffs.items():
        if len(by) < MIN_DATES:
            continue
        per_day = [sum(x) / len(x) for x in by.values()]
        raw_day = [sum(x) / len(x) for x in raws[(fam, k)].values()]
        n = sum(len(x) for x in by.values())
        top3 = sorted((len(x) for x in by.values()), reverse=True)[:3]
        t_stat = tstat(per_day)
        excess = sum(per_day) / len(per_day)
        out.append({"family": fam, "k": k, "units": n, "dates": len(by), "mean_ratio_by_date": round(sum(raw_day) / len(raw_day), 3),
                    "excess_over_control": round(excess, 3), "t_over_dates": None if t_stat is None else round(t_stat, 2),
                    "top3_date_share": round(sum(top3) / n, 3), "verdict": verdict(t_stat, excess, len(by))})
    return sorted(out, key=lambda r: (r["k"], -(r["t_over_dates"] if r["t_over_dates"] is not None else -99)))


__all__ = ["CONTROL", "KS", "MIN_DATES", "grade_units", "tstat", "verdict"]
