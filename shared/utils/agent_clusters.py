"""Date-clustered grades for agent calls: one observation per (agent, horizon, detection date), t over dates.

The stored agent_skill / agent_calls grades count every call as independent, so one regime week carried most of an agent's sample (quant_trading_engine hit 0.448,
t -3.6 on 402 calls; clustered by date, t -1.37 on 29 dates; stock_correlation_agent.baseline stored t 6.3, clustered 1.87 on 7 dates). Here each date contributes
the mean signed excess return over SPY (positive = the call was right; `agent_predictions.excess_spy`), and a row with fewer than MIN_DATES dates is "not judged",
the registered rule (A2: 20 independent days), which is different from "no skill". Additive: nothing is gated on it. Pure; no I/O.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.utils.grades_v2 import tstat

MIN_DATES = 20
MIN_SHOWN_DATES = 3
# One-sided 5% across about 8 agents (Bonferroni): the macro engine's 24-hour t of 2.03 is the best of eight tried and does not clear it.
MULTIPLE_T = 2.5

Call = Tuple[str, Optional[int], date, Optional[float]]      # (agent, horizon hours, detection date, signed excess return or None)


def signed_excess(direction: str, excess: Optional[float]) -> Optional[float]:
    """Positive = the call was right. Flat or unknown directions, and calls with no excess return, give None."""
    if excess is None:
        return None
    if direction == "up":
        return float(excess)
    if direction == "down":
        return -float(excess)
    return None


def verdict(t: Optional[float], mean: float, n_dates: int) -> str:
    if n_dates < MIN_DATES or t is None:
        return "not judged: too few dates"
    if t >= MULTIPLE_T and mean > 0:
        return "beats the market on the date-clustered test"
    if t <= -MULTIPLE_T and mean < 0:
        return "trails the market on the date-clustered test"
    return "no difference from the market"


def grade_agents(calls: Iterable[Call]) -> List[Dict[str, Any]]:
    """One row per (agent, horizon or 'all') with at least MIN_SHOWN_DATES dates; calls with no excess return are counted per agent, not graded."""
    by: Dict[Tuple[str, Optional[int]], Dict[date, List[float]]] = defaultdict(lambda: defaultdict(list))
    skipped: Dict[str, int] = defaultdict(int)
    for agent, horizon, day, s in calls:
        if s is None:
            skipped[agent] += 1
            continue
        by[(agent, None)][day].append(float(s))
        by[(agent, horizon)][day].append(float(s))
    out: List[Dict[str, Any]] = []
    for (agent, h), days in by.items():
        if len(days) < MIN_SHOWN_DATES:
            continue
        per_day = [sum(v) / len(v) for v in days.values()]
        n = sum(len(v) for v in days.values())
        mean = sum(per_day) / len(per_day)
        t = tstat(per_day)
        top3 = sum(sorted((len(v) for v in days.values()), reverse=True)[:3]) / n
        out.append({"agent": agent, "horizon_h": h, "calls": n, "dates": len(days), "mean_signed_bps": round(1e4 * mean, 1),
                    "t_over_dates": None if t is None else round(t, 2), "top3_date_share": round(top3, 3),
                    "hit_share_by_date": round(sum(sum(1 for x in v if x > 0) / len(v) for v in days.values()) / len(days), 3),
                    "skipped_no_excess": skipped.get(agent, 0) if h is None else None, "verdict": verdict(t, mean, len(days))})
    return sorted(out, key=lambda r: (r["agent"], r["horizon_h"] is not None, r["horizon_h"] or 0))


__all__ = ["MIN_DATES", "grade_agents", "signed_excess", "verdict"]
