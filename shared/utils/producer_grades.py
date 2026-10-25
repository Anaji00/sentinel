"""
shared/utils/producer_grades.py

Grading the platform's other voices against prices: correlation rules,
scenarios and agent calls.

Until now each graded itself against its own outputs -- a scenario "confirmed"
when its own watch signal fired, a wargamer "hit" when the vessel it named
appeared in any event within a day. Here:

  - a correlation rule or a scenario that names a stock is asked whether the
    stock then moved more than it normally does: the absolute move net of SPY
    from the next open over 1 and 5 sessions, divided by that stock's typical
    move over the 60 sessions before. A rule that carries no information
    averages about 1.0. Tested per rule, with false-discovery control;
  - a directional agent call is graded net of SPY over its horizon;
  - an entity claim is compared with a placebo: how often the same entity
    appears in a window of the same length a week earlier.

Pure: bars in, grades out.
"""
from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from shared.utils.paper_ledger import entry_session

LOOKBACK = 60


def move_ratio(days: Sequence[date], opens: Sequence[float], closes: Sequence[float],
               spy_by_day: Dict[date, Tuple[float, float]], at: datetime, k: int) -> Optional[Tuple[float, float]]:
    """(|abnormal move| / its usual size, signed abnormal move) from the first open
    after `at` to the close k sessions on; None without enough history or bars."""
    i = entry_session(days, at)
    if i is None or i < LOOKBACK + k or i + k - 1 >= len(days):
        return None

    def window(t: int) -> Optional[float]:
        """Open of session t to the close k sessions on, net of SPY's same window."""
        s0 = spy_by_day.get(days[t], (None, None))[0]
        s1 = spy_by_day.get(days[t + k - 1], (None, None))[1]
        if not opens[t] or not closes[t + k - 1] or not s0 or not s1:
            return None
        return math.log(closes[t + k - 1] / opens[t]) - math.log(s1 / s0)

    # The baseline is the same construction over the stock's own recent past,
    # so the null mean is 1.0 whatever the shape of its returns: an open-to-
    # close move set against a close-to-close volatility (as first written)
    # left out the overnight gap and read every rule as "moves less".
    base = [abs(x) for x in (window(t) for t in range(i - LOOKBACK, i - k + 1)) if x is not None]
    abn = window(i)
    if abn is None or len(base) < LOOKBACK // 2:
        return None
    usual = sum(base) / len(base)
    if usual <= 0:
        return None
    return abs(abn) / usual, abn


def _p_two_sided(t: float) -> float:
    return math.erfc(abs(t) / math.sqrt(2.0))


def ratio_table(rows: Iterable[Tuple[str, float]], min_n: int = 10) -> List[Dict[str, object]]:
    """[(group, ratio)] -> per group: n, mean ratio, t of (ratio - 1), p, BH q."""
    from shared.utils.reaction_study import benjamini_hochberg

    by: Dict[str, List[float]] = {}
    for g, r in rows:
        if r is not None and math.isfinite(r):
            by.setdefault(g, []).append(r)
    out = []
    for g, xs in by.items():
        n = len(xs)
        mean = sum(xs) / n
        sd = math.sqrt(sum((x - mean) ** 2 for x in xs) / (n - 1)) if n > 1 else 0.0
        t = (mean - 1.0) / (sd / math.sqrt(n)) if sd > 0 and n > 1 else 0.0
        out.append({"group": g, "n": n, "mean_ratio": round(mean, 3), "t": round(t, 2),
                    "p": _p_two_sided(t) if n >= min_n else None})
    tested = [r for r in out if r["p"] is not None]
    for r, q in zip(tested, benjamini_hochberg([r["p"] for r in tested])):
        r["q"] = round(q, 4)
        r["verdict"] = ("moves more" if r["t"] > 0 else "moves less") if q <= 0.10 else "no difference"
        r["p"] = round(r["p"], 4)
    for r in out:
        r.setdefault("q", None)
        r.setdefault("verdict", "too few")
    return sorted(out, key=lambda r: -r["n"])


def placebo_rate(hits: Sequence[bool], placebo_hits: Sequence[bool]) -> Dict[str, Optional[float]]:
    """A claim's hit rate against the same claim over the day before it was made."""
    n = len(hits)
    if not n:
        return {"n": 0, "hit_rate": None, "placebo_rate": None, "skill": None}
    h = sum(hits) / n
    p = sum(placebo_hits) / max(1, len(placebo_hits))
    return {"n": n, "hit_rate": round(h, 3), "placebo_rate": round(p, 3), "skill": round(h - p, 3)}


RETIRE_MIN_N = 50
_ENTITY_SUFFIX = re.compile(r"\s*\([^()]*\)\s*$")


def rule_family(name: str) -> str:
    """A rule's name without the entity a cascade or cluster rule appends:
    "Geopolitical Cascade (Msft)" -> "Geopolitical Cascade". Graded per entity,
    each variant fired too rarely to be judged, so the family is graded too."""
    return _ENTITY_SUFFIX.sub("", str(name)).strip() or str(name)


def uninformative_rules(grades: Optional[Dict[str, object]], min_n: int = RETIRE_MIN_N) -> set:
    """Rules graded on enough stock alerts with no sign of preceding bigger
    moves, at one session or five; a family name stands for all its entity
    variants. Rules about ships, aircraft or places carry no stock and are
    never in the grades, so they are never retired here."""
    if not grades:
        return set()
    seen: Dict[str, List[str]] = {}
    for k in ("correlations_1d", "correlations_5d", "correlation_families_1d", "correlation_families_5d"):
        for r in grades.get(k) or []:
            if isinstance(r, dict) and int(r.get("n") or 0) >= min_n:
                seen.setdefault(str(r.get("group")), []).append(str(r.get("verdict")))
    return {g for g, vs in seen.items() if vs and "moves more" not in vs}


def is_uninformative(rule_name: str, retired: set) -> bool:
    """The rule, or the family it belongs to, has been graded uninformative."""
    return rule_name in retired or rule_family(rule_name) in retired


__all__ = ["LOOKBACK", "RETIRE_MIN_N", "is_uninformative", "move_ratio", "placebo_rate", "ratio_table", "rule_family",
           "uninformative_rules"]
