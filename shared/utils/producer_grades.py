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


def lift_table(rows: Iterable[Tuple[str, float]], baseline: Sequence[float], min_n: int = 10) -> List[Dict[str, object]]:
    """Each group's move ratio against a naive baseline's: the same measure for
    stocks that had a headline that day and no alert. "Moves more than usual"
    is the wrong bar for an alert about a stock in the news, because news days
    move more anyway (Boudoukh, Feldman, Kogan & Richardson 2019); an alert earns
    its place only by beating a headline. Welch's t on the two means, BH q."""
    from shared.utils.reaction_study import benjamini_hochberg

    base = [b for b in baseline if b is not None and math.isfinite(b)]
    if len(base) < min_n:
        return []
    bm = sum(base) / len(base)
    bv = sum((x - bm) ** 2 for x in base) / (len(base) - 1)
    by: Dict[str, List[float]] = {}
    for g, r in rows:
        if r is not None and math.isfinite(r):
            by.setdefault(g, []).append(r)
    out = []
    for g, xs in by.items():
        n = len(xs)
        m = sum(xs) / n
        v = sum((x - m) ** 2 for x in xs) / (n - 1) if n > 1 else 0.0
        se = math.sqrt(v / n + bv / len(base)) if n > 1 else 0.0
        t = (m - bm) / se if se > 0 else 0.0
        out.append({"group": g, "n": n, "mean_ratio": round(m, 3), "baseline_ratio": round(bm, 3),
                    "lift": round(m / bm, 3) if bm > 0 else None, "t": round(t, 2),
                    "p": _p_two_sided(t) if n >= min_n else None})
    tested = [r for r in out if r["p"] is not None]
    for r, q in zip(tested, benjamini_hochberg([r["p"] for r in tested])):
        r["q"] = round(q, 4)
        r["verdict"] = ("beats a headline" if r["t"] > 0 else "below a headline") if q <= 0.10 else "no better than a headline"
        r["p"] = round(r["p"], 4)
    for r in out:
        r.setdefault("q", None)
        r.setdefault("verdict", "too few")
    return sorted(out, key=lambda r: -r["n"])


def paired_lift_table(rows: Iterable[Tuple[str, str, float]], baseline: Iterable[Tuple[str, float]],
                      min_n: int = 10, min_base: int = 3) -> List[Dict[str, object]]:
    """Each group's move ratio against the same stock's headline days.

    lift_table compares an alert with other stocks' headline days, and the
    stocks alerts name are calmer than the average stock in the news: on 29 Oct
    2026 alerts pooled at 0.995 against 1.237, while within the same stock five
    families beat that stock's own headline days (block and options +0.11, t
    12.8; geopolitical cascade +0.29, t 8.0). Paired here: each alert minus
    the mean of its stock's headline-day ratios (stocks with at least
    `min_base` of them), a one-sample t on the differences, BH q. Same verdict
    labels as lift_table, so uninformative_rules reads either."""
    from shared.utils.reaction_study import benjamini_hochberg

    per: Dict[str, List[float]] = {}
    for t, b in baseline:
        if b is not None and math.isfinite(b):
            per.setdefault(t, []).append(b)
    base = {t: sum(v) / len(v) for t, v in per.items() if len(v) >= min_base}
    by: Dict[str, List[Tuple[float, float]]] = {}
    for g, t, r in rows:
        if r is not None and math.isfinite(r) and t in base:
            by.setdefault(g, []).append((r, base[t]))
    out = []
    for g, pairs in by.items():
        n = len(pairs)
        d = [r - b for r, b in pairs]
        m = sum(d) / n
        sd = math.sqrt(sum((x - m) ** 2 for x in d) / (n - 1)) if n > 1 else 0.0
        t = m / (sd / math.sqrt(n)) if sd > 0 else 0.0
        mr, mb = sum(r for r, _ in pairs) / n, sum(b for _, b in pairs) / n
        out.append({"group": g, "n": n, "mean_ratio": round(mr, 3), "baseline_ratio": round(mb, 3),
                    "lift": round(mr / mb, 3) if mb > 0 else None, "diff": round(m, 3), "t": round(t, 2),
                    "p": _p_two_sided(t) if n >= min_n else None})
    tested = [r for r in out if r["p"] is not None]
    for r, q in zip(tested, benjamini_hochberg([r["p"] for r in tested])):
        r["q"] = round(q, 4)
        r["verdict"] = ("beats a headline" if r["t"] > 0 else "below a headline") if q <= 0.10 else "no better than a headline"
        r["p"] = round(r["p"], 4)
    for r in out:
        r.setdefault("q", None)
        r.setdefault("verdict", "too few")
    return sorted(out, key=lambda r: -r["n"])


RETIRE_MIN_N = 50
# Distinct alert days a family needs before a positive verdict counts. Alerts on
# one day share that day's market: peer decoupling's 443 alerts and its 2.2x
# "moves more" all came from 29 Oct 2026, one session, and it was flagging.
MIN_ALERT_DAYS = 10


def _enough_days(grades: Dict[str, object], group: str) -> bool:
    """True when the family fired on MIN_ALERT_DAYS days, or the grades predate the count."""
    days = grades.get("family_days")
    return not isinstance(days, dict) or int(days.get(group) or 0) >= MIN_ALERT_DAYS


def prior_move(days: Sequence[date], closes: Sequence[float], spy_close: Dict[date, float],
               at: datetime) -> Optional[float]:
    """The session before entry, close to close, net of SPY: the move an alert
    arrives on, known before the first open after it."""
    i = entry_session(days, at)
    if i is None or i < 2:
        return None
    a, b = days[i - 2], days[i - 1]
    if not closes[i - 2] or not closes[i - 1] or not spy_close.get(a) or not spy_close.get(b):
        return None
    return math.log(closes[i - 1] / closes[i - 2]) - math.log(spy_close[b] / spy_close[a])


def follow_table(rows: Iterable[Tuple[str, Optional[float], Optional[float]]], min_n: int = 10) -> List[Dict[str, object]]:
    """[(group, prior move, signed abnormal move after)] -> per group: n, the share
    that continued the prior move, the mean move after aligned with the prior's
    sign in basis points, t, p, BH q.

    move_ratio grades size only, and a family that precedes 2x moves is no use
    to a trade until its direction is known: on 29 Oct 2026 peer-decoupling
    alerts reversed the prior session by 82 bp the next day (n 174, t -2.0)
    while block-and-options alerts continued it by 64 bp over five (n 1,794,
    t 4.3). Groups with a zero or missing prior are skipped."""
    from shared.utils.reaction_study import benjamini_hochberg

    by: Dict[str, List[Tuple[bool, float]]] = {}
    for g, prior, after in rows:
        if prior is None or after is None or prior == 0 or not math.isfinite(prior) or not math.isfinite(after):
            continue
        by.setdefault(g, []).append(((after > 0) == (prior > 0), after * (1.0 if prior > 0 else -1.0)))
    out = []
    for g, xs in by.items():
        n = len(xs)
        al = [a * 1e4 for _, a in xs]
        m = sum(al) / n
        sd = math.sqrt(sum((x - m) ** 2 for x in al) / (n - 1)) if n > 1 else 0.0
        t = m / (sd / math.sqrt(n)) if sd > 0 else 0.0
        out.append({"group": g, "n": n, "continued": round(sum(c for c, _ in xs) / n, 3), "follow_bp": round(m, 1),
                    "t": round(t, 2), "p": _p_two_sided(t) if n >= min_n else None})
    tested = [r for r in out if r["p"] is not None]
    for r, q in zip(tested, benjamini_hochberg([r["p"] for r in tested])):
        r["q"] = round(q, 4)
        r["verdict"] = ("continues" if r["t"] > 0 else "reverses") if q <= 0.10 else "no direction"
        r["p"] = round(r["p"], 4)
    for r in out:
        r.setdefault("q", None)
        r.setdefault("verdict", "too few")
    return sorted(out, key=lambda r: -r["n"])


def follow_tendencies(grades: Optional[Dict[str, object]], min_n: int = RETIRE_MIN_N) -> Dict[str, str]:
    """Family -> a sentence on the direction its alerts were followed in, for
    families with a significant verdict on enough alerts at 1 or 5 sessions."""
    out: Dict[str, List[str]] = {}
    for k, label in (("family_follow_1d", "next session"), ("family_follow_5d", "next 5 sessions")):
        for r in (grades or {}).get(k) or []:
            if (isinstance(r, dict) and int(r.get("n") or 0) >= min_n and r.get("verdict") in ("continues", "reverses")
                    and _enough_days(grades or {}, str(r.get("group")))):
                verb = "continued" if r["verdict"] == "continues" else "reversed"
                out.setdefault(str(r["group"]), []).append(
                    f"{verb} the prior session's move over the {label} ({float(r['follow_bp']):+.0f} bp, n {r['n']})")
    return {g: "; ".join(v) for g, v in out.items()}


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
    retired = {g for g, vs in seen.items() if vs and "moves more" not in vs}
    # And a family that moves more than usual but no more than a stock with a
    # headline and no alert: it has measured the news, not added to it.
    lifts: Dict[str, List[str]] = {}
    for k in ("family_lift_1d", "family_lift_5d"):
        for r in grades.get(k) or []:
            if isinstance(r, dict) and int(r.get("n") or 0) >= min_n:
                lifts.setdefault(str(r.get("group")), []).append(str(r.get("verdict")))
    retired |= {g for g, vs in lifts.items() if vs and "beats a headline" not in vs}
    # One day of alerts is no more evidence against a family than for it.
    return {g for g in retired if _enough_days(grades, rule_family(g))}


def move_flagging_families(grades: Optional[Dict[str, object]], min_n: int = RETIRE_MIN_N) -> set:
    """Families whose alerts precede bigger moves on both bars: more than the
    stock usually moves, and more than its own headline days. Size, not
    direction -- a risk input, not a trade signal."""
    if not grades:
        return set()

    def verdicts(prefix: str) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        for k in (f"{prefix}_1d", f"{prefix}_5d"):
            for r in grades.get(k) or []:
                if isinstance(r, dict) and int(r.get("n") or 0) >= min_n:
                    out.setdefault(str(r.get("group")), []).append(str(r.get("verdict")))
        return out

    usual, lift = verdicts("correlation_families"), verdicts("family_lift")
    return {g for g, vs in lift.items()
            if "beats a headline" in vs and "moves more" in usual.get(g, []) and _enough_days(grades, g)}


STRONG_LIFT = 1.15


def strong_move_families(grades: Optional[Dict[str, object]], min_lift: float = STRONG_LIFT,
                         min_n: int = RETIRE_MIN_N) -> set:
    """Move-flagging families whose alerts beat the stock's own headline days by
    at least min_lift at 1 or 5 sessions. B1's alert risk read every flagging
    family, and Semantic Convergence (lift 1.03) fires on most of the universe:
    on 30 Oct 2026, 91 of the book's 179 names traded at half weight."""
    flagging = move_flagging_families(grades, min_n)
    strong = set()
    for k in ("family_lift_1d", "family_lift_5d"):
        for r in (grades or {}).get(k) or []:
            if (isinstance(r, dict) and str(r.get("group")) in flagging and r.get("verdict") == "beats a headline"
                    and float(r.get("lift") or 0.0) >= min_lift and int(r.get("n") or 0) >= min_n):
                strong.add(str(r["group"]))
    return strong


def is_uninformative(rule_name: str, retired: set) -> bool:
    """The rule, or the family it belongs to, has been graded uninformative."""
    return rule_name in retired or rule_family(rule_name) in retired


__all__ = ["LOOKBACK", "RETIRE_MIN_N", "STRONG_LIFT", "follow_table", "follow_tendencies", "is_uninformative", "lift_table",
           "move_flagging_families", "move_ratio", "paired_lift_table", "placebo_rate", "prior_move", "ratio_table",
           "rule_family", "strong_move_families", "uninformative_rules"]
