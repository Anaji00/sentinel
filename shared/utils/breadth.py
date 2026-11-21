"""Market breadth by session from per-day counts: advancers and decliners, new 52-week highs and lows, and the share of stocks above their
50-day and 200-day averages, over a fixed universe (stocks with a known market cap of $1bn or more).

A session where far fewer stocks reported than usual is dropped, not shown: a half-loaded day reads as a crash in breadth (the 6 Nov
daily-bar outage left sessions at under half coverage). A share whose denominator is zero is None, never 0. Pure; no I/O.
"""
from __future__ import annotations

from statistics import median
from typing import Any, Dict, List, Mapping, Optional, Sequence

MIN_COVERAGE = 0.8       # of the median number of stocks reporting; below it the session is dropped


def _share(num: Any, den: Any) -> Optional[float]:
    try:
        n, d = float(num), float(den)
    except (TypeError, ValueError):
        return None
    return round(n / d, 4) if d > 0 else None


def build_series(rows: Sequence[Mapping[str, Any]], days: int) -> Dict[str, Any]:
    """`rows`: one per session with n (stocks with a close), n_prev (with a prior close), adv, dec, hi52, lo52, n_52 (with a full year), above50,
    n_50, above200, n_200. Returns the newest `days` complete sessions, oldest first, and which sessions were dropped for thin coverage."""
    usable = [r for r in rows if r.get("n")]
    if not usable:
        return {"series": [], "dropped_thin": []}
    typical = median(float(r["n"]) for r in usable)
    keep, dropped = [], []
    for r in sorted(usable, key=lambda r: str(r["day"])):
        (keep if float(r["n"]) >= MIN_COVERAGE * typical else dropped).append(r)
    out: List[Dict[str, Any]] = []
    for r in keep[-max(1, int(days)):]:
        adv, dec = int(r.get("adv") or 0), int(r.get("dec") or 0)
        prev = int(r.get("n_prev") or 0)
        out.append({
            "day": str(r["day"])[:10], "n": int(r["n"]), "advancers": adv, "decliners": dec, "unchanged": max(0, prev - adv - dec),
            "share_up": _share(adv, prev), "new_52w_highs": int(r["hi52"]) if r.get("n_52") else None,
            "new_52w_lows": int(r["lo52"]) if r.get("n_52") else None,
            "share_above_50d": _share(r.get("above50"), r.get("n_50")), "share_above_200d": _share(r.get("above200"), r.get("n_200")),
        })
    return {"series": out, "dropped_thin": [str(r["day"])[:10] for r in dropped][-10:]}


__all__ = ["MIN_COVERAGE", "build_series"]
