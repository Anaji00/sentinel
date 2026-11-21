"""Earnings calendar rows: listed report dates in a window, one row per stock, soonest first (larger companies first within a day).

Only dates the vendor calendar lists; dates inferred from an earlier 8-K are not mixed in (the snapshot route carries those, flagged as
estimates). A figure that is not on file is None, never 0. Pure; no I/O.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

MAX_DAYS = 30
MAX_ROWS = 500
HOURS = {"bmo": "bmo", "amc": "amc", "dmh": "during_market", "": None}


def as_day(v: Any) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def window(today: date, days: int) -> tuple[date, date]:
    n = max(1, min(int(days), MAX_DAYS))
    return today, today + timedelta(days=n)


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _positive(v: Any) -> Optional[float]:
    """A revenue estimate of zero is the vendor's placeholder for none, not a forecast of no revenue."""
    x = _num(v)
    return x if x is not None and x > 0 else None


def build_rows(raw: Iterable[Mapping[str, Any]], start: date, end: date, universe: Optional[Set[str]], caps: Mapping[str, float],
               min_cap: Optional[float], limit: int) -> List[Dict[str, Any]]:
    """Rows inside [start, end]; a stock listed on two dates keeps the earlier one. `universe` None means every stock; a minimum cap
    excludes stocks whose cap is unknown (unknown is not large)."""
    best: Dict[str, Dict[str, Any]] = {}
    for r in raw:
        t = str(r.get("ticker") or "").upper()
        d = as_day(r.get("report_date"))
        if not t or d is None or d < start or d > end or (universe is not None and t not in universe):
            continue
        cap = caps.get(t)
        if min_cap is not None and (cap is None or cap < min_cap):
            continue
        if t in best and best[t]["date"] <= d.isoformat():
            continue
        hour = str(r.get("hour") or "").lower()
        best[t] = {"ticker": t, "date": d.isoformat(), "hour": HOURS.get(hour), "eps_estimate": _num(r.get("eps_estimate")),
                   "revenue_estimate": _positive(r.get("revenue_estimate")), "estimated": False,
                   "market_cap": round(cap, 0) if cap else None}
    rows = sorted(best.values(), key=lambda x: (x["date"], -(x["market_cap"] or 0), x["ticker"]))
    return rows[: max(1, min(int(limit), MAX_ROWS))]


__all__ = ["MAX_DAYS", "MAX_ROWS", "as_day", "build_rows", "window"]
