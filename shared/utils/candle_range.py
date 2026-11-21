"""Range presets for the chart: which bar size a range uses, which bars fall inside it, and the warm-up bars ahead of it for indicators.

1D is the latest session (New York calendar day of the newest bar); 1M and 3M are calendar windows on hourly bars; 1Y and 5Y are daily and
weekly. Warm-up bars are real bars before the window, marked, so a moving average can start on the window's first bar. When stored
history does not reach back to the window's start the result says so rather than showing a short series as if it were the range. Pure.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

_NY = ZoneInfo("America/New_York")
# range -> (timeframe, window in calendar days; None = the latest session, bars to ask for before warm-up)
RANGES: Dict[str, Dict[str, Any]] = {
    "1D": {"timeframe": "5m", "days": None, "fetch": 400},
    "1M": {"timeframe": "1h", "days": 30, "fetch": 800},
    "3M": {"timeframe": "1h", "days": 92, "fetch": 2300},
    "1Y": {"timeframe": "1d", "days": 365, "fetch": 260},
    "5Y": {"timeframe": "1w", "days": 1826, "fetch": 265},
}
MAX_WARMUP = 500


def _ts(c: Dict[str, Any]) -> Optional[datetime]:
    try:
        d = datetime.fromisoformat(str(c.get("ts")).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def plan(range_: str, warmup: int) -> Dict[str, Any]:
    spec = RANGES[range_]
    w = max(0, min(int(warmup), MAX_WARMUP))
    return {"timeframe": spec["timeframe"], "days": spec["days"], "fetch": spec["fetch"] + w, "warmup": w}


def split(candles: Sequence[Dict[str, Any]], range_: str, warmup: int, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Oldest-first candles with `warmup: true` on the bars ahead of the window, plus the window start and whether history falls short."""
    spec = RANGES[range_]
    rows = sorted(((t, c) for c in candles if (t := _ts(c)) is not None), key=lambda x: x[0])
    if not rows:
        return {"candles": [], "warmup_count": 0, "window_start": None, "history_limited": None}
    now = now or datetime.now(timezone.utc)
    last = rows[-1][0]
    if spec["days"] is None:
        day = last.astimezone(_NY).date()
        start = datetime.combine(day, datetime.min.time(), tzinfo=_NY).astimezone(timezone.utc)
    else:
        start = (min(now, last + timedelta(days=1)) - timedelta(days=spec["days"]))
        if spec["timeframe"] in ("1d", "1w"):
            start = datetime.combine(start.astimezone(_NY).date(), datetime.min.time(), tzinfo=_NY).astimezone(timezone.utc)
    inside = [(t, c) for t, c in rows if t >= start]
    w = max(0, min(int(warmup), MAX_WARMUP))
    older = [(t, c) for t, c in rows if t < start]
    before = older[len(older) - w:] if w else []
    out: List[Dict[str, Any]] = [{**c, "warmup": True} for _, c in before] + [{**c, "warmup": False} for _, c in inside]
    first_inside = inside[0][0] if inside else None
    slack = timedelta(days=7 if spec["timeframe"] == "1w" else 4)
    return {"candles": out, "warmup_count": len(before), "window_start": start.isoformat(),
            "history_limited": bool(first_inside is not None and spec["days"] is not None and first_inside > start + slack)}


__all__ = ["MAX_WARMUP", "RANGES", "plan", "split"]
