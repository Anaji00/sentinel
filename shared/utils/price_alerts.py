"""Price alerts: validate a new one, and decide from the bars since it was last checked whether the price crossed its level.

Evaluated on read from stored minute bars (high and low), so a crossing between two polls is not missed and no background job is needed.
An alert whose ticker has no bars since it was checked simply does not fire; nothing is invented. Pure; no I/O.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

MAX_ACTIVE_PER_OWNER = 50
MAX_NOTE = 140
DIRECTIONS = ("above", "below")


def as_dt(v: Any) -> Optional[datetime]:
    """A timezone-aware datetime from a datetime or an ISO string (the pool returns timestamps as strings); None when it is neither."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


class AlertInvalid(ValueError):
    """A new alert that cannot be stored; the message is safe to show."""


def validate_new(ticker: Optional[str], direction: str, level: Any, note: Optional[str]) -> Dict[str, Any]:
    if not ticker:
        raise AlertInvalid("Name a ticker.")
    if direction not in DIRECTIONS:
        raise AlertInvalid("Direction must be 'above' or 'below'.")
    try:
        lv = float(level)
    except (TypeError, ValueError):
        raise AlertInvalid("The level must be a number.")
    if not math.isfinite(lv) or lv <= 0:
        raise AlertInvalid("The level must be a positive price.")
    return {"ticker": ticker, "direction": direction, "level": round(lv, 6), "note": (note or "").strip()[:MAX_NOTE] or None}


def crossed(direction: str, level: float, high: Optional[float], low: Optional[float]) -> bool:
    if direction == "above":
        return high is not None and high >= level
    if direction == "below":
        return low is not None and low <= level
    return False


def evaluate(alerts: Sequence[Mapping[str, Any]], extremes: Mapping[str, Tuple[Optional[float], Optional[float], Optional[datetime]]]
             ) -> List[Dict[str, Any]]:
    """Alerts (id, ticker, direction, level) against each ticker's (high, low, last bar time) since its alerts were checked.
    Returns the ones that fired with the price that did it (the high for 'above', the low for 'below')."""
    fired: List[Dict[str, Any]] = []
    for a in alerts:
        high, low, at = extremes.get(a["ticker"], (None, None, None))
        if crossed(a["direction"], float(a["level"]), high, low):
            fired.append({"id": a["id"], "price": high if a["direction"] == "above" else low, "at": at})
    return fired


__all__ = ["AlertInvalid", "as_dt", "DIRECTIONS", "MAX_ACTIVE_PER_OWNER", "MAX_NOTE", "crossed", "evaluate", "validate_new"]
