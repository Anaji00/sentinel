"""The paper account's header chip: equity now, and its change since the last stored daily equity row. Pure; a figure that cannot be known is
None, never 0 (a missing base is not a flat day)."""
from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Dict, Optional, Sequence, Tuple


def day_change(equity: Optional[float], rows: Sequence[Tuple[date, float]], today: date) -> Tuple[Optional[float], Optional[float], Optional[date]]:
    """(change, change_pct, base_day): equity against the newest stored row dated before `today`. `rows` are (day, equity) pairs."""
    if equity is None or not math.isfinite(equity):
        return None, None, None
    prior = [(d, float(e)) for d, e in rows if d < today and e is not None and float(e) > 0]
    if not prior:
        return None, None, None
    base_day, base = max(prior, key=lambda r: r[0])
    return round(equity - base, 2), round((equity / base - 1.0) * 100.0, 2), base_day


def summary(account: Dict[str, Any], rows: Sequence[Tuple[date, float]], today: date, now: datetime) -> Dict[str, Any]:
    raw = account.get("portfolio_value")
    equity = float(raw) if raw is not None and math.isfinite(float(raw)) and float(raw) > 0 else None
    change, pct, base_day = day_change(equity, rows, today)
    count = account.get("positions_count")
    return {
        "equity": round(equity, 2) if equity is not None else None,
        "day_change": change,
        "day_change_pct": pct,
        "base_date": base_day.isoformat() if base_day else None,
        "positions_count": int(count) if count is not None else None,
        "as_of": now.isoformat(),
    }


__all__ = ["day_change", "summary"]
