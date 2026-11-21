"""When a filing became public, for the event's `occurred_at`: the SEC's acceptance time, not the moment the collector noticed it.

8-K and Form 4 events were stamped with collection time, so SEC-to-platform latency could not be measured and a filing collected late looked new. The
acceptance time is used when it parses, is not in the future, and is not older than `max_age`; otherwise collection time stands (and the caller keeps the
raw value in the payload). Pure.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

MAX_AGE = timedelta(days=14)
FUTURE_SLACK = timedelta(minutes=5)


def parse_accepted(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return (ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def occurred_at(raw: Any, now: datetime, max_age: timedelta = MAX_AGE) -> Tuple[datetime, bool]:
    """(time to stamp, whether it is the acceptance time). Falls back to `now` for a missing, future or very old value."""
    ts = parse_accepted(raw)
    if ts is None or ts > now + FUTURE_SLACK or ts < now - max_age:
        return now, False
    return ts, True


__all__ = ["FUTURE_SLACK", "MAX_AGE", "occurred_at", "parse_accepted"]
