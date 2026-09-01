"""
shared/utils/freshness.py

How late an observation may be and still be worth reasoning about.

Every stage downstream of collection answers a question of the form "what is
happening now": correlation asks what co-occurs, the agents ask what to act on,
the reasoning service asks what it means. All three answers expire, and none of
them said so.

The consequence is only visible after an interruption, and interruptions are
routine -- a deploy, a crash, a rebalance, a laptop going to sleep. Whatever the
cause, the consumer resumes at its committed offset and works forward through a
backlog, emitting alerts and analyses about a market that has since closed and a
vessel that has since arrived. Measured on this deployment: the correlation
engine was 357,000 messages and 32 hours behind, and every cascade it fired
described the previous day. After one overnight suspend, the reasoning service
held 26,405 correlations and the radar orchestrator ~44,000.

Skipping is deliberately not seeking. The message is still consumed, still
committed and still counted; it simply produces no analysis. Nothing is dropped
from the offset record, so the lag figure stays honest, and a backlog drains at
parse speed rather than at inference speed.
"""

import os
from datetime import datetime, timezone
from typing import Any, Optional

# Default ceiling. Correlation and the agents answer "what is happening now" and
# want a tight bound; the reasoning service is slower by nature and is given a
# longer one by its caller.
DEFAULT_MAX_AGE_SEC = int(os.getenv("MAX_EVENT_AGE_SEC", "900"))


def occurred_at_of(item: Any) -> Optional[datetime]:
    """The timestamp on an event, cluster or raw dict, or None.

    Producers differ: some pass a pydantic model, some the dict it was decoded
    from, and the field is `occurred_at` or `detected_at` depending on whether
    the thing is an observation or a finding.
    """
    for attr in ("occurred_at", "detected_at", "created_at"):
        value = getattr(item, attr, None)
        if value is None and isinstance(item, dict):
            value = item.get(attr)
        if value is None:
            continue
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                continue
    return None


def is_stale(item: Any, max_age_sec: int = DEFAULT_MAX_AGE_SEC, now: Optional[datetime] = None) -> bool:
    """True when something arrived too late for its analysis to mean anything.

    Fails toward processing. An item with no usable timestamp is treated as
    current, because refusing it would silently drop a whole producer's output
    over a missing field -- a worse failure than analysing one item that turns
    out to be old.
    """
    occurred = occurred_at_of(item)
    if occurred is None:
        return False
    try:
        if occurred.tzinfo is None:
            # Naive timestamps are UTC by convention across this codebase.
            # Reading them as local time would make a day-old event look current
            # for the length of the offset.
            occurred = occurred.replace(tzinfo=timezone.utc)
        reference = now or datetime.now(timezone.utc)
        return (reference - occurred).total_seconds() > max_age_sec
    except (TypeError, ValueError, OverflowError):
        return False
