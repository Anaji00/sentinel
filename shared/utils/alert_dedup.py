"""
shared/utils/alert_dedup.py

One alert per stock, rule family and trading session, unless a later one is more severe.

Rule-matched alerts repeated for as long as their trigger kept arriving: three stored copies of one rule title fired 3
times each in 12 minutes on the same stocks, and a single "Yield Drop" rule fired 1,995 times on 981 stocks in 48 hours
(reports/Divergence alert.md). The key is (entity, rule family, New York session date). A repeat is let through only
when its tier outranks the one already published for that key, so an escalation is never swallowed.

The family is the rule's title with filler words removed and the rest sorted, so copies of one rule that differ in id
(a rule is re-minted when its definition changes) share a key.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
KEY = "sentinel:alert_dedup:{entity}:{family}:{session}"
TTL_SEC = 3 * 86400
TIER_RANK = {"WATCH": 0, "ALERT": 1, "ELEVATED": 2, "INTELLIGENCE": 3, "CRITICAL": 4}
_FILLER = {"and", "to", "the", "of", "on", "a", "an", "for", "in", "with", "leads", "led"}


def rule_family(rule_name: Optional[str]) -> str:
    words = re.findall(r"[a-z0-9]+", (rule_name or "").lower())
    kept = sorted({w for w in words if w not in _FILLER})
    return "_".join(kept)[:80] or "unnamed"


def session_date(when: Optional[datetime]) -> str:
    """The New York calendar date of a moment; a missing or naive time is read as now, UTC."""
    if when is None:
        when = datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when.astimezone(NY).date().isoformat()


def tier_rank(tier: Any) -> int:
    name = getattr(tier, "value", tier)
    return TIER_RANK.get(str(name).upper(), 0)


def dedup_key(entity_id: Optional[str], rule_name: Optional[str], when: Optional[datetime]) -> str:
    return KEY.format(entity=str(entity_id or "UNKNOWN").upper(), family=rule_family(rule_name), session=session_date(when))


def outranks(previous_rank: Optional[int], new_rank: int) -> bool:
    """True when nothing was published for the key, or the new alert is strictly more severe."""
    return previous_rank is None or new_rank > previous_rank


async def admit(raw_redis: Any, entity_id: Optional[str], rule_name: Optional[str], when: Optional[datetime], tier: Any) -> bool:
    """Record the alert and say whether to publish it. A store failure raises; the caller counts it and lets the alert through."""
    key = dedup_key(entity_id, rule_name, when)
    rank = tier_rank(tier)
    if await raw_redis.set(key, str(rank), nx=True, ex=TTL_SEC):
        return True
    prev = await raw_redis.get(key)
    try:
        prev_rank = int(prev.decode() if isinstance(prev, bytes) else prev)
    except (TypeError, ValueError):
        prev_rank = None
    if outranks(prev_rank, rank):
        await raw_redis.set(key, str(rank), ex=TTL_SEC)
        return True
    return False


__all__ = ["admit", "dedup_key", "outranks", "rule_family", "session_date", "tier_rank"]
