"""The "what changed since you last looked" digest: parse the client's timestamp safely, and shape each source as {count, items} with the items
capped. A source that could not be read reports status "error" and null counts; it never reads as "nothing changed". Pure; no I/O.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

MAX_DAYS_BACK = 7
MAX_ITEMS = 20


class SinceInvalid(ValueError):
    """The client's timestamp could not be used; the message is safe to show."""


def parse_since(raw: Optional[str], now: datetime, max_days: int = MAX_DAYS_BACK) -> datetime:
    """The client's `since`, as UTC, never in the future and never more than `max_days` back (older is clamped, not refused)."""
    if not raw:
        raise SinceInvalid("Send since as an ISO timestamp, for example 2026-11-12T09:30:00Z.")
    try:
        d = datetime.fromisoformat(str(raw).strip().replace("Z", "+00:00"))
    except ValueError:
        raise SinceInvalid("since is not an ISO timestamp.")
    d = d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    d = d.astimezone(timezone.utc)
    if d > now:
        raise SinceInvalid("since is in the future.")
    return max(d, now - timedelta(days=max_days))


def part(items: Sequence[Dict[str, Any]], total: Optional[int] = None, cap: int = MAX_ITEMS) -> Dict[str, Any]:
    """{status: "ok", count, items}: count is the true total when known (items are the newest `cap`)."""
    return {"status": "ok", "count": len(items) if total is None else int(total), "items": list(items)[:cap]}


def feed_item(kind: str, key: Any, ticker: Optional[str], at: Optional[str], title: str, precision: str = "second",
              detail: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """One feed entry with a stable id (`kind:key`, the same for the same underlying record on every call) so a screen can mark it seen."""
    return {"id": f"{kind}:{key}", "kind": kind, "ticker": ticker, "at": at, "at_precision": precision, "title": title, "detail": detail or {}}


def merge_feed(items: Sequence[Dict[str, Any]], limit: int = 50) -> List[Dict[str, Any]]:
    """Newest first across sources, one entry per id, undated entries last; ties broken by id so the order is stable between calls."""
    seen: Dict[str, Dict[str, Any]] = {}
    for it in items:
        seen.setdefault(it["id"], it)
    dated = sorted((i for i in seen.values() if i.get("at")), key=lambda i: (i["at"], i["id"]), reverse=True)
    undated = sorted((i for i in seen.values() if not i.get("at")), key=lambda i: i["id"])
    return (dated + undated)[: max(1, int(limit))]


def failed(note: str) -> Dict[str, Any]:
    return {"status": "error", "count": None, "items": [], "note": note}


def side(direction: Any) -> Optional[str]:
    try:
        d = int(direction)
    except (TypeError, ValueError):
        return None
    return "long" if d > 0 else "short" if d < 0 else None


def total_changes(sections: Dict[str, Dict[str, Any]]) -> Optional[int]:
    """Sum of the sources that were read; None when none were."""
    counts: List[int] = [s["count"] for s in sections.values() if s.get("status") == "ok" and s.get("count") is not None]
    return sum(counts) if counts else None


__all__ = ["feed_item", "merge_feed", "MAX_DAYS_BACK", "MAX_ITEMS", "SinceInvalid", "failed", "parse_since", "part", "side", "total_changes"]
