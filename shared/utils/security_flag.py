"""Whether a primary entity is a security (a ticker with a recent daily bar), so the dashboard links to a stock page only when it is. Pure.

A headline-family correlation's primary entity is often a concept, person or country (layer audit 2.1: 26 to 63% of them); a ticker with a daily bar in the last sessions is
the platform's own test of "a stock page exists for this"."""
from __future__ import annotations

from typing import Any, Iterable, Optional, Set

SESSIONS_WINDOW_DAYS = 8       # calendar days that cover the last five sessions across a weekend and a holiday


def normalize(entity_id: Any) -> Optional[str]:
    """The upper-cased ticker text of an entity id, or None for an empty or implausibly long id."""
    text = str(entity_id or "").strip().upper()
    return text if text and len(text) <= 12 else None


def candidate_ids(rows: Iterable[Any]) -> list:
    """Distinct normalised entity ids to look up, in first-seen order."""
    seen, out = set(), []
    for r in rows:
        n = normalize(r)
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def is_security(entity_id: Any, with_bars: Set[str]) -> bool:
    """True when the entity id is a ticker present in `with_bars` (tickers with a bar in the last SESSIONS_WINDOW_DAYS days)."""
    n = normalize(entity_id)
    return bool(n and n in with_bars)


__all__ = ["SESSIONS_WINDOW_DAYS", "candidate_ids", "is_security", "normalize"]
