"""Which stocks the gateway keeps warm: the ones people follow, then the most traded (frontend/parent request, 20 Nov 2026). Pure.

The first stock-page load after a gateway restart paid for the snapshot, the positioning and the regime reads cold. A background pass reads them ahead of time for a bounded
set: every account's followed stocks (interleaved, so one long list cannot crowd out the others), then the top names by dollar volume, at most MAX_NAMES in all. No model call
is involved; the reads are the ones the stock page already makes."""
from __future__ import annotations

from typing import Any, Callable, Iterable, List, Optional, Sequence

MAX_NAMES = 80
TOP_BY_DOLLAR_VOLUME = 30


def choose(followed_lists: Iterable[Sequence[Any]], top_by_dollar_volume: Sequence[Any], clean: Callable[[str], Optional[str]],
           cap: int = MAX_NAMES) -> List[str]:
    """Distinct cleaned tickers: followed first, taking one name from each account in turn, then the most traded, capped at `cap`. An unusable entry is skipped."""
    out: List[str] = []
    seen = set()

    def add(raw: Any) -> None:
        t = clean(str(raw)) if raw is not None else None
        if t and t not in seen and len(out) < cap:
            seen.add(t)
            out.append(t)

    lists = [list(x) for x in followed_lists if x]
    for depth in range(max((len(x) for x in lists), default=0)):
        for lst in lists:
            if depth < len(lst):
                add(lst[depth])
    for t in top_by_dollar_volume:
        add(t)
    return out


__all__ = ["MAX_NAMES", "TOP_BY_DOLLAR_VOLUME", "choose"]
