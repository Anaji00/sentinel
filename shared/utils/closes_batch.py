"""A batch of recent daily closes for many tickers, so a table can draw a trend line per row from one request (frontend request, 19 Nov 2026). Pure.

Bounds: at most MAX_TICKERS tickers, and between MIN_DAYS and MAX_DAYS sessions each. A ticker with no bars is returned with an empty list, never dropped or invented."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

MAX_TICKERS = 100
MIN_DAYS = 5
MAX_DAYS = 250


def parse_tickers(raw: Optional[str], clean: Any) -> List[str]:
    """Distinct valid tickers from a comma-separated string, in order, capped at MAX_TICKERS; `clean` is the ticker validator (None for invalid)."""
    out: List[str] = []
    for part in str(raw or "").split(","):
        t = clean(part)
        if t and t not in out:
            out.append(t)
        if len(out) >= MAX_TICKERS:
            break
    return out


def clamp_days(days: Any) -> int:
    try:
        return max(MIN_DAYS, min(MAX_DAYS, int(days)))
    except (TypeError, ValueError):
        return 60


def build(tickers: Iterable[str], rows: Iterable[Mapping[str, Any]], days: int) -> Dict[str, List[float]]:
    """{ticker: the last `days` closes, oldest first}; `rows` carry ticker, day and close in any order. Every requested ticker is a key."""
    by: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for r in rows:
        try:
            by[str(r["ticker"]).upper()].append((str(r["day"])[:10], float(r["close"])))
        except (KeyError, TypeError, ValueError):
            continue
    return {t: [round(c, 4) for _, c in sorted(by.get(t, []))[-days:]] for t in tickers}


__all__ = ["MAX_DAYS", "MAX_TICKERS", "MIN_DAYS", "build", "clamp_days", "parse_tickers"]
