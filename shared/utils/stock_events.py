"""Dated markers for a stock's price chart: earnings, filings and alerts (frontend request, 19 Nov 2026). Stored rows only; no model call, no inference.

Pure: rows in, a newest-first list of {date, kind, label, url?} out. A row without a usable date is dropped (counted by the caller via the returned `dropped` count),
never given today's date; at most MAX_ITEMS are returned, and each kind keeps its own share so a busy filer cannot push the earnings dates off the chart."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel

MAX_ITEMS = 400
MAX_PER_KIND = 200
KINDS = ("earnings", "filing", "alert")


class StockEvent(BaseModel):
    date: date
    kind: str
    label: str
    url: Optional[str] = None


def as_day(value: Any) -> Optional[date]:
    """A date from a date, datetime or ISO string; None when it cannot be read."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _num(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def earnings_label(eps_actual: Any, eps_estimate: Any) -> str:
    a, e = _num(eps_actual), _num(eps_estimate)
    if a is None:
        return "Earnings report"
    if e is None:
        return f"Earnings: EPS {a:.2f}"
    return f"Earnings: EPS {a:.2f} vs {e:.2f} expected"


def build(earnings: Sequence[Mapping[str, Any]], filings: Sequence[Mapping[str, Any]], alerts: Sequence[Mapping[str, Any]],
          days: int, today: Optional[date] = None) -> Tuple[List[StockEvent], int]:
    """(events newest first, rows dropped for lacking a date). Rows: earnings {report_date, eps_actual, eps_estimate}; filings {occurred_at, form_type, url};
    alerts {detected_at, rule_name}. Only events within `days` days before `today` (and not after it) are kept."""
    today = today or date.today()
    earliest = today - timedelta(days=max(1, int(days)))
    per_kind: Dict[str, List[StockEvent]] = {k: [] for k in KINDS}
    dropped = 0

    def add(kind: str, when: Any, label: str, url: Optional[str] = None) -> None:
        nonlocal dropped
        d = as_day(when)
        if d is None:
            dropped += 1
            return
        if earliest <= d <= today:
            per_kind[kind].append(StockEvent(date=d, kind=kind, label=label, url=url or None))

    for r in earnings:
        add("earnings", r.get("report_date"), earnings_label(r.get("eps_actual"), r.get("eps_estimate")))
    for r in filings:
        form = str(r.get("form_type") or "").strip()
        add("filing", r.get("occurred_at"), f"Filing: {form}" if form else "Filing", r.get("url"))
    for r in alerts:
        add("alert", r.get("detected_at"), f"Alert: {str(r.get('rule_name') or 'rule fired').strip()}")
    merged: List[StockEvent] = []
    for kind in KINDS:
        merged.extend(sorted(per_kind[kind], key=lambda e: e.date, reverse=True)[:MAX_PER_KIND])
    merged.sort(key=lambda e: (e.date, e.kind), reverse=True)
    return merged[:MAX_ITEMS], dropped


__all__ = ["KINDS", "MAX_ITEMS", "StockEvent", "as_day", "build", "earnings_label"]
