"""CFTC Commitments of Traders, disaggregated futures-only, from the CFTC public reporting API (Socrata dataset 72hh-3qpy, no key).

Pure parsing. `report_date` is the Tuesday the positions are as of; `published_at` is when the CFTC releases them: Friday 15:30 ET of the same week
(delayed to Monday after a Friday holiday, which this does not model). It is a scheduled time, recorded as such in `published_basis`, never earlier
than the true release, and the event study enters no earlier than the next session after the publication date, so a Monday release does not leak.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
PUBLISHED_BASIS = "scheduled_friday_1530_et"
API_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"

COLUMNS: Tuple[str, ...] = (
    "report_date", "contract_code", "market_name", "exchange_code", "commodity_name", "open_interest",
    "prod_long", "prod_short", "swap_long", "swap_short", "swap_spread", "mm_long", "mm_short", "mm_spread",
    "other_long", "other_short", "nonrept_long", "nonrept_short", "chg_mm_long", "chg_mm_short",
    "published_at", "published_basis",
)

_INT_FIELDS: Dict[str, str] = {
    "open_interest": "open_interest_all",
    "prod_long": "prod_merc_positions_long", "prod_short": "prod_merc_positions_short",
    "swap_long": "swap_positions_long_all", "swap_short": "swap__positions_short_all", "swap_spread": "swap__positions_spread_all",
    "mm_long": "m_money_positions_long_all", "mm_short": "m_money_positions_short_all", "mm_spread": "m_money_positions_spread",
    "other_long": "other_rept_positions_long", "other_short": "other_rept_positions_short",
    "nonrept_long": "nonrept_positions_long_all", "nonrept_short": "nonrept_positions_short_all",
    "chg_mm_long": "change_in_m_money_long_all", "chg_mm_short": "change_in_m_money_short_all",
}


def _int(x: Any) -> Optional[int]:
    try:
        return int(float(str(x).strip())) if x is not None and str(x).strip() != "" else None
    except (TypeError, ValueError):
        return None


def published_at(report_date: date) -> datetime:
    """Friday 15:30 ET of the report week (the Tuesday report date plus three days), in UTC."""
    release_day = report_date + timedelta(days=(4 - report_date.weekday()) % 7 or 3)
    return datetime(release_day.year, release_day.month, release_day.day, 15, 30, tzinfo=ET).astimezone(timezone.utc)


def parse_records(records: Iterable[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    """API records -> rows in COLUMNS order. A record without a date, a contract code or open interest is skipped."""
    out: List[Tuple[Any, ...]] = []
    for r in records or []:
        try:
            d = date.fromisoformat(str(r.get("report_date_as_yyyy_mm_dd"))[:10])
        except ValueError:
            continue
        code = str(r.get("cftc_contract_market_code") or "").strip()
        oi = _int(r.get("open_interest_all"))
        if not code or oi is None:
            continue
        vals = {k: _int(r.get(src)) for k, src in _INT_FIELDS.items()}
        out.append((d, code, str(r.get("contract_market_name") or "").strip(), str(r.get("cftc_market_code") or "").strip(),
                    str(r.get("commodity_name") or "").strip(),
                    *(vals[k] for k in COLUMNS[5:20]), published_at(d), PUBLISHED_BASIS))
    return out


__all__ = ["API_URL", "COLUMNS", "PUBLISHED_BASIS", "parse_records", "published_at"]
