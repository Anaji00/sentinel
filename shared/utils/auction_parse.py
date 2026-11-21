"""US Treasury auction results from the TreasuryDirect web service (`/TA_WS/securities/search`, no key).

Pure parsing. One row per auction (CUSIP and competitive close date). Bidder shares are each class's competitive amount awarded over the sum of the
three classes (indirect, direct, primary dealers). `published_at` is the competitive close time (11:30 or 13:00 ET) plus ten minutes: Treasury posts
results within minutes of the close, and the later time is the conservative one. The tail against the when-issued yield is not in this feed, so it
is not stored. Nothing here is a number the platform acts on.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
SEARCH_URL = "https://www.treasurydirect.gov/TA_WS/securities/search"
PUBLISHED_BASIS = "close_time_plus_10m"
RELEASE_LAG = timedelta(minutes=10)

COLUMNS: Tuple[str, ...] = (
    "cusip", "auction_date", "security_type", "security_term", "issue_date", "close_time", "bid_to_cover", "high_yield", "median_yield",
    "offering_amount", "indirect_awarded", "direct_awarded", "dealer_awarded", "indirect_share", "direct_share", "dealer_share",
    "reopening", "cash_management", "published_at", "published_basis",
)


def _num(x: Any) -> Optional[float]:
    try:
        s = str(x).strip()
        return float(s) if s != "" else None
    except (TypeError, ValueError):
        return None


def _day(x: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(x)[:10])
    except ValueError:
        return None


def _clock(x: Any) -> Optional[time]:
    try:
        return datetime.strptime(str(x).strip(), "%I:%M %p").time()
    except ValueError:
        return None


def published_at(auction_day: date, close_clock: time) -> datetime:
    return (datetime.combine(auction_day, close_clock, tzinfo=ET) + RELEASE_LAG).astimezone(timezone.utc)


def parse_records(records: Iterable[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    """Search records -> rows in COLUMNS order. An auction without a bid-to-cover ratio has not happened yet and is skipped; so is one with no
    CUSIP, close date or close time."""
    out: List[Tuple[Any, ...]] = []
    for r in records or []:
        cusip = str(r.get("cusip") or "").strip()
        d = _day(r.get("competitiveAuctionCloseDate"))
        clock = _clock(r.get("competitiveAuctionCloseTime"))
        btc = _num(r.get("bidToCoverRatio"))
        if not cusip or d is None or clock is None or btc is None or btc <= 0:
            continue
        ind = _num(r.get("indirectBiddersCompetitiveParAwarded"))
        dire = _num(r.get("directBiddersCompetitiveParAwarded"))
        dealer = _num(r.get("primaryDealersCompetitiveParAwarded"))
        parts = [v for v in (ind, dire, dealer)]
        total = sum(parts) if all(v is not None for v in parts) else None
        share = (lambda v: round(v / total, 5) if total and v is not None else None)
        hy = _num(r.get("highYield"))
        out.append((cusip, d, str(r.get("securityType") or "").strip(), str(r.get("securityTerm") or "").strip(),
                    _day(r.get("issueDate")), clock.strftime("%H:%M"), btc, hy if hy is not None else _num(r.get("highRate")),
                    _num(r.get("medianYield")), _num(r.get("offeringAmount")), ind, dire, dealer, share(ind), share(dire), share(dealer),
                    str(r.get("reopening") or "").strip().lower() == "yes",
                    str(r.get("cashManagementBill") or "").strip().lower() == "yes",
                    published_at(d, clock), PUBLISHED_BASIS))
    return out


__all__ = ["COLUMNS", "PUBLISHED_BASIS", "SEARCH_URL", "parse_records", "published_at"]
