"""
services/collector-filings/short_interest.py

FINRA consolidated short interest, every settlement since 2020, into
`short_interest` for the cross-sectional model.

Days-to-cover -- shares short over average daily volume -- is the short-side
measure with the strongest record: Hong, Li, Ni, Scheinkman & Yan (2015) found
a long-short on it earned 1.2% a month and was positive in 23 of 25 years,
beating the plain short ratio, because a short that would take many days to
unwind is one the seller must be confident in.

FINRA publishes it twice a month, about eight business days after each
settlement date, at api.finra.org without a key; five years of history are
served. The model reads each settlement only from SHORT_INTEREST_AVAILABLE_LAG_DAYS
after it, so no day sees a figure before it was public.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.filings.short_interest")

FINRA_SI_URL = "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
# A generic agent: the SEC contact address is for EDGAR and goes nowhere else.
FINRA_HEADERS = {"Content-Type": "application/json", "Accept": "application/json",
                 "User-Agent": "Sentinel-Intelligence-Platform/1.0"}
PAGE = 5000
# Any symbol listed throughout gives the full list of settlement dates.
CALENDAR_SYMBOL = "AAPL"
REQUEST_PAUSE_SEC = 1.0
TOPUP_INTERVAL_SEC = 12 * 3600

UPSERT_SQL = """
    INSERT INTO short_interest (ticker, settlement_date, short_qty, prev_short_qty,
                                avg_daily_volume, days_to_cover, change_pct)
    VALUES ($1, $2::date, $3, $4, $5, $6, $7)
    ON CONFLICT (ticker, settlement_date) DO UPDATE SET
        short_qty = EXCLUDED.short_qty, prev_short_qty = EXCLUDED.prev_short_qty,
        avg_daily_volume = EXCLUDED.avg_daily_volume, days_to_cover = EXCLUDED.days_to_cover,
        change_pct = EXCLUDED.change_pct
"""

Row = Tuple[str, date, float, Optional[float], Optional[float], Optional[float], Optional[float]]


def _num(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def rows_from(records: Iterable[Dict[str, Any]], universe: Set[str]) -> List[Row]:
    """FINRA records -> table rows for tickers in the universe.

    FINRA's own daysToCoverQuantity is rounded to a whole day, which flattens
    the measure for most liquid names to 1; it is recomputed from the parts."""
    out: List[Row] = []
    for r in records or []:
        t = str(r.get("symbolCode") or "").upper().strip()
        if not t or t not in universe:
            continue
        try:
            d = date.fromisoformat(str(r.get("settlementDate"))[:10])
        except ValueError:
            continue
        short = _num(r.get("currentShortPositionQuantity"))
        if short is None or short < 0:
            continue
        adv = _num(r.get("averageDailyVolumeQuantity"))
        dtc = short / adv if adv and adv > 0 else None
        out.append((t, d, short, _num(r.get("previousShortPositionQuantity")), adv,
                    round(dtc, 4) if dtc is not None else None, _num(r.get("changePercent"))))
    return out


async def _post(session: Any, body: Dict[str, Any]) -> List[Dict[str, Any]]:
    async with session.post(FINRA_SI_URL, json=body, headers=FINRA_HEADERS) as resp:
        if resp.status != 200:
            raise RuntimeError(f"FINRA short interest HTTP {resp.status}")
        data = await resp.json(content_type=None)
    return data if isinstance(data, list) else []


async def settlement_dates(session: Any) -> List[date]:
    rows = await _post(session, {"limit": PAGE, "compareFilters": [
        {"compareType": "EQUAL", "fieldName": "symbolCode", "fieldValue": CALENDAR_SYMBOL}]})
    out = set()
    for r in rows:
        try:
            out.add(date.fromisoformat(str(r.get("settlementDate"))[:10]))
        except ValueError:
            continue
    return sorted(out)


async def fetch_settlement(session: Any, d: date, universe: Set[str]) -> List[Row]:
    rows: List[Row] = []
    offset = 0
    while True:
        page = await _post(session, {"limit": PAGE, "offset": offset, "compareFilters": [
            {"compareType": "EQUAL", "fieldName": "settlementDate", "fieldValue": d.isoformat()}]})
        rows.extend(rows_from(page, universe))
        if len(page) < PAGE:
            return rows
        offset += PAGE
        await asyncio.sleep(REQUEST_PAUSE_SEC)


async def sync(session: Any, db: Any) -> Dict[str, int]:
    """Every settlement FINRA serves that the table lacks, for every ticker with prices."""
    universe = {str(r["ticker"]).upper() for r in await db.query("SELECT DISTINCT ticker FROM daily_bars") or []}
    have = {r["settlement_date"] for r in
            await db.query("SELECT DISTINCT settlement_date FROM short_interest") or []}
    stats = {"settlements": 0, "rows": 0}
    if not universe:
        return stats
    for d in await settlement_dates(session):
        if d in have:
            continue
        rows = await fetch_settlement(session, d, universe)
        if rows:
            await db.execute_many(UPSERT_SQL, rows)
        stats["settlements"] += 1
        stats["rows"] += len(rows)
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return stats


async def run_forever() -> None:
    """Its own HTTP session: the collector's is capped at ten connections that
    EDGAR and the 13F downloads keep busy, and on it this loop waited for a
    free connection indefinitely. Separate also keeps FINRA off the SEC client."""
    import aiohttp
    from shared.db import get_timescale

    await asyncio.sleep(120)
    timeout = aiohttp.ClientTimeout(total=120)
    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                stats = await sync(session, await get_timescale())
                if stats["settlements"]:
                    logger.info("FINRA short interest: %s new settlements, %s rows",
                                stats["settlements"], stats["rows"])
        except Exception as e:
            swallowed("collector_filings.short_interest", e, logger)
        await asyncio.sleep(TOPUP_INTERVAL_SEC)


__all__ = ["rows_from", "settlement_dates", "fetch_settlement", "sync", "run_forever"]
