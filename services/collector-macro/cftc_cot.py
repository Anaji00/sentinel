"""
services/collector-macro/cftc_cot.py

CFTC Commitments of Traders (disaggregated, futures only) into `cftc_cot`, every contract and every week since 2006, with the release time recorded.
Free public API, no key. Measurement only: nothing here feeds an alert, a Kafka topic or the recommendation engine; the event study is
scripts/study_cftc_cot.py. The CFTC releases on Fridays at 15:30 ET with positions as of the Tuesday before.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from shared.utils.cot_parse import API_URL, COLUMNS, parse_records
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.macro.cftc_cot")

FIRST_YEAR = 2006
PAGE = 5000
REQUEST_PAUSE_SEC = 1.0
TOPUP_INTERVAL_SEC = 3 * 3600
# A week's figures are rewritten rarely; the look-back picks up a late week without re-reading the history.
LOOKBACK_DAYS = 28

INSERT_SQL = f"""
    INSERT INTO cftc_cot ({", ".join(COLUMNS)})
    VALUES ({", ".join(f"${i + 1}" for i in range(len(COLUMNS)))})
    ON CONFLICT (contract_code, report_date) DO NOTHING
"""


def where_clause(start: date, end_exclusive: Optional[date]) -> str:
    cond = f"report_date_as_yyyy_mm_dd >= '{start.isoformat()}T00:00:00.000'"
    if end_exclusive is not None:
        cond += f" AND report_date_as_yyyy_mm_dd < '{end_exclusive.isoformat()}T00:00:00.000'"
    return cond


async def _get(session: Any, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    async with session.get(API_URL, params=params) as resp:
        if resp.status != 200:
            raise RuntimeError(f"CFTC API HTTP {resp.status}")
        data = await resp.json(content_type=None)
    return data if isinstance(data, list) else []


async def fetch_range(session: Any, start: date, end_exclusive: Optional[date]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        page = await _get(session, {"$where": where_clause(start, end_exclusive), "$order": "report_date_as_yyyy_mm_dd,cftc_contract_market_code",
                                    "$limit": PAGE, "$offset": offset})
        out.extend(page)
        if len(page) < PAGE:
            return out
        offset += PAGE
        await asyncio.sleep(REQUEST_PAUSE_SEC)


async def sync(session: Any, db: Any, today: Optional[date] = None) -> Dict[str, int]:
    """First run: every year since 2006, one year per request series. After that: the last four weeks."""
    today = today or date.today()
    last = await db.query("SELECT MAX(report_date) AS d FROM cftc_cot")
    have = last[0]["d"] if last and last[0]["d"] else None
    if have is None:
        windows = [(date(y, 1, 1), date(y + 1, 1, 1)) for y in range(FIRST_YEAR, today.year + 1)]
    else:
        windows = [(have - timedelta(days=LOOKBACK_DAYS), None)]
    stats = {"records": 0, "rows": 0}
    for start, end in windows:
        rows = parse_records(await fetch_range(session, start, end))
        for i in range(0, len(rows), 2000):
            await db.execute_many(INSERT_SQL, rows[i:i + 2000])
        stats["records"] += len(rows)
        stats["rows"] += len(rows)
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return stats


async def run_forever() -> None:
    """Its own HTTP session, so a slow CFTC response never holds a connection the macro feed needs."""
    import aiohttp
    from shared.db import get_timescale
    from shared.utils.retry_sleep import next_delay

    await asyncio.sleep(150)
    timeout = aiohttp.ClientTimeout(total=120)
    failures = 0
    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "Sentinel-Intelligence-Platform/1.0"}) as session:
                db = await get_timescale()
                before = await db.query("SELECT COUNT(*) AS n FROM cftc_cot")
                stats = await sync(session, db)
                after = await db.query("SELECT COUNT(*) AS n FROM cftc_cot")
                added = int(after[0]["n"]) - int(before[0]["n"])
                if added:
                    logger.info("CFTC COT: %s new rows (%s read)", added, stats["rows"])
            failures = 0
        except Exception as e:
            failures += 1
            swallowed("collector_macro.cftc_cot", e, logger)
        await asyncio.sleep(next_delay(failures, TOPUP_INTERVAL_SEC))


__all__ = ["fetch_range", "run_forever", "sync", "where_clause"]
