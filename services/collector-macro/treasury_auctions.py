"""
services/collector-macro/treasury_auctions.py

US Treasury auction results (bid-to-cover, high yield, bidder shares) into `treasury_auctions`, every auction since 2016, with a release time
recorded. TreasuryDirect web service, no key. Measurement only: nothing here feeds an alert, a Kafka topic or the recommendation engine; the event
study is scripts/study_treasury_auctions.py. The tail against the when-issued yield is not in this feed and is not stored.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from shared.utils.auction_parse import COLUMNS, SEARCH_URL, parse_records
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.macro.treasury_auctions")

FIRST_DAY = date(2016, 1, 1)
REQUEST_PAUSE_SEC = 1.0
TOPUP_INTERVAL_SEC = 3 * 3600
# The search filters on issue date, which trails the auction by days; the window reaches ahead of today so an auction that closed this morning is in it.
LOOKBACK_DAYS = 45
LOOKAHEAD_DAYS = 14

INSERT_SQL = f"""
    INSERT INTO treasury_auctions ({", ".join(COLUMNS)})
    VALUES ({", ".join(f"${i + 1}" for i in range(len(COLUMNS)))})
    ON CONFLICT (cusip, auction_date) DO NOTHING
"""


def month_windows(start: date, end: date) -> List[tuple]:
    """Consecutive (first day, last day) month windows covering start..end; one request each stays under the service's row cap."""
    out = []
    d = date(start.year, start.month, 1)
    while d <= end:
        nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
        out.append((d, nxt - timedelta(days=1)))
        d = nxt
    return out


async def fetch_window(session: Any, start: date, end: date) -> List[Dict[str, Any]]:
    params = {"format": "json", "startDate": start.isoformat(), "endDate": end.isoformat()}
    async with session.get(SEARCH_URL, params=params) as resp:
        if resp.status != 200:
            raise RuntimeError(f"TreasuryDirect HTTP {resp.status}")
        data = await resp.json(content_type=None)
    return data if isinstance(data, list) else []


async def sync(session: Any, db: Any, today: Optional[date] = None) -> int:
    """First run: every month since 2016. After that: the last 45 days and the next two weeks. Returns rows read."""
    today = today or date.today()
    last = await db.query("SELECT MAX(auction_date) AS d FROM treasury_auctions")
    have = last[0]["d"] if last and last[0]["d"] else None
    start = FIRST_DAY if have is None else today - timedelta(days=LOOKBACK_DAYS)
    windows = month_windows(start, today + timedelta(days=LOOKAHEAD_DAYS)) if have is None else [(start, today + timedelta(days=LOOKAHEAD_DAYS))]
    read = 0
    for a, b in windows:
        rows = parse_records(await fetch_window(session, a, b))
        if rows:
            await db.execute_many(INSERT_SQL, rows)
        read += len(rows)
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return read


async def run_forever() -> None:
    import aiohttp
    from shared.db import get_timescale
    from shared.utils.retry_sleep import next_delay

    await asyncio.sleep(210)
    timeout = aiohttp.ClientTimeout(total=120)
    failures = 0
    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "Sentinel-Intelligence-Platform/1.0"}) as session:
                db = await get_timescale()
                before = await db.query("SELECT COUNT(*) AS n FROM treasury_auctions")
                read = await sync(session, db)
                after = await db.query("SELECT COUNT(*) AS n FROM treasury_auctions")
                added = int(after[0]["n"]) - int(before[0]["n"])
                if added:
                    logger.info("Treasury auctions: %s new rows (%s read)", added, read)
            failures = 0
        except Exception as e:
            failures += 1
            swallowed("collector_macro.treasury_auctions", e, logger)
        await asyncio.sleep(next_delay(failures, TOPUP_INTERVAL_SEC))


__all__ = ["fetch_window", "month_windows", "run_forever", "sync"]
