"""
services/collector-tradfi/short_data.py

Collects the free short-side data parsed in shared/utils/short_data.py:

  - FINRA daily short sale volume: every missing session from 2021 on start
    (about 1,450 small files, paced), then each evening once the day's file posts.
  - Interactive Brokers' borrow file: a snapshot every two hours, the day's
    latest kept, from the first run on (IB publishes no history).
"""
from __future__ import annotations

import asyncio
import ftplib
import io
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Set
from zoneinfo import ZoneInfo

import aiohttp

from shared.utils import short_data as sd
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.short_data")

NY = ZoneInfo("America/New_York")
# FINRA is not the SEC; the generic platform agent, never the SEC contact.
USER_AGENT = "Sentinel-Intelligence-Platform/1.0"
FINRA_PACE_SEC = 0.5
FINRA_POLL_SEC = 60 * 60
FINRA_POSTED_HOUR_ET = 18            # posted "no later than 6:00pm ET"
BORROW_INTERVAL_SEC = 2 * 60 * 60
FTP_TIMEOUT_SEC = 60

_FINRA_DAYS_SQL = "SELECT DISTINCT day FROM finra_short_volume WHERE day >= $1::date"
_FINRA_INSERT_SQL = """
    INSERT INTO finra_short_volume (day, ticker, short_volume, exempt_volume, total_volume)
    VALUES ($1, $2, $3, $4, $5) ON CONFLICT (day, ticker) DO NOTHING
"""
_BORROW_UPSERT_SQL = """
    INSERT INTO borrow_rates (day, ticker, fee_rate, rebate_rate, available, taken_at)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (day, ticker) DO UPDATE SET fee_rate = EXCLUDED.fee_rate, rebate_rate = EXCLUDED.rebate_rate,
        available = EXCLUDED.available, taken_at = EXCLUDED.taken_at
    WHERE borrow_rates.taken_at < EXCLUDED.taken_at
"""


def _weekdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _last_posted_day(now: Optional[datetime] = None) -> date:
    """The latest session whose FINRA file should exist by now."""
    now = (now or datetime.now(timezone.utc)).astimezone(NY)
    return now.date() if now.hour >= FINRA_POSTED_HOUR_ET else now.date() - timedelta(days=1)


async def _finra_day(session: aiohttp.ClientSession, db, day: date) -> Optional[int]:
    """Rows stored for one session; 0 for a market holiday (no file); None on error."""
    try:
        async with session.get(sd.FINRA_URL.format(day=day)) as resp:
            if resp.status in (403, 404):
                return 0
            if resp.status != 200:
                logger.warning("FINRA %s: HTTP %s", day, resp.status)
                return None
            text = await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        swallowed("collector.short_data.finra_fetch", e, logger)
        return None
    rows = sd.parse_finra(text)
    if rows:
        await db.execute_many(_FINRA_INSERT_SQL, rows)
    return len(rows)


async def finra_loop(db) -> None:
    """Fill every missing session since FINRA_START, then keep up daily."""
    holidays: Set[date] = set()
    connector = aiohttp.TCPConnector(ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout,
                                     headers={"User-Agent": USER_AGENT}) as session:
        while True:
            try:
                have = {r["day"] for r in await db.query(_FINRA_DAYS_SQL, sd.FINRA_START) or []}
                todo = [d for d in _weekdays(sd.FINRA_START, _last_posted_day()) if d not in have and d not in holidays]
                stored = 0
                for d in todo:
                    n = await _finra_day(session, db, d)
                    if n == 0:
                        holidays.add(d)
                    stored += n or 0
                    if len(todo) > 1:
                        await asyncio.sleep(FINRA_PACE_SEC)
                if todo:
                    logger.info("FINRA short volume: %s sessions checked, %s rows stored", len(todo), stored)
            except Exception as e:
                logger.error("FINRA short volume pass failed: %s", e, exc_info=True)
            await asyncio.sleep(FINRA_POLL_SEC)


def _fetch_borrow_file() -> str:
    """Blocking FTP fetch; run in a thread."""
    buf = io.BytesIO()
    with ftplib.FTP(sd.IBKR_HOST, timeout=FTP_TIMEOUT_SEC) as ftp:
        ftp.login(sd.IBKR_USER, "")
        ftp.retrbinary(f"RETR {sd.IBKR_FILE}", buf.write)
    return buf.getvalue().decode("latin-1")


async def borrow_loop(db) -> None:
    while True:
        try:
            text = await asyncio.to_thread(_fetch_borrow_file)
            taken, rows = sd.parse_ibkr(text)
            taken = taken or datetime.now(timezone.utc)
            day = taken.astimezone(NY).date()
            await db.execute_many(_BORROW_UPSERT_SQL, [(day, t, fee, reb, avail, taken) for t, fee, reb, avail in rows])
            logger.info("Borrow rates: %s US names as of %s", len(rows), taken.isoformat())
        except (OSError, EOFError, ftplib.Error) as e:
            swallowed("collector.short_data.borrow_fetch", e, logger)
        except Exception as e:
            logger.error("Borrow rates pass failed: %s", e, exc_info=True)
        await asyncio.sleep(BORROW_INTERVAL_SEC)


async def run_forever() -> None:
    from shared.db import get_timescale

    db = await get_timescale()
    await asyncio.gather(finra_loop(db), borrow_loop(db))


__all__ = ["borrow_loop", "finra_loop", "run_forever"]
