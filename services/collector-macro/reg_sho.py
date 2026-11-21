"""
services/collector-macro/reg_sho.py

Regulation SHO threshold securities list (Nasdaq's daily file, free, no key) into `reg_sho_threshold`, with the file's own creation time as the
publication time. First run reads back REG_SHO_BACKFILL_DAYS (default 730) of weekdays; afterwards the last week's missing days. Weekdays with no
file (holidays) answer with a redirect and are skipped. NYSE's list has no open endpoint this reads, so only the Nasdaq list is stored. Measurement
only: nothing here feeds an alert, a Kafka topic or the recommendation engine; the event study is scripts/study_reg_sho.py.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set

from shared.utils.quiet_failures import swallowed
from shared.utils.reg_sho_parse import parse_file, url_for

logger = logging.getLogger("collector.macro.reg_sho")

BACKFILL_DAYS = int(os.getenv("REG_SHO_BACKFILL_DAYS", "730"))
RECENT_DAYS = 7
REQUEST_PAUSE_SEC = 0.5
TOPUP_INTERVAL_SEC = 3 * 3600

INSERT_SQL = """
    INSERT INTO reg_sho_threshold (list_date, symbol, security_name, market_category, exchange, published_at)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (list_date, symbol, exchange) DO NOTHING
"""


def weekdays(start: date, end: date) -> List[date]:
    out = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


async def fetch_day(session: Any, d: date) -> Optional[str]:
    """The file's text, or None when there is no file for that day (holiday or not yet published)."""
    async with session.get(url_for(d), allow_redirects=False) as resp:
        if resp.status == 200:
            return await resp.text()
        if resp.status in (301, 302, 303, 307, 308, 404):
            return None
        raise RuntimeError(f"Nasdaq Reg SHO HTTP {resp.status} for {d}")


async def sync(session: Any, db: Any, backfill: bool, today: Optional[date] = None) -> Dict[str, int]:
    today = today or date.today()
    have: Set[date] = {r["list_date"] for r in await db.query("SELECT DISTINCT list_date FROM reg_sho_threshold") or []}
    horizon = today - timedelta(days=BACKFILL_DAYS if backfill else RECENT_DAYS)
    stats = {"days": 0, "rows": 0, "errors": 0, "checked": 0}
    for d in weekdays(horizon, today):
        if d in have:
            continue
        stats["checked"] += 1
        try:
            text = await fetch_day(session, d)
            rows = parse_file(text, d) if text else []
            if rows:
                await db.execute_many(INSERT_SQL, rows)
                stats["days"] += 1
                stats["rows"] += len(rows)
        except Exception as e:
            stats["errors"] += 1
            swallowed("collector_macro.reg_sho.day", e, logger, detail=str(d))
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    if stats["checked"] and stats["errors"] == stats["checked"]:
        raise RuntimeError(f"Reg SHO: all {stats['checked']} day fetches failed")
    return stats


async def run_forever() -> None:
    import aiohttp
    from shared.db import get_timescale
    from shared.utils.retry_sleep import next_delay

    await asyncio.sleep(270)
    timeout = aiohttp.ClientTimeout(total=60)
    failures = 0
    first = True
    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "Sentinel-Intelligence-Platform/1.0"}) as session:
                stats = await sync(session, await get_timescale(), backfill=first)
                first = False
                if stats["rows"]:
                    logger.info("Reg SHO threshold: %s new days, %s rows", stats["days"], stats["rows"])
            failures = 0
        except Exception as e:
            failures += 1
            swallowed("collector_macro.reg_sho", e, logger)
        await asyncio.sleep(next_delay(failures, TOPUP_INTERVAL_SEC))


__all__ = ["fetch_day", "run_forever", "sync", "weekdays"]
