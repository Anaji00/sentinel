"""
services/agents/macro_bars_job.py

Every six hours: daily closes for macro instruments into macro_daily_bars
(shared/utils/macro_bars.py). A symbol with less than BACKFILL_BELOW stored
sessions is backfilled first (Yahoo's ten-year range, Treasury.gov from
FIRST_YEAR); after that each pass refreshes the last month. Free sources only.
One symbol's failure is counted and does not stop the others.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any, Dict, List

import aiohttp

from shared.utils import macro_bars as mb
from shared.utils.quiet_failures import swallowed
from shared.utils.treasury import TREASURY_XML

logger = logging.getLogger("agent.macro_bars")

INTERVAL_SEC = 6 * 3600
BACKFILL_BELOW = 500
REQUEST_GAP_SEC = 1.0                     # between Yahoo requests: a free endpoint, asked politely
_HEADERS = {"User-Agent": "Mozilla/5.0"}
_TIMEOUT = aiohttp.ClientTimeout(total=30)

_COUNTS_SQL = "SELECT symbol, count(*) AS n FROM macro_daily_bars GROUP BY symbol"
_UPSERT_SQL = """
    INSERT INTO macro_daily_bars (symbol, day, close, source)
    SELECT * FROM unnest($1::text[], $2::date[], $3::float8[], $4::text[])
    ON CONFLICT (symbol, day) DO UPDATE SET close = EXCLUDED.close, source = EXCLUDED.source
"""


class MacroBarsJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db

    async def run(self) -> None:
        await asyncio.sleep(60)
        while True:
            try:
                logger.info("Macro bars: %s", await self.run_once())
            except Exception as e:
                swallowed("agents.macro_bars.pass", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, int]:
        counts = {str(r["symbol"]): int(r["n"]) for r in await self.db.query(_COUNTS_SQL) or []}
        out = {"symbols": 0, "rows": 0, "failed": 0}
        async with aiohttp.ClientSession(headers=_HEADERS, timeout=_TIMEOUT) as session:
            for sym, ysym in mb.YAHOO_SYMBOLS.items():
                rng = "10y" if counts.get(sym, 0) < BACKFILL_BELOW else "1mo"
                try:
                    async with session.get(mb.YAHOO_DAILY.format(symbol=ysym, range=rng)) as resp:
                        resp.raise_for_status()
                        rows = mb.yahoo_rows(sym, await resp.json(content_type=None))
                    out["rows"] += await self._store(rows)
                    out["symbols"] += bool(rows)
                except Exception as e:
                    out["failed"] += 1
                    swallowed("agents.macro_bars.yahoo", e, logger, detail=sym)
                await asyncio.sleep(REQUEST_GAP_SEC)
            backfill = counts.get("US10Y", 0) < BACKFILL_BELOW
            for year in mb.years(date.today(), backfill):
                try:
                    async with session.get(TREASURY_XML.format(year=year)) as resp:
                        resp.raise_for_status()
                        rows = mb.treasury_rows(await resp.text())
                    out["rows"] += await self._store(rows)
                except Exception as e:
                    out["failed"] += 1
                    swallowed("agents.macro_bars.treasury", e, logger, detail=str(year))
            out["symbols"] += len(mb.TREASURY_ALIASES)
        return out

    async def _store(self, rows: List[mb.Row]) -> int:
        if not rows:
            return 0
        syms, days, closes, srcs = (list(x) for x in zip(*rows))
        await self.db.execute(_UPSERT_SQL, syms, days, closes, srcs)
        return len(rows)


__all__ = ["MacroBarsJob"]
