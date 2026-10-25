"""
services/agents/feature_jobs.py

Writes the filing-based stock features into stock_features_daily
(shared/utils/stock_features.py) each weekday evening, before the factor
engine's nightly run: insider measures on a weekly grid from Form 4s, and
adverse 8-K items as events. History once (insiders from 2016, 8-K items from
when the archive begins), then the last few weeks each night. Commodity
exposures are written by the collector that fetches the prices.

Nothing here reaches the live model: these columns are read by trials until one
is adopted.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from shared.utils import stock_features as sf
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.feature_jobs")

RUN_HOUR_UTC = 21
HISTORY_START = date(2016, 1, 1)
TICKER_BATCH = 400
INCREMENTAL_WEEKS = 3
DONE_KEY = "sentinel:feature_jobs:day"
BACKFILLED_KEY = "sentinel:feature_jobs:backfilled"

_TICKERS_SQL = "SELECT DISTINCT ticker FROM insider_transactions WHERE filed >= $1::date ORDER BY ticker"
_TRADES_SQL = """
    SELECT ticker, filed, owner_cik, code, planned, insider_class FROM insider_transactions
    WHERE ticker = ANY($1::text[]) AND filed >= $2::date AND code IN ('P', 'S')
"""
_FILINGS_SQL = """
    SELECT upper(coalesce(financial_data->>'ticker', primary_entity_id)) AS ticker,
           (coalesce(occurred_at, created_at) AT TIME ZONE 'America/New_York')::date AS filed,
           financial_data->'items' AS items
    FROM signal_event_archive
    WHERE type = 'filing' AND financial_data ? 'items' AND created_at >= $1::date
"""
_UPSERT = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source
"""


def _items(raw: Any) -> List[str]:
    import json

    if isinstance(raw, (str, bytes)):
        try:
            raw = json.loads(raw)
        except ValueError:
            return []
    return [str(x) for x in raw] if isinstance(raw, list) else []


class FeatureJobs:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(120)
        while True:
            try:
                now = datetime.now(timezone.utc)
                last = await self.raw.get(DONE_KEY) if self.raw is not None else None
                last_s = last.decode() if isinstance(last, bytes) else last
                backfilled = bool(await self.raw.exists(BACKFILLED_KEY)) if self.raw is not None else False
                if not backfilled or (now.weekday() < 5 and now.hour >= RUN_HOUR_UTC
                                      and last_s != now.date().isoformat()):
                    out = await self.run_once(backfill=not backfilled)
                    if self.raw is not None:
                        await self.raw.set(DONE_KEY, now.date().isoformat())
                        await self.raw.set(BACKFILLED_KEY, "1")
                    logger.info("Stock features written: %s", out)
            except Exception as e:
                swallowed("agents.feature_jobs", e, logger)
            await asyncio.sleep(900)

    async def run_once(self, backfill: bool) -> Dict[str, int]:
        today = datetime.now(timezone.utc).date()
        grid_start = HISTORY_START if backfill else today - timedelta(weeks=INCREMENTAL_WEEKS)
        grid = sf.weekly_grid(grid_start, today)
        insider = await self._insider(grid, grid_start - timedelta(days=182))
        events = await self._eightk(date(2000, 1, 1) if backfill else today - timedelta(days=10))
        return {"insider_rows": insider, "event_rows": events, "weeks": len(grid)}

    async def _write(self, rows: List[Tuple[str, Any, str, float]], source: str) -> int:
        batch = [(t, d, f, v, source) for t, d, f, v in rows]
        for k in range(0, len(batch), 5000):
            await self.db.execute_many(_UPSERT, batch[k:k + 5000])
        return len(batch)

    async def _insider(self, grid: List[date], since: date) -> int:
        tickers = [r["ticker"] for r in await self.db.query(_TICKERS_SQL, since) or []]
        written = 0
        for k in range(0, len(tickers), TICKER_BATCH):
            trades: Dict[str, List[sf.Trade]] = defaultdict(list)
            for r in await self.db.query(_TRADES_SQL, tickers[k:k + TICKER_BATCH], since) or []:
                trades[str(r["ticker"]).upper()].append(sf.Trade(
                    r["filed"], str(r["owner_cik"]), str(r["code"]), bool(r["planned"]),
                    r["insider_class"] == "opportunistic"))
            written += await self._write(sf.insider_rows(trades, grid), "insider")
            await asyncio.sleep(0)
        return written

    async def _eightk(self, since: date) -> int:
        filings = [(str(r["ticker"]), r["filed"], _items(r["items"]))
                   for r in await self.db.query(_FILINGS_SQL, since) or [] if r["ticker"]]
        return await self._write(sf.eightk_rows(filings), "filings")


__all__ = ["FeatureJobs"]
