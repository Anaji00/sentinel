"""
services/agents/comention_job.py

Writes comention_mom_21 (shared/utils/comention.py) into stock_features_daily:
once a day for the latest session, and once, for every month end since
BACKFILL_FROM, so the researcher's monthly screen has history to test it on
from its first run. No model is involved, so the backfill carries no look-ahead:
each value uses only headlines before its day and closes up to it.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Sequence

from shared.utils import comention as cm
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.comention")

INTERVAL_SEC = 6 * 3600
BACKFILL_FROM = date(2014, 1, 1)
LOOKBACK_SESSIONS = 21
SOURCE = "comention"
DAY_KEY = "sentinel:comention:feature_day"
BACKFILL_KEY = "sentinel:comention:backfilled"

_HEADLINES_SQL = """
    SELECT created_at::date AS d, symbols FROM stock_news
    WHERE cardinality(symbols) BETWEEN 2 AND $2 AND created_at >= $1
"""
_SESSIONS_SQL = "SELECT day FROM daily_bars WHERE ticker = 'SPY' AND day >= $1 ORDER BY day"
_CLOSES_SQL = "SELECT ticker, day, close FROM daily_bars WHERE day = ANY($1::date[]) AND ticker = ANY($2::text[])"
_UPSERT_FEATURE = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source
"""


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


def month_ends(sessions: Sequence[date]) -> List[date]:
    """The last session of each month."""
    return [d for d, n in zip(sessions, list(sessions[1:]) + [None]) if n is None or n.month != d.month]


class CoMentionJob:
    def __init__(self, db: Any, redis: Any):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(900)
        while True:
            try:
                if self.raw is not None and not await self.raw.get(BACKFILL_KEY):
                    n = await self.write(backfill=True)
                    await self.raw.set(BACKFILL_KEY, "1")
                    logger.info("Co-mention momentum backfilled: %s rows", n)
                n = await self.write(backfill=False)
                if n:
                    logger.info("Co-mention momentum: %s rows", n)
            except Exception as e:
                swallowed("agents.comention", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def write(self, backfill: bool) -> int:
        start = BACKFILL_FROM if backfill else date.today() - timedelta(days=60)
        sessions = [r["day"] for r in await self.db.query(_SESSIONS_SQL, start - timedelta(days=45)) or []]
        if len(sessions) <= LOOKBACK_SESSIONS:
            return 0
        days = [d for d in month_ends(sessions) if d >= start] if backfill else [sessions[-1]]
        if not backfill and self.raw is not None and _s(await self.raw.get(DAY_KEY)) == days[-1].isoformat():
            return 0
        rows = await self.db.query(_HEADLINES_SQL, start - timedelta(days=cm.WINDOW_DAYS), cm.MAX_NAMES) or []
        # Pairing and counting are CPU-bound over a year of headlines per day,
        # so they run in a worker thread, off the event loop.
        events = await asyncio.to_thread(cm.pair_events, [(r["d"], r["symbols"]) for r in rows])
        idx = {d: k for k, d in enumerate(sessions)}
        written = 0
        for day in days:
            k = idx.get(day)
            if k is None or k < LOOKBACK_SESSIONS:
                continue
            links = await asyncio.to_thread(cm.links_asof, events, day)
            if not links:
                continue
            tickers = sorted(set(links) | {p for v in links.values() for p in v})
            then = sessions[k - LOOKBACK_SESSIONS]
            px: Dict[str, Dict[date, float]] = {}
            for r in await self.db.query(_CLOSES_SQL, [then, day], tickers) or []:
                px.setdefault(str(r["ticker"]).upper(), {})[r["day"]] = float(r["close"])
            rets = {t: p[day] / p[then] - 1 for t, p in px.items() if p.get(then) and p.get(day)}
            rows_out = [(t, day, cm.FEATURE, v, SOURCE) for t, v in cm.feature(links, rets).items()]
            if rows_out:
                await self.db.execute_many(_UPSERT_FEATURE, rows_out)
                written += len(rows_out)
        if not backfill and self.raw is not None:
            await self.raw.set(DAY_KEY, days[-1].isoformat())
        return written


__all__ = ["CoMentionJob", "month_ends"]
