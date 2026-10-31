"""
services/agents/market_cap_job.py

Every six hours: each traded stock's best-known capitalisation into
market_cap.CAPS_KEY, which every forecast and B1 read to keep calls to
companies of at least $1bn. Shares outstanding (SEC within 18 months, else the
vendor's count) times the latest close, or the Finnhub figure, whichever is
larger (market_cap.best_cap). The hash is replaced whole, so a reader never
sees half a table; a failed pass leaves the last one until its TTL.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from shared.utils import market_cap as mc
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.market_cap")

INTERVAL_SEC = 6 * 3600
_COMPUTED_SQL = """
WITH px AS (SELECT DISTINCT ON (ticker) ticker, close FROM daily_bars WHERE day > now() - interval '10 days'
            ORDER BY ticker, day DESC),
sh AS (SELECT DISTINCT ON (ticker) ticker, value AS shares FROM fundamentals
       WHERE concept = 'EntityCommonStockSharesOutstanding' AND period_end > now() - interval '18 months'
       ORDER BY ticker, period_end DESC),
vs AS (SELECT DISTINCT ON (ticker) ticker, shares FROM vendor_shares WHERE as_of > now() - interval '18 months'
       ORDER BY ticker, as_of DESC)
SELECT px.ticker, coalesce(sh.shares, vs.shares) * px.close AS cap
FROM px LEFT JOIN sh USING (ticker) LEFT JOIN vs USING (ticker)
"""


class MarketCapJob:
    def __init__(self, db: Any, redis: Any = None):
        self.db = db
        self.raw = getattr(redis, "raw", redis)

    async def run(self) -> None:
        await asyncio.sleep(30)
        while True:
            try:
                logger.info("Market caps: %s", await self.run_once())
            except Exception as e:
                swallowed("agents.market_cap.pass", e, logger)
            await asyncio.sleep(INTERVAL_SEC)

    async def run_once(self) -> Dict[str, int]:
        rows = await self.db.query(_COMPUTED_SQL) or []
        tickers = [str(r["ticker"]).upper() for r in rows]
        computed = {str(r["ticker"]).upper(): float(r["cap"]) for r in rows if r["cap"]}
        cached: Dict[str, Optional[float]] = {}
        for i in range(0, len(tickers), 500):
            chunk = tickers[i:i + 500]
            for t, v in zip(chunk, await self.raw.mget([mc.market_cap_key(t) for t in chunk])):
                cached[t] = mc.parse_market_cap(v)
        best = mc.cap_rows(computed, cached, tickers)
        if not best:
            return {"tickers": len(tickers), "with_cap": 0}
        tmp = mc.CAPS_KEY + ":building"
        async with self.raw.pipeline() as pipe:
            pipe.delete(tmp)
            pipe.hset(tmp, mapping={t: repr(v) for t, v in best.items()})
            pipe.expire(tmp, mc.CAPS_TTL_SEC)
            pipe.rename(tmp, mc.CAPS_KEY)
            await pipe.execute()
        return {"tickers": len(tickers), "with_cap": len(best),
                "at_least_1bn": sum(v >= mc.DEFAULT_MIN_MARKET_CAP_USD for v in best.values())}


__all__ = ["MarketCapJob"]
