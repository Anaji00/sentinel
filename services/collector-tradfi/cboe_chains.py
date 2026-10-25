"""
services/collector-tradfi/cboe_chains.py

Each weekday evening, every listed contract for the option universe from
Cboe's free delayed quotes (about fifteen minutes late; taken well after the
close, so the day is complete), summarised per stock into option_chain_daily
(shared/utils/option_chain.py). One request a name at about one a second:
the 600 names of the IV history plus the live focus take ten to fifteen
minutes. Cboe asks for nothing but a polite pace; there is no key.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Any, List
from zoneinfo import ZoneInfo

import aiohttp

from shared.db import get_redis, get_timescale
from shared.utils import live_focus as lf
from shared.utils import option_chain as oc
from shared.utils.quiet_failures import swallowed

from option_history import DONE_KEY as UNIVERSE_KEY   # option_history's 600 names

logger = logging.getLogger("collector.tradfi.cboe_chains")

CHAIN_URL = os.getenv("CBOE_CHAIN_URL", "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json")
NY = ZoneInfo("America/New_York")
RUN_AFTER_NY_HOUR = 17           # an hour after the close: the delayed day is complete
REQUEST_GAP_SEC = 1.0
DONE_KEY = "sentinel:cboe_chains:day"
UPSERT_SQL = """
    INSERT INTO option_chain_daily (ticker, day, call_volume, put_volume, call_oi, put_oi, pcr_volume, pcr_oi,
        iv30, atm_iv, skew_25d, expiry_days, contracts, spot, source, fetched_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'cboe_delayed', NOW())
    ON CONFLICT (ticker, day) DO UPDATE SET call_volume = EXCLUDED.call_volume, put_volume = EXCLUDED.put_volume,
        call_oi = EXCLUDED.call_oi, put_oi = EXCLUDED.put_oi, pcr_volume = EXCLUDED.pcr_volume,
        pcr_oi = EXCLUDED.pcr_oi, iv30 = EXCLUDED.iv30, atm_iv = EXCLUDED.atm_iv, skew_25d = EXCLUDED.skew_25d,
        expiry_days = EXCLUDED.expiry_days, contracts = EXCLUDED.contracts, spot = EXCLUDED.spot,
        fetched_at = NOW()
"""


def row(ticker: str, s: dict) -> tuple:
    return (ticker, s["day"], s["call_volume"], s["put_volume"], s["call_oi"], s["put_oi"], s["pcr_volume"],
            s["pcr_oi"], s["iv30"], s["atm_iv"], s["skew_25d"], s["expiry_days"], s["contracts"], s["spot"])


async def _names(raw: Any) -> List[str]:
    universe = {(m.decode() if isinstance(m, bytes) else str(m)).upper()
                for m in (await raw.smembers(UNIVERSE_KEY) or [])}
    focus = [(m.decode() if isinstance(m, bytes) else str(m)).upper()
             for m in (await raw.zrevrange(lf.LIVE_FOCUS_KEY, 0, 199) or [])]
    return list(dict.fromkeys(focus + sorted(universe)))


async def run_once(session: aiohttp.ClientSession, raw: Any, day) -> int:
    db = await get_timescale()
    written = 0
    for t in await _names(raw):
        try:
            async with session.get(CHAIN_URL.format(symbol=t), timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    await asyncio.sleep(60)
                    continue
                if resp.status != 200:
                    continue
                body = await resp.json(content_type=None)
            s = oc.summarize(body, day)
            if s:
                await db.execute(UPSERT_SQL, *row(t, s))
                written += 1
        except Exception as e:
            swallowed("collector.tradfi.cboe_chains.fetch", e, logger, detail=t)
        await asyncio.sleep(REQUEST_GAP_SEC)
    return written


async def run_forever() -> None:
    raw = getattr(await get_redis(), "raw", None)
    if raw is None:
        logger.warning("Cboe chains off: no Redis")
        return
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (sentinel research)"}) as session:
        while True:
            try:
                now = datetime.now(NY)
                day = now.date()
                last = await raw.get(DONE_KEY)
                last_s = last.decode() if isinstance(last, bytes) else last
                if now.weekday() < 5 and now.hour >= RUN_AFTER_NY_HOUR and last_s != day.isoformat():
                    n = await run_once(session, raw, day)
                    await raw.set(DONE_KEY, day.isoformat(), ex=7 * 86400)
                    logger.info("Cboe chains: %s stocks summarised for %s.", n, day)
            except Exception as e:
                swallowed("collector.tradfi.cboe_chains", e, logger)
            await asyncio.sleep(600)


__all__ = ["row", "run_forever", "run_once"]
