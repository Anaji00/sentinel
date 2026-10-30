"""
services/collector-tradfi/commodities.py

Each weekday evening: front-month futures from Yahoo's chart endpoint (full
history back to 2015 on the first run, the last month after), DRAM/NAND/GDDR
spot from DRAMeXchange's public page, and GPU rental rates from RunPod's public
API -- into commodity_prices. Then each linked stock's exposure to them
(shared/utils/commodity_links.py), the signed 21- and 63-session move of what
it makes or buys, into stock_features_daily for the model's trials.

Also the Caldara-Iacoviello geopolitical risk index, daily and by country
(shared/utils/gpr.py), into macro_indices, with its latest reading in Redis.

Free and keyless, one request a second. The memory and GPU series have no free
history; theirs starts the first evening this runs.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

import aiohttp

from shared.db import get_redis, get_timescale
from shared.utils import commodity_links as cl
from shared.utils import gpr
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.tradfi.commodities")

NY = ZoneInfo("America/New_York")
RUN_AFTER_NY_HOUR = 18           # futures have settled
REQUEST_GAP_SEC = 1.0
HISTORY_START = date(2015, 1, 1)
DONE_KEY = "sentinel:commodities:day"
YAHOO_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
DRAM_URL = os.getenv("DRAMEXCHANGE_URL", "https://www.dramexchange.com/")
RUNPOD_URL = os.getenv("RUNPOD_GRAPHQL_URL", "https://api.runpod.io/graphql")
WINDOWS = (21, 63)
FEATURE_SOURCE = "commodity_links"

_UPSERT_PRICE = """
    INSERT INTO commodity_prices (series, day, value, unit, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (series, day) DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit, source = EXCLUDED.source
"""
_COUNT_SQL = "SELECT count(*) AS n FROM commodity_prices WHERE series = $1"
_ALL_SQL = "SELECT series, day, value FROM commodity_prices ORDER BY series, day"
_UPSERT_FEATURE = """
    INSERT INTO stock_features_daily (ticker, day, feature, value, source) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, day, feature) DO UPDATE SET value = EXCLUDED.value, source = EXCLUDED.source
"""
_UPSERT_INDEX = """
    INSERT INTO macro_indices (series, day, value, source) VALUES ($1, $2, $3, $4)
    ON CONFLICT (series, day) DO UPDATE SET value = EXCLUDED.value
"""
_GPR_SQL = "SELECT day, value FROM macro_indices WHERE series = 'gpr_daily' ORDER BY day"
_SESSIONS_SQL = "SELECT day FROM daily_bars WHERE ticker = 'SPY' AND day >= $1::date ORDER BY day"
_LAST_UPDATE = re.compile(r"Last Update:\s*([A-Z][a-z]{2})\.?\s*(\d{1,2})\s+(\d{4})")


def yahoo_rows(symbol: str, body: Dict[str, Any]) -> List[Tuple]:
    """(series, day, close, unit, source) from a Yahoo chart response."""
    try:
        r = body["chart"]["result"][0]
        closes = r["indicators"]["quote"][0]["close"]
        unit = r["meta"].get("currency")
        out = []
        for ts, c in zip(r["timestamp"], closes):
            if c is not None and c > 0:
                d = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(NY).date()
                out.append((symbol, d, float(c), unit, "yahoo"))
        return out
    except (KeyError, IndexError, TypeError):
        return []


def dram_day(page: str, fallback: date) -> date:
    """The spot table's own date (its first "Last Update"), else the fallback."""
    m = _LAST_UPDATE.search(page or "")
    if not m:
        return fallback
    try:
        return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y").date()
    except ValueError:
        return fallback


async def _futures(session: aiohttp.ClientSession, db: Any, today: date) -> int:
    written = 0
    for sym in cl.FUTURES:
        try:
            have = (await db.query_one(_COUNT_SQL, sym) or {}).get("n") or 0
            start = HISTORY_START if have < 100 else today - timedelta(days=35)
            p1 = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
            p2 = int(datetime.now(timezone.utc).timestamp())
            async with session.get(YAHOO_URL.format(symbol=sym), params={"period1": str(p1), "period2": str(p2),
                                                                         "interval": "1d"},
                                   timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    continue
                rows = yahoo_rows(sym, await resp.json(content_type=None))
            if rows:
                await db.execute_many(_UPSERT_PRICE, rows)
                written += len(rows)
        except Exception as e:
            swallowed("collector.tradfi.commodities.futures", e, logger, detail=sym)
        await asyncio.sleep(REQUEST_GAP_SEC)
    return written


async def _memory(session: aiohttp.ClientSession, db: Any, today: date) -> int:
    try:
        async with session.get(DRAM_URL, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                return 0
            page = await resp.text(errors="ignore")
        day = dram_day(page, today)
        rows = [(k, day, v, "USD", "dramexchange") for k, v in cl.parse_dramexchange(page).items()]
        if rows:
            await db.execute_many(_UPSERT_PRICE, rows)
        return len(rows)
    except Exception as e:
        swallowed("collector.tradfi.commodities.memory", e, logger)
        return 0


async def _gpus(session: aiohttp.ClientSession, db: Any, today: date) -> int:
    try:
        async with session.post(RUNPOD_URL, json={"query": cl.GPU_QUERY},
                                timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                return 0
            body = await resp.json(content_type=None)
        rows = [(k, today, v, "USD per GPU-hour", "runpod") for k, v in cl.parse_runpod(body).items()]
        if rows:
            await db.execute_many(_UPSERT_PRICE, rows)
        return len(rows)
    except Exception as e:
        swallowed("collector.tradfi.commodities.gpus", e, logger)
        return 0


async def _gpr(session: aiohttp.ClientSession, db: Any, raw: Any) -> int:
    """The geopolitical risk index, daily and monthly (whole files: they are
    small and revised), and its latest reading for the agents."""
    import io

    import pandas as pd

    written = 0
    for url, parse in ((gpr.DAILY_URL, gpr.daily_rows), (gpr.MONTHLY_URL, gpr.monthly_rows)):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status != 200:
                    continue
                body = await resp.read()
            frame = await asyncio.to_thread(pd.read_stata, io.BytesIO(body))
            rows = parse(frame.to_dict("records"))
            for k in range(0, len(rows), 5000):
                await db.execute_many(_UPSERT_INDEX, rows[k:k + 5000])
            written += len(rows)
        except Exception as e:
            swallowed("collector.tradfi.commodities.gpr", e, logger, detail=url)
        await asyncio.sleep(REQUEST_GAP_SEC)
    hist = await db.query(_GPR_SQL) or []
    s = gpr.summary([r["day"] for r in hist], [float(r["value"]) for r in hist])
    if s and raw is not None:
        await raw.set(gpr.LATEST_KEY, json.dumps(s))
    return written


async def write_features(db: Any, since: date) -> int:
    """Each linked stock's exposure on every session from `since`."""
    closes: Dict[str, List[Tuple[date, float]]] = {}
    for r in await db.query(_ALL_SQL) or []:
        closes.setdefault(str(r["series"]), []).append((r["day"], float(r["value"])))
    groups: Dict[str, List[str]] = {}
    for s in closes:
        g = cl.group_of(s)
        if g:
            groups.setdefault(g, []).append(s)
    sessions = [r["day"] for r in await db.query(_SESSIONS_SQL, since) or []]
    rows: List[Tuple] = []
    for t in cl.linked_tickers():
        for d in sessions:
            for w in WINDOWS:
                v = cl.exposure(t, d, groups, closes, w)
                if v is not None:
                    rows.append((t, d, f"cmdty_exposure_{w}", round(v, 6), FEATURE_SOURCE))
    for k in range(0, len(rows), 5000):
        await db.execute_many(_UPSERT_FEATURE, rows[k:k + 5000])
    return len(rows)


async def run_once(session: aiohttp.ClientSession, today: date, backfill: bool, raw: Any = None) -> Dict[str, int]:
    db = await get_timescale()
    out = {"futures": await _futures(session, db, today), "memory": await _memory(session, db, today),
           "gpu": await _gpus(session, db, today), "gpr": await _gpr(session, db, raw)}
    out["features"] = await write_features(db, HISTORY_START if backfill else today - timedelta(days=14))
    return out


async def run_forever() -> None:
    raw = getattr(await get_redis(), "raw", None)
    if raw is None:
        logger.warning("Commodities off: no Redis")
        return
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (sentinel research)"}) as session:
        while True:
            try:
                now = datetime.now(NY)
                today = now.date()
                last = await raw.get(DONE_KEY)
                last_s = last.decode() if isinstance(last, bytes) else last
                if now.weekday() < 5 and now.hour >= RUN_AFTER_NY_HOUR and last_s != today.isoformat():
                    out = await run_once(session, today, backfill=last_s is None, raw=raw)
                    await raw.set(DONE_KEY, today.isoformat())
                    logger.info("Commodities for %s: %s", today, json.dumps(out))
            except Exception as e:
                swallowed("collector.tradfi.commodities", e, logger)
            await asyncio.sleep(600)


__all__ = ["dram_day", "run_forever", "run_once", "write_features", "yahoo_rows"]
