"""
services/collector-tradfi/stock_news.py

Company news from Alpaca's feed into stock_news (shared/utils/stock_news.py):
the live tail every two minutes, and history walked back a day at a time to
BACKFILL_START, resuming where it stopped. Scoring happens in the
correlation service, which holds the model.

Alpaca's data API allows 200 requests a minute per account and this
collector's bars, quotes and options share it, so news is paced at about a
hundred.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from shared.db import get_redis, get_timescale
from shared.utils import stock_news as sn
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.tradfi.stock_news")

LIVE_EVERY_SEC = 120
REQUEST_SPACING_SEC = 0.6
BACKFILL_CURSOR_KEY = "sentinel:stock_news:backfill_day"   # the earliest day loaded so far
_INSERT_SQL = f"""
    INSERT INTO {sn.NEWS_TABLE} (id, created_at, symbols, headline, summary, source, url)
    VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO NOTHING
"""
_LATEST_SQL = f"SELECT max(created_at) AS at FROM {sn.NEWS_TABLE}"


def _headers() -> Dict[str, str]:
    return {"APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID") or "",
            "APCA-API-SECRET-KEY": (os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET")
                                    or os.getenv("APCA_API_SECRET_KEY") or "")}


async def fetch_window(session: aiohttp.ClientSession, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Every article created in [start, end), oldest first."""
    rows: List[Dict[str, Any]] = []
    token: Optional[str] = None
    while True:
        params = {"start": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "limit": "50", "sort": "asc", "include_content": "false"}
        if token:
            params["page_token"] = token
        async with session.get(sn.NEWS_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 429:
                await asyncio.sleep(30)
                continue
            resp.raise_for_status()
            body = await resp.json()
        rows.extend(r for r in (sn.parse_item(x) for x in body.get("news") or []) if r)
        token = body.get("next_page_token")
        await asyncio.sleep(REQUEST_SPACING_SEC)
        if not token:
            return rows


async def _store(db: Any, rows: List[Dict[str, Any]]) -> int:
    if rows:
        await db.execute_many(_INSERT_SQL, [(r["id"], r["created_at"], r["symbols"], r["headline"], r["summary"],
                                             r["source"], r["url"]) for r in rows])
    return len(rows)


async def _live(session: aiohttp.ClientSession) -> None:
    while True:
        try:
            db = await get_timescale()
            row = await db.query_one(_LATEST_SQL)
            latest = row.get("at") if row else None
            if isinstance(latest, str):
                latest = datetime.fromisoformat(latest.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            start = max(latest - timedelta(minutes=10), now - timedelta(days=2)) if latest else now - timedelta(days=1)
            n = await _store(db, await fetch_window(session, start, now))
            if n:
                logger.debug("Stock news: %s articles", n)
        except Exception as e:
            swallowed("collector.tradfi.stock_news.live", e, logger)
        await asyncio.sleep(LIVE_EVERY_SEC)


async def _backfill(session: aiohttp.ClientSession) -> None:
    raw = getattr(await get_redis(), "raw", None)
    while True:
        try:
            db = await get_timescale()
            cur = await raw.get(BACKFILL_CURSOR_KEY) if raw is not None else None
            cur_day = date.fromisoformat(cur.decode() if isinstance(cur, bytes) else cur) if cur else \
                datetime.now(timezone.utc).date()
            if cur_day <= sn.BACKFILL_START:
                logger.info("Stock news history complete back to %s", sn.BACKFILL_START)
                return
            day = cur_day - timedelta(days=1)
            start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
            n = await _store(db, await fetch_window(session, start, start + timedelta(days=1)))
            if raw is not None:
                await raw.set(BACKFILL_CURSOR_KEY, day.isoformat())
            if day.day == 1:
                logger.info("Stock news history: back to %s (%s articles that day)", day, n)
        except Exception as e:
            swallowed("collector.tradfi.stock_news.backfill", e, logger)
            await asyncio.sleep(60)


async def run_forever() -> None:
    if not _headers()["APCA-API-KEY-ID"]:
        logger.warning("Stock news off: no Alpaca credentials")
        return
    async with aiohttp.ClientSession(headers=_headers()) as session:
        await asyncio.gather(_live(session), _backfill(session))


__all__ = ["BACKFILL_CURSOR_KEY", "fetch_window", "run_forever"]
