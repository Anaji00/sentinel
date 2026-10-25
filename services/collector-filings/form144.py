"""
services/collector-filings/form144.py

Insiders' notices of proposed sales (Form 144), from EDGAR's current feed,
every ten minutes (shared/utils/form144.py parses). Kept in form144_notices.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any, Callable, Optional

import aiohttp

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.db import get_timescale  # noqa: E402
from shared.utils import form144 as f144  # noqa: E402
from shared.utils.quiet_failures import swallowed  # noqa: E402

logger = logging.getLogger("collector_filings.form144")

SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
POLL_SEC = 600
DOC_PAUSE_SEC = 0.15            # well inside the SEC's ten requests a second

_KNOWN_SQL = "SELECT accession FROM form144_notices WHERE accession = ANY($1::text[])"
_INSERT_SQL = """
    INSERT INTO form144_notices (accession, issuer_cik, ticker, issuer_name, seller, relationship, security, shares,
        market_value, pct_outstanding, approx_sale_date, acquired_how, notice_date)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (accession) DO NOTHING
"""


async def poll_once(session: aiohttp.ClientSession, db: Any, cik_ticker: Callable[[str], Optional[str]]) -> int:
    async with session.get(f144.FEED_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        if resp.status != 200:
            logger.warning("Form 144 feed: HTTP %s", resp.status)
            return 0
        filings = f144.parse_feed((await resp.read()).decode("utf-8", "replace"))
    if not filings:
        return 0
    known = {r["accession"] for r in await db.query(_KNOWN_SQL, [f["accession"] for f in filings]) or []}
    stored = 0
    for f in filings:
        if f["accession"] in known:
            continue
        await asyncio.sleep(DOC_PAUSE_SEC)
        try:
            async with session.get(f["doc_url"], timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    continue
                n = f144.parse_notice((await resp.read()).decode("utf-8", "replace"))
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            swallowed("collector_filings.form144.doc", e, logger, detail=f["accession"])
            continue
        if not n:
            continue
        ticker = None
        if n["issuer_cik"]:
            try:
                ticker = cik_ticker(n["issuer_cik"])
            except Exception as e:
                swallowed("collector_filings.form144.ticker", e, logger, detail=n["issuer_cik"])
        await db.execute(_INSERT_SQL, f["accession"], n["issuer_cik"], ticker, n["issuer_name"], n["seller"],
                         n["relationship"], n["security"], n["shares"], n["market_value"], n["pct_outstanding"],
                         n["approx_sale_date"], n["acquired_how"], n["notice_date"])
        stored += 1
    return stored


async def run_forever(cik_ticker: Callable[[str], Optional[str]]) -> None:
    while True:
        try:
            db = await get_timescale()
            async with aiohttp.ClientSession(headers=SEC_HEADERS) as session:
                n = await poll_once(session, db, cik_ticker)
            if n:
                logger.info("Form 144: %s new notices of proposed insider sales.", n)
        except Exception as e:
            swallowed("collector_filings.form144.poll", e, logger)
        await asyncio.sleep(POLL_SEC)
