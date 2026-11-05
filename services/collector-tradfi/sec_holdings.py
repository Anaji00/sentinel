"""
services/collector-tradfi/sec_holdings.py

Loads the free SEC sets parsed in shared/utils/sec_holdings.py: fails-to-deliver from
FTD_START_YEAR, then every quarter of N-PORT. Each file is recorded in sec_bulk_files,
so a restart resumes, and one that keeps failing is parked rather than retried forever.

N-PORT zips are about 440 MB. They are streamed to disk and read a row at a time, and
only outside the session, because reading one holds the interpreter from the live pollers.
"""
from __future__ import annotations

import asyncio
import io
import logging
import os
import tempfile
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp

from shared.utils import sec_holdings as sh
from shared.utils.quiet_failures import swallowed
from shared.utils.sec_rate import sec_slot

logger = logging.getLogger("collector.sec_holdings")

NY = ZoneInfo("America/New_York")
POLL_SEC = 6 * 3600
BUSY_WAIT_SEC = 20 * 60
TIMEOUT = aiohttp.ClientTimeout(total=3600, sock_read=300)

_FILES_SQL = "SELECT name, loaded_at, attempts FROM sec_bulk_files WHERE kind = $1"
_ATTEMPT_SQL = """
    INSERT INTO sec_bulk_files (name, kind, attempts, last_error) VALUES ($1, $2, 1, $3)
    ON CONFLICT (name) DO UPDATE SET attempts = sec_bulk_files.attempts + 1, last_error = EXCLUDED.last_error"""
_DONE_SQL = """
    INSERT INTO sec_bulk_files (name, kind, attempts, rows_stored, loaded_at) VALUES ($1, $2, 1, $3, NOW())
    ON CONFLICT (name) DO UPDATE SET rows_stored = EXCLUDED.rows_stored, loaded_at = NOW(), last_error = NULL"""
_FTD_SQL = """
    INSERT INTO fails_to_deliver (settle_day, ticker, cusip, quantity, price) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (settle_day, ticker) DO UPDATE SET cusip = EXCLUDED.cusip, quantity = EXCLUDED.quantity, price = EXCLUDED.price"""
_CUSIP_SQL = "SELECT cusip, ticker FROM cusip_tickers"
_CUSIP_UPSERT_SQL = """
    INSERT INTO cusip_tickers (cusip, ticker, last_day) VALUES ($1, $2, $3)
    ON CONFLICT (cusip) DO UPDATE SET ticker = EXCLUDED.ticker, last_day = EXCLUDED.last_day
    WHERE cusip_tickers.last_day <= EXCLUDED.last_day"""
_NPORT_SQL = """
    INSERT INTO fund_holdings_monthly (quarter, report_month, ticker, positions, shares, value_usd, first_public)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (quarter, report_month, ticker) DO UPDATE SET positions = EXCLUDED.positions, shares = EXCLUDED.shares,
        value_usd = EXCLUDED.value_usd, first_public = EXCLUDED.first_public"""


def _session() -> aiohttp.ClientSession:
    # SEC asks for its contact agent; a compressed response would only be unpacked here.
    return aiohttp.ClientSession(timeout=TIMEOUT, connector=aiohttp.TCPConnector(ttl_dns_cache=300), headers={
        "User-Agent": os.getenv("SEC_USER_AGENT", ""), "Accept-Encoding": "identity"})


async def _page(session: aiohttp.ClientSession, url: str) -> str:
    await sec_slot()
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text(errors="ignore")


async def _todo(db: Any, kind: str, names: List[str]) -> List[str]:
    known = {r["name"]: r for r in await db.query(_FILES_SQL, kind) or []}
    return [n for n in names if n not in known or (not known[n]["loaded_at"] and sh.retry_allowed(known[n]["attempts"]))]


def _read_ftd_zip(body: bytes) -> List[sh.FtdRow]:
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        return sh.parse_ftd(z.read(z.namelist()[0]).decode("latin-1"))


async def load_ftd(session: aiohttp.ClientSession, db: Any) -> int:
    links = [u for u in sh.file_links(await _page(session, sh.FTD_PAGE), "cnsfails")
             if (sh.ftd_year(u) or 0) >= sh.FTD_START_YEAR]
    by_name = {u.rsplit("/", 1)[-1]: u for u in links}
    stored = 0
    for name in await _todo(db, "ftd", sorted(by_name)):
        try:
            await sec_slot()
            async with session.get(by_name[name]) as resp:
                resp.raise_for_status()
                body = await resp.read()
            rows = await asyncio.to_thread(_read_ftd_zip, body)
            if rows:
                await db.execute_many(_FTD_SQL, rows)
                latest = {}
                for day, ticker, cusip, _, _ in sorted(rows, key=lambda x: x[0]):
                    latest[cusip] = (cusip, ticker, day)
                await db.execute_many(_CUSIP_UPSERT_SQL, list(latest.values()))
            await db.execute(_DONE_SQL, name, "ftd", len(rows))
            stored += len(rows)
        except Exception as e:
            swallowed("collector.sec_holdings.ftd_file", e, logger, detail=name)
            await db.execute(_ATTEMPT_SQL, name, "ftd", str(e)[:300])
    return stored


def _read_nport(path: str, tickers: Dict[str, str]) -> Dict[Tuple[Any, str], List]:
    with zipfile.ZipFile(path) as z:
        def rows(name: str):
            with z.open(name) as raw:
                yield from sh.tsv(io.TextIOWrapper(raw, "utf-8", errors="replace", newline=""))
        filings = sh.latest_filings(rows("SUBMISSION.tsv"), rows("FUND_REPORTED_INFO.tsv"))
        return sh.aggregate_holdings(rows("FUND_REPORTED_HOLDING.tsv"), filings, tickers)


def _in_session(now: Optional[datetime] = None) -> bool:
    now = (now or datetime.now(NY)).astimezone(NY)
    return now.weekday() < 5 and 9 <= now.hour < 17


async def load_nport(session: aiohttp.ClientSession, db: Any) -> int:
    links = [u for u in sh.file_links(await _page(session, sh.NPORT_PAGE), r"nport\.zip") if sh.nport_quarter(u)]
    by_name = {sh.nport_quarter(u): u for u in links}
    tickers = {r["cusip"]: r["ticker"] for r in await db.query(_CUSIP_SQL) or []}
    if not tickers:
        return 0        # no fails loaded yet, so nothing to match against: not "no holdings"
    stored = 0
    for quarter in await _todo(db, "nport", sorted(by_name, reverse=True)):
        while _in_session():
            await asyncio.sleep(BUSY_WAIT_SEC)
        path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
                path = f.name
                await sec_slot()
                async with session.get(by_name[quarter]) as resp:
                    resp.raise_for_status()
                    async for chunk in resp.content.iter_chunked(1 << 20):
                        await asyncio.to_thread(f.write, chunk)
            held = await asyncio.to_thread(_read_nport, path, tickers)
            await db.execute_many(_NPORT_SQL, [(quarter, m, t, v[0], v[1], v[2], v[3]) for (m, t), v in held.items()])
            await db.execute(_DONE_SQL, quarter, "nport", len(held))
            stored += len(held)
            logger.info("N-PORT %s: %s month-stock rows", quarter, len(held))
        except Exception as e:
            swallowed("collector.sec_holdings.nport_file", e, logger, detail=quarter)
            await db.execute(_ATTEMPT_SQL, quarter, "nport", str(e)[:300])
        finally:
            if path and os.path.exists(path):
                os.remove(path)
    return stored


async def run_forever() -> None:
    from shared.db import get_timescale

    db = await get_timescale()
    while True:
        try:
            async with _session() as session:
                n = await load_ftd(session, db)
                if n:
                    logger.info("Fails-to-deliver: %s rows stored", n)
                await load_nport(session, db)
        except Exception as e:
            logger.error("SEC holdings pass failed: %s", e, exc_info=True)
        await asyncio.sleep(POLL_SEC)


__all__ = ["load_ftd", "load_nport", "run_forever"]
