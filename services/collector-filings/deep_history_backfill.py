"""
services/collector-filings/deep_history_backfill.py

One-off, paced run of the three history loaders back to 2016, outside the
collector's own loops. Resumable: every step records what it finished in Redis
and writes with ON CONFLICT DO NOTHING, so a stopped run continues and a rerun
duplicates nothing.

    docker exec -w /app sentinel-filings python services/collector-filings/deep_history_backfill.py 8k insider 13f

Steps run in the order given: `8k` (EDGAR submissions, with their older pages),
`insider` (Form 3/4/5 zips for 2013-2015, then re-classify and archive 2016 on
from the table) and `13f`. Runs at low priority and spaces requests beyond the
shared SEC slot, to leave the host to the nightly model run.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))
sys.path.insert(0, str(HERE))

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils import insider_history as ih  # noqa: E402
from shared.utils.quiet_failures import swallowed  # noqa: E402
from shared.utils.signal_families import backfill_universe  # noqa: E402
from shared.utils.signal_store import EDGAR_8K_HISTORY_DONE_KEY  # noqa: E402
from shared.utils.watchlists import WATCHED_EQUITIES_KEY  # noqa: E402

import history  # noqa: E402
import insider_history  # noqa: E402
import ownership_history  # noqa: E402

logger = logging.getLogger("collector_filings.deep_history")
EXTRA_SPACING_SEC = 0.25     # on top of the 6 requests a second the shared slot allows


async def run_8k(session: aiohttp.ClientSession, db, redis) -> None:
    import main as filings_main
    from thirteen_f import load_sec_company_tickers, resolve_ticker_dynamically
    raw = getattr(redis, "raw", redis)
    await load_sec_company_tickers(session, redis)
    tickers = set(backfill_universe())
    tickers |= {(m.decode() if isinstance(m, bytes) else str(m)).upper()
                for m in (await raw.zrange(WATCHED_EQUITIES_KEY, 0, -1) or [])}
    tickers |= {str(r["ticker"]).upper() for r in await db.query("SELECT DISTINCT ticker FROM daily_bars") or []}
    mapping = history.universe_ciks(resolve_ticker_dynamically.__globals__.get("_DYNAMIC_CIK_TO_TICKER") or {}, tickers)
    for r in await db.query("SELECT ticker, cik FROM issuer_ticker") or []:
        mapping.setdefault(str(r["ticker"]).upper(), str(r["cik"]))
    logger.info("8-K deep history: %s tickers to check", len(mapping))
    stats = await history.backfill_8k_history(
        session, filings_main.SEC_HEADERS, db, redis, mapping, filings_main.ITEM_DESCRIPTIONS,
        EDGAR_8K_HISTORY_DONE_KEY, extra_spacing_sec=EXTRA_SPACING_SEC)
    logger.info("8-K deep history pass finished: %s", stats)


async def run_insider(db, redis) -> None:
    raw = getattr(redis, "raw", redis)
    today = datetime.now(timezone.utc).date()
    async with aiohttp.ClientSession() as session:
        done = {x.decode() if isinstance(x, bytes) else x for x in await raw.smembers(insider_history.DONE_KEY) or []}
        for year, q in ih.quarters(today, insider_history.LOAD_START):
            tag = f"{year}q{q}"
            if tag in done:
                continue
            out = await insider_history.load_quarter(db, session, year, q)
            if out is None:
                logger.info("Insider: %s not posted yet; stopping the load", tag)
                break
            await raw.sadd(insider_history.DONE_KEY, tag)
            logger.info("Insider %s: %s open-market trades, %s archived", tag, *out)
            await asyncio.sleep(2)
    done = {x.decode() if isinstance(x, bytes) else x for x in await raw.smembers(insider_history.DONE_KEY) or []}
    archived = {x.decode() if isinstance(x, bytes) else x
                for x in await raw.smembers(insider_history.ARCHIVED_KEY) or []}
    if not all(f"{y}q{n}" in done for y, n in ih.quarters(insider_history.ARCHIVE_START, insider_history.LOAD_START)):
        logger.warning("Insider: 2013-2015 quarters are not all loaded; not re-classifying 2016 on")
        return
    for year, q in insider_history.archive_quarters(today):
        tag = f"{year}q{q}"
        if tag in archived or tag not in done:
            continue
        out = await insider_history.reprocess_quarter(db, year, q)
        await raw.sadd(insider_history.ARCHIVED_KEY, tag)
        logger.info("Insider %s re-archived from the table: %s trades, %s archived", tag, *out)
        await asyncio.sleep(1)


async def run_13f(session: aiohttp.ClientSession, db, redis) -> None:
    from thirteen_f import load_sec_company_tickers, resolve_ticker_dynamically
    await load_sec_company_tickers(session, redis)
    n = await ownership_history.run_once(resolve_ticker_dynamically)
    logger.info("13F deep history: %s flows archived", n)


async def main(steps: list[str]) -> None:
    try:
        os.nice(15)
    except (AttributeError, OSError) as e:
        swallowed("collector_filings.deep_history.nice", e, logger)
    db, redis = await get_timescale(), await get_redis()
    connector = aiohttp.TCPConnector(limit=2, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        for step in steps:
            try:
                if step == "8k":
                    await run_8k(session, db, redis)
                elif step == "insider":
                    await run_insider(db, redis)
                elif step == "13f":
                    await run_13f(session, db, redis)
                else:
                    logger.error("unknown step %r (use 8k, insider, 13f)", step)
            except Exception as e:
                swallowed(f"collector_filings.deep_history.{step}", e, logger)
                logger.error("step %s stopped: %s", step, e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(main(sys.argv[1:] or ["insider", "13f", "8k"]))
