"""
services/collector-filings/insider_history.py

Insider open-market trades from the SEC's quarterly Form 3/4/5 data sets
(shared/utils/insider_history.py), oldest quarter first, so each trade is
classified routine or opportunistic against the three years already loaded.

Every trade goes into insider_transactions. Opportunistic trades, and routine
purchases as the control, also go into signal_event_archive in the live Form 4
event's shape, so the reaction study judges the insider families on five years
instead of the weeks the live feed has seen. One zip a quarter (about 8 MB),
paced by the shared SEC slot; a loaded quarter is recorded in Redis and never
fetched again, and the newest completes once the SEC posts it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils import insider_history as ih  # noqa: E402
from shared.utils.quiet_failures import swallowed  # noqa: E402
from shared.utils.sec_rate import sec_slot  # noqa: E402

logger = logging.getLogger("collector_filings.insider_history")

SEC_HEADERS = {"User-Agent": os.getenv("SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local")}
DONE_KEY = "sentinel:history:form345:done"
PASS_SEC = 24 * 3600
FIRST_DELAY_SEC = 20 * 60
# The archive opens with the first daily price bar. Classifying a trade needs three
# years of the insider's earlier trades, so trades are loaded from 2013.
ARCHIVE_START = date(2016, 1, 1)
LOAD_START = (2013, 1)
ARCHIVED_KEY = "sentinel:history:form345:archived2016"   # quarters re-classified and archived from the table

_INSERT_SQL = f"""
    INSERT INTO {ih.INSIDER_TABLE} (accession, line, owner_cik, filed, traded, issuer_cik, ticker, owner_name,
        relationship, title, code, shares, price, planned)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ON CONFLICT (accession, line, owner_cik) DO NOTHING
"""
_PRIOR_SQL = f"""
    SELECT owner_cik, traded FROM {ih.INSIDER_TABLE}
    WHERE owner_cik = ANY($1::text[]) AND traded >= $2::date AND traded < $3::date
"""
_CLASS_SQL = f"UPDATE {ih.INSIDER_TABLE} SET insider_class = $4 WHERE accession = $1 AND line = $2 AND owner_cik = $3"
_ARCHIVE_SQL = """
    INSERT INTO signal_event_archive
        (event_id, type, source, created_at, occurred_at, headline, summary, tags,
         sentiment, primary_entity_id, financial_data, region)
    VALUES ($1, 'insider_trade', $2, $3, $3, $4, $5, $6, NULL, $7, $8::jsonb, NULL)
    ON CONFLICT DO NOTHING
"""


async def _fetch(session: aiohttp.ClientSession, year: int, q: int) -> Optional[bytes]:
    """The quarter's zip, from whichever of the SEC's two paths holds it; None
    when neither does yet."""
    for prefix in ih.URL_PREFIXES:
        await sec_slot()
        async with session.get(ih.quarter_url(year, q, prefix), headers=SEC_HEADERS,
                               timeout=aiohttp.ClientTimeout(total=300)) as resp:
            if resp.status == 200:
                return await resp.read()
    return None


def _archive_rows(trades: List[ih.Trade], classes: Dict[Tuple[str, str, str], Optional[str]],
                  since: date) -> List[Tuple[Any, ...]]:
    """One archive row per filing, owner and direction: opportunistic trades
    and routine purchases (the control), in the live event's shape."""
    groups: Dict[Tuple[str, str, str], List[ih.Trade]] = defaultdict(list)
    for t in trades:
        if t.filed >= since:
            groups[(t.accession, t.owner_cik, t.code)].append(t)
    rows = []
    for (acc, owner, code), ts in groups.items():
        cls = next((classes.get((t.accession, t.line, t.owner_cik)) for t in ts), None)
        if cls != "opportunistic" and not (cls == "routine" and code == "P"):
            continue
        first = ts[0]
        value = sum(t.shares * t.price for t in ts if t.price)
        label = "Open Market Buy" if code == "P" else "Open Market Sale"
        role = (first.title or first.relationship or "INSIDER").upper()
        rows.append((
            f"f345:{acc}:{owner}:{code}", ih.ARCHIVE_SOURCE, ih.known_at(first.filed),
            f"Insider {label}: {first.ticker} ${value / 1e6:.1f}M by {role}",
            f"{first.owner_name} ({first.relationship}), {cls} trader, filed {first.filed.isoformat()}",
            ih.archive_tags(code, cls, first.planned) + [first.ticker.lower()],
            first.ticker,
            json.dumps({"ticker": first.ticker, "premium_usd": round(value, 2) or None, "insider_class": cls,
                        "instrument_type": "equity"}),
        ))
    return rows


async def load_quarter(db: Any, session: aiohttp.ClientSession, year: int, q: int) -> Optional[Tuple[int, int]]:
    """Load, classify and archive one quarter. (trades, archived), or None when
    the SEC has not posted it."""
    blob = await _fetch(session, year, q)
    if blob is None:
        return None
    trades = await asyncio.to_thread(ih.parse_quarter, blob)
    del blob
    for i in range(0, len(trades), 2000):
        await db.execute_many(_INSERT_SQL, [
            (t.accession, t.line, t.owner_cik, t.filed, t.traded, t.issuer_cik, t.ticker, t.owner_name,
             t.relationship, t.title, t.code, t.shares, t.price, t.planned) for t in trades[i:i + 2000]])
    return await classify_and_archive(db, trades, year, q)


def quarter_bounds(year: int, q: int) -> Tuple[date, date]:
    """First day of the quarter and of the next one."""
    return date(year, 3 * q - 2, 1), date(year + (q == 4), 1 if q == 4 else 3 * q + 1, 1)


def archive_quarters(today: date) -> List[Tuple[int, int]]:
    """Quarters whose zips were loaded when the archive began five years back
    from `today`, so only part of them (or none) was archived: from the
    archive's start to the quarter holding that day. The table pass redoes them."""
    window_start = date(today.year - 5, today.month, 1)
    return ih.quarters(quarter_bounds(window_start.year, (window_start.month - 1) // 3 + 1)[1],
                       (ARCHIVE_START.year, (ARCHIVE_START.month - 1) // 3 + 1))


def trades_from_rows(rows: List[Dict[str, Any]]) -> Tuple[List[ih.Trade], Dict[Tuple[str, str, str], Optional[str]]]:
    """Trades, and their stored classes, from insider_transactions rows."""
    trades, current = [], {}
    for r in rows:
        t = ih.Trade(accession=r["accession"], line=r["line"], filed=r["filed"], traded=r["traded"],
                     issuer_cik=r["issuer_cik"], ticker=r["ticker"], owner_cik=r["owner_cik"],
                     owner_name=r["owner_name"], relationship=r["relationship"], title=r["title"],
                     code=r["code"], shares=float(r["shares"]), price=float(r["price"]) if r["price"] else None,
                     planned=bool(r["planned"]))
        trades.append(t)
        current[(t.accession, t.line, t.owner_cik)] = r.get("insider_class")
    return trades, current


_TABLE_SQL = f"""
    SELECT accession, line, owner_cik, filed, traded, issuer_cik, ticker, owner_name, relationship, title, code,
           shares, price, planned, insider_class
    FROM {ih.INSIDER_TABLE} WHERE filed >= $1::date AND filed < $2::date
"""


async def reprocess_quarter(db: Any, year: int, q: int) -> Tuple[int, int]:
    """Classify and archive a quarter already in insider_transactions, no network.
    (trades, archived)."""
    start, end = quarter_bounds(year, q)
    trades, current = trades_from_rows(await db.query(_TABLE_SQL, start, end) or [])
    return await classify_and_archive(db, trades, year, q, current)


async def classify_and_archive(db: Any, trades: List[ih.Trade], year: int, q: int,
                               current: Optional[Dict[Tuple[str, str, str], Optional[str]]] = None
                               ) -> Tuple[int, int]:
    """Classify a quarter's trades against the earlier ones in the table, store
    the classes that changed, and archive them from ARCHIVE_START."""
    owners = sorted({t.owner_cik for t in trades})
    q_end = quarter_bounds(year, q)[1]
    prior: Dict[str, List[date]] = defaultdict(list)
    for i in range(0, len(owners), 5000):
        for r in await db.query(_PRIOR_SQL, owners[i:i + 5000], date(year - ih.CLASSIFY_YEARS - 1, 1, 1), q_end) or []:
            prior[str(r["owner_cik"])].append(r["traded"])
    classes: Dict[Tuple[str, str, str], Optional[str]] = {}
    for t in trades:
        classes[(t.accession, t.line, t.owner_cik)] = ih.classify(
            t.traded, [d for d in prior.get(t.owner_cik, []) if d < t.traded], t.planned)
    updates = [(a, ln, o, c) for (a, ln, o), c in classes.items()
               if c and (current is None or current.get((a, ln, o)) != c)]
    for i in range(0, len(updates), 5000):
        await db.execute_many(_CLASS_SQL, updates[i:i + 5000])
    archive = _archive_rows(trades, classes, ARCHIVE_START)
    for i in range(0, len(archive), 2000):
        await db.execute_many(_ARCHIVE_SQL, archive[i:i + 2000])
    return len(trades), len(archive)


async def run_forever() -> None:
    """Load every missing quarter oldest first, then look for the next daily."""
    await asyncio.sleep(FIRST_DELAY_SEC)
    while True:
        try:
            db, redis = await get_timescale(), await get_redis()
            raw = getattr(redis, "raw", redis)
            done = {x.decode() if isinstance(x, bytes) else x for x in await raw.smembers(DONE_KEY) or []}
            async with aiohttp.ClientSession() as session:
                today = datetime.now(timezone.utc).date()
                for year, q in ih.quarters(today, LOAD_START):
                    tag = f"{year}q{q}"
                    if tag in done:
                        continue
                    out = await load_quarter(db, session, year, q)
                    if out is None:
                        logger.info("Insider history: %s not posted yet", tag)
                        break
                    await raw.sadd(DONE_KEY, tag)
                    logger.info("Insider history %s: %s open-market trades, %s archived", tag, *out)
            # Quarters loaded before the archive reached back to 2016 (and before
            # 2013-15 gave 2016-18 their three prior years): classify and archive
            # them from the table, oldest first, once each.
            done = {x.decode() if isinstance(x, bytes) else x for x in await raw.smembers(DONE_KEY) or []}
            archived = {x.decode() if isinstance(x, bytes) else x for x in await raw.smembers(ARCHIVED_KEY) or []}
            prior_loaded = all(f"{y}q{n}" in done for y, n in ih.quarters(ARCHIVE_START, LOAD_START))
            for year, q in archive_quarters(today) if prior_loaded else []:
                tag = f"{year}q{q}"
                if tag in archived or tag not in done:
                    continue
                out = await reprocess_quarter(db, year, q)
                await raw.sadd(ARCHIVED_KEY, tag)
                logger.info("Insider history %s re-archived from table: %s trades, %s archived", tag, *out)
        except Exception as e:
            swallowed("collector_filings.insider_history", e, logger)
        await asyncio.sleep(PASS_SEC)


__all__ = ["DONE_KEY", "load_quarter", "run_forever"]
