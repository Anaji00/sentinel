"""
services/collector-filings/fundamentals.py

Reported financials for every filer, from SEC XBRL frames, into `fundamentals`.

A frame is one line item for every company for one period
(data.sec.gov/api/xbrl/frames/us-gaap/Assets/USD/CY2025Q4I.json): about 6,000
filers in a single request, so five years of the items the cross-sectional
model needs is under a hundred requests. The model reads them point in time
(a period counts from 90 days after it ends), for value, profitability,
investment and accruals -- the fundamentals every institutional equity model
carries and this one had none of.

Frames already fetched are not fetched again unless recent, when late filers
and amendments still arrive.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Tuple

from shared.utils.quiet_failures import swallowed
from shared.utils.sec_rate import sec_slot

logger = logging.getLogger("collector.filings.fundamentals")

FRAMES_URL = "https://data.sec.gov/api/xbrl/frames/{taxonomy}/{concept}/{unit}/{frame}.json"
FIRST_YEAR = 2020
REQUEST_SPACING_SEC = 0.25
REFRESH_INTERVAL_SEC = 24 * 3600
DONE_KEY = "sentinel:fundamentals:frames_done"
RECENT_QUARTERS = 4          # refetched every pass: late filers and amendments

# Balance-sheet items at each quarter end; flows over each calendar year.
INSTANT = (
    ("us-gaap", "Assets", "USD"),
    ("us-gaap", "StockholdersEquity", "USD"),
    ("dei", "EntityCommonStockSharesOutstanding", "shares"),
)
ANNUAL = (
    ("us-gaap", "NetIncomeLoss", "USD"),
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities", "USD"),
)

UPSERT_SQL = """
    INSERT INTO fundamentals (cik, ticker, concept, frame, period_start, period_end, value, accn)
    VALUES ($1, $2, $3, $4, $5::date, $6::date, $7, $8)
    ON CONFLICT (cik, concept, frame) DO UPDATE SET
        ticker = EXCLUDED.ticker, period_start = EXCLUDED.period_start, period_end = EXCLUDED.period_end,
        value = EXCLUDED.value, accn = EXCLUDED.accn
"""


def frames_wanted(today: date) -> List[Tuple[str, str, str, str, bool]]:
    """(taxonomy, concept, unit, frame, recent) for every frame from FIRST_YEAR."""
    out = []
    last_q = (today.month - 1) // 3          # quarters fully ended this year
    quarters = [(y, q) for y in range(FIRST_YEAR, today.year + 1) for q in range(1, 5)
                if (y, q) <= (today.year, last_q) and not (y == today.year and q > last_q)]
    recent_q = set(quarters[-RECENT_QUARTERS:])
    for tax, concept, unit in INSTANT:
        for y, q in quarters:
            out.append((tax, concept, unit, f"CY{y}Q{q}I", (y, q) in recent_q))
    for tax, concept, unit in ANNUAL:
        for y in range(FIRST_YEAR, today.year):
            out.append((tax, concept, unit, f"CY{y}", y >= today.year - 2))
    return out


def rows_from(payload: Dict[str, Any], concept: str, frame: str, cik_to_ticker: Dict[str, str]) -> List[Tuple]:
    out = []
    for r in (payload or {}).get("data") or []:
        try:
            cik = str(int(r["cik"]))
            end = date.fromisoformat(str(r["end"])[:10])
            val = float(r["val"])
        except (KeyError, TypeError, ValueError):
            continue
        start = None
        if r.get("start"):
            try:
                start = date.fromisoformat(str(r["start"])[:10])
            except ValueError:
                start = None
        out.append((cik, cik_to_ticker.get(cik), concept, frame, start, end, val, r.get("accn")))
    return out


async def sync(session: Any, headers: Dict[str, str], db: Any, raw: Any, cik_to_ticker: Dict[str, str]) -> Dict[str, int]:
    done = {(m.decode() if isinstance(m, bytes) else str(m)) for m in (await raw.smembers(DONE_KEY) or [])}
    stats = {"frames": 0, "rows": 0, "errors": 0}
    for tax, concept, unit, frame, recent in frames_wanted(datetime.now(timezone.utc).date()):
        key = f"{concept}:{frame}"
        if key in done and not recent:
            continue
        url = FRAMES_URL.format(taxonomy=tax, concept=concept, unit=unit, frame=frame)
        try:
            await sec_slot()
            async with session.get(url, headers=headers) as resp:
                if resp.status == 404:
                    await raw.sadd(DONE_KEY, key)      # no filer reported it for this period
                    continue
                if resp.status == 429:
                    logger.warning("SEC throttled the fundamentals pass; pausing a minute.")
                    await asyncio.sleep(60)
                    continue
                if resp.status != 200:
                    stats["errors"] += 1
                    continue
                payload = await resp.json(content_type=None)
            rows = rows_from(payload, concept, frame, cik_to_ticker)
            if rows:
                await db.execute_many(UPSERT_SQL, rows)
            await raw.sadd(DONE_KEY, key)
            stats["frames"] += 1
            stats["rows"] += len(rows)
        except Exception as e:
            stats["errors"] += 1
            swallowed("collector_filings.fundamentals.frame", e, logger, detail=key)
        await asyncio.sleep(REQUEST_SPACING_SEC)
    return stats


async def run_forever(headers: Dict[str, str], redis_client: Any, cik_to_ticker_fn) -> None:
    """Its own HTTP session: the collector's shared one is capped at ten
    connections that EDGAR and 13F downloads keep busy, and the short-interest
    loop waited on it indefinitely the same way."""
    import aiohttp
    from shared.db import get_timescale

    raw = getattr(redis_client, "raw", redis_client)
    await asyncio.sleep(240)
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
                stats = await sync(session, headers, await get_timescale(), raw, cik_to_ticker_fn() or {})
                if stats["frames"]:
                    logger.info("SEC fundamentals: %s frames, %s rows, %s errors.",
                                stats["frames"], stats["rows"], stats["errors"])
        except Exception as e:
            swallowed("collector_filings.fundamentals", e, logger)
        await asyncio.sleep(REFRESH_INTERVAL_SEC)


__all__ = ["frames_wanted", "rows_from", "run_forever", "sync"]
