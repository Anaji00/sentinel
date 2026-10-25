"""
services/collector-tradfi/earnings_estimates.py

Keeps Finnhub's free earnings data, which the platform otherwise only reads in
passing (main.poll_finnhub_earnings turns it into live alerts):

  - The earnings calendar from a week back to EARNINGS_LOOKAHEAD_DAYS ahead,
    snapshotted once a day: each row carries the consensus EPS and revenue
    estimate that day, so a revision is the difference between our own copies
    (no free source sells estimate history).
  - /stock/earnings for the most liquid names: actual against estimate for the
    last four reported quarters (the free tier's depth), refreshed weekly.

Free tier: 60 calls a minute; this module stays under a third of it.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.earnings_estimates")

NY = ZoneInfo("America/New_York")
FINNHUB = "https://finnhub.io/api/v1"
EARNINGS_LOOKBACK_DAYS = 7
EARNINGS_LOOKAHEAD_DAYS = 60
CALENDAR_WINDOW_DAYS = 2
CALENDAR_ROW_CAP = 1500
SNAPSHOT_HOUR_ET = 7                  # before the open, after overnight revisions
SURPRISE_NAMES = 1000
SURPRISE_REFRESH_DAYS = 7
CALL_PACE_SEC = 3.0                   # 20 calls a minute
LOOP_SEC = 30 * 60

_SNAP_SQL = """
    INSERT INTO earnings_estimates (snap_day, ticker, report_date, fiscal_year, fiscal_quarter, hour,
        eps_estimate, revenue_estimate, eps_actual, revenue_actual)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING
"""
_LIQUID_SQL = """
    SELECT ticker FROM daily_bars WHERE day > CURRENT_DATE - 31
    GROUP BY ticker ORDER BY avg(close * volume) DESC LIMIT $1
"""
_SURPRISE_SQL = """
    INSERT INTO earnings_surprises (ticker, period, eps_actual, eps_estimate, surprise_pct)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (ticker, period) DO UPDATE SET eps_actual = EXCLUDED.eps_actual,
        eps_estimate = EXCLUDED.eps_estimate, surprise_pct = EXCLUDED.surprise_pct
"""


def _f(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _i(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def calendar_rows(snap_day: date, items: List[Dict[str, Any]]) -> List[Tuple]:
    """Finnhub calendar entries -> earnings_estimates rows (malformed ones skipped)."""
    out = []
    for e in items or []:
        sym = str(e.get("symbol") or "").upper()
        try:
            rd = date.fromisoformat(str(e.get("date")))
        except ValueError:
            continue
        if not sym:
            continue
        out.append((snap_day, sym, rd, _i(e.get("year")), _i(e.get("quarter")), e.get("hour") or None,
                    _f(e.get("epsEstimate")), _f(e.get("revenueEstimate")),
                    _f(e.get("epsActual")), _f(e.get("revenueActual"))))
    return out


def surprise_rows(ticker: str, items: List[Dict[str, Any]]) -> List[Tuple]:
    """Finnhub /stock/earnings entries -> earnings_surprises rows."""
    out = []
    for e in items or []:
        try:
            period = date.fromisoformat(str(e.get("period")))
        except ValueError:
            continue
        out.append((ticker, period, _f(e.get("actual")), _f(e.get("estimate")), _f(e.get("surprisePercent"))))
    return out


async def _get(session: aiohttp.ClientSession, path: str, params: Dict[str, str]) -> Optional[Any]:
    try:
        async with session.get(f"{FINNHUB}{path}", params=params) as resp:
            if resp.status == 429:
                await asyncio.sleep(60)
                return None
            if resp.status != 200:
                logger.warning("Finnhub %s: HTTP %s", path, resp.status)
                return None
            return await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
        swallowed("collector.earnings_estimates.fetch", e, logger)
        return None


async def snapshot(session: aiohttp.ClientSession, db, key: str, today: date) -> int:
    """The calendar in CALENDAR_WINDOW_DAYS slices: Finnhub returns at most
    CALENDAR_ROW_CAP rows a call (the latest dates), so one wide call silently
    dropped the nearest month on 9 Oct 2026."""
    total = 0
    start = today - timedelta(days=EARNINGS_LOOKBACK_DAYS)
    end = today + timedelta(days=EARNINGS_LOOKAHEAD_DAYS)
    while start <= end:
        stop = min(end, start + timedelta(days=CALENDAR_WINDOW_DAYS - 1))
        body = await _get(session, "/calendar/earnings", {"from": start.isoformat(), "to": stop.isoformat(), "token": key})
        items = (body or {}).get("earningsCalendar") or []
        if len(items) >= CALENDAR_ROW_CAP:
            logger.warning("Finnhub calendar %s..%s hit the %s-row cap; some reports are missing",
                           start, stop, CALENDAR_ROW_CAP)
        rows = calendar_rows(today, items)
        if rows:
            await db.execute_many(_SNAP_SQL, rows)
        total += len(rows)
        start = stop + timedelta(days=1)
        await asyncio.sleep(CALL_PACE_SEC)
    return total


async def surprises(session: aiohttp.ClientSession, db, key: str, checked: Dict[str, datetime]) -> int:
    """Each liquid name at most once per SURPRISE_REFRESH_DAYS, including the
    ones Finnhub has nothing for (funds), so they are not asked again each pass."""
    names = [str(r["ticker"]).upper() for r in await db.query(_LIQUID_SQL, SURPRISE_NAMES) or []]
    stale = datetime.now(NY) - timedelta(days=SURPRISE_REFRESH_DAYS)
    stored = 0
    for t in names:
        if checked.get(t, stale) > stale:
            continue
        checked[t] = datetime.now(NY)
        body = await _get(session, "/stock/earnings", {"symbol": t, "token": key})
        rows = surprise_rows(t, body if isinstance(body, list) else [])
        if rows:
            await db.execute_many(_SURPRISE_SQL, rows)
            stored += len(rows)
        await asyncio.sleep(CALL_PACE_SEC)
    return stored


async def run_forever() -> None:
    key = os.getenv("FINNHUB_API_KEY")
    if not key:
        logger.warning("FINNHUB_API_KEY not set: earnings estimates are not kept.")
        return
    from shared.db import get_timescale

    db = await get_timescale()
    timeout = aiohttp.ClientTimeout(total=30)
    checked: Dict[str, datetime] = {}
    snapped: Optional[date] = None
    async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(ttl_dns_cache=300)) as session:
        while True:
            try:
                now = datetime.now(NY)
                if now.hour >= SNAPSHOT_HOUR_ET and snapped != now.date():
                    n = await snapshot(session, db, key, now.date())
                    snapped = now.date()
                    if n:
                        logger.info("Earnings estimates: %s calendar rows snapshotted for %s", n, now.date())
                m = await surprises(session, db, key, checked)
                if m:
                    logger.info("Earnings surprises: %s quarter rows stored", m)
            except Exception as e:
                logger.error("Earnings estimates pass failed: %s", e, exc_info=True)
            await asyncio.sleep(LOOP_SEC)


__all__ = ["calendar_rows", "run_forever", "surprise_rows"]
