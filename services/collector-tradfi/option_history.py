"""
services/collector-tradfi/option_history.py

Implied-volatility history (shared/utils/option_iv.py) into
option_iv_history: every session since Alpaca's option bars begin for the
most traded names, once, then each evening's session for the same names.

Per ticker: the monthly contracts (expired and live) from the trading API,
the stock's unadjusted daily closes (strikes are not split-adjusted, so
adjusted closes would misprice every contract before a split), and the daily
bars of each expiry's contracts within 25% of the stock over the 7-60 days
before it, a month at a time. About seventy requests a name, at about 130 a
minute, leaving the rest of the account's 200 to this collector's live work.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import aiohttp

from shared.db import get_redis, get_timescale
from shared.utils import option_iv as oiv
from shared.utils import options_positioning as op
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.tradfi.option_history")

CONTRACTS_URL = os.getenv("ALPACA_CONTRACTS_URL", "https://paper-api.alpaca.markets/v2/options/contracts")
OPTION_BARS_URL = "https://data.alpaca.markets/v1beta1/options/bars"
STOCK_BARS_URL = "https://data.alpaca.markets/v2/stocks/{ticker}/bars"
HISTORY_START = date(2024, 2, 1)           # Alpaca's option bars begin here
UNIVERSE = int(os.getenv("OPTION_IV_UNIVERSE", "600"))
STRIKE_BAND = 0.25
SYMBOLS_PER_REQUEST = 100
REQUEST_SPACING_SEC = 0.45
TAIL_DAYS = 7                              # each evening recomputes the last week (late prints)
TAIL_HOUR_UTC = 22
DONE_KEY = "sentinel:option_iv:backfilled"  # set of tickers whose history is loaded
TAIL_KEY = "sentinel:option_iv:tail_day"
UNIVERSE_KEY = "sentinel:option_iv:universe"  # fixed once chosen, so a restart resumes the same names
RETRY_SEC = 300

_UNIVERSE_SQL = """
    SELECT ticker FROM daily_bars
    WHERE day > CURRENT_DATE - 14 AND close > 5
    GROUP BY ticker HAVING count(*) >= 7
    ORDER BY avg(close * volume) DESC LIMIT $1
"""
_UPSERT_SQL = """
    INSERT INTO option_iv_history (ticker, day, atm_iv, iv_skew, skew_expiry_days, cp_iv_spread, cp_pairs,
                                   call_volume, put_volume, put_call_ratio, contracts, spot)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    ON CONFLICT (ticker, day) DO UPDATE SET
        atm_iv = EXCLUDED.atm_iv, iv_skew = EXCLUDED.iv_skew, skew_expiry_days = EXCLUDED.skew_expiry_days,
        cp_iv_spread = EXCLUDED.cp_iv_spread, cp_pairs = EXCLUDED.cp_pairs, call_volume = EXCLUDED.call_volume,
        put_volume = EXCLUDED.put_volume, put_call_ratio = EXCLUDED.put_call_ratio,
        contracts = EXCLUDED.contracts, spot = EXCLUDED.spot, updated_at = NOW()
"""


def _headers() -> Dict[str, str]:
    return {"APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID") or "",
            "APCA-API-SECRET-KEY": (os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET")
                                    or os.getenv("APCA_API_SECRET_KEY") or "")}


async def _get_pages(session: aiohttp.ClientSession, url: str, params: Dict[str, str], key: str) -> List[Any]:
    """Every page of a paged Alpaca listing; a dict-of-lists `key` is merged."""
    out: List[Any] = []
    merged: Dict[str, List[Any]] = {}
    token: Optional[str] = None
    while True:
        p = dict(params, **({"page_token": token} if token else {}))
        async with session.get(url, params=p, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status == 429:
                await asyncio.sleep(30)
                continue
            resp.raise_for_status()
            body = await resp.json()
        got = body.get(key) or ([] if key != "bars" else {})
        if isinstance(got, dict):
            for sym, rows in got.items():
                merged.setdefault(sym, []).extend(rows or [])
        else:
            out.extend(got)
        token = body.get("next_page_token")
        await asyncio.sleep(REQUEST_SPACING_SEC)
        if not token:
            return [merged] if merged else out


def third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(4 - first.weekday()) % 7 + 14)


def _months(start: date, end: date) -> List[Tuple[int, int]]:
    """Every (year, month) whose monthly expiry can fall 7-60 days after a
    session in [start, end]."""
    out: List[Tuple[int, int]] = []
    y, m = start.year, start.month
    last = end + timedelta(days=op.IV_MAX_DAYS)
    while (y, m) <= (last.year, last.month):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


async def _contracts(session: aiohttp.ClientSession, ticker: str, year: int, month: int, strikes: Tuple[float, float]
                     ) -> Dict[date, List[Tuple[str, float, str]]]:
    """One month's standard expiry -> (symbol, strike, right) within the strike
    band. Asked a month at a time with the band: SPY's whole two-year chain is
    hundreds of thousands of contracts, which put this collector out of memory
    (17 Oct)."""
    found: Dict[date, List[Tuple[str, float, str]]] = {}
    hi_d = third_friday(year, month)
    lo_d = hi_d - timedelta(days=1)          # the Thursday before, when that Friday is a holiday
    today = datetime.now(timezone.utc).date()
    statuses = (["inactive"] if hi_d < today else []) + (["active"] if hi_d >= today - timedelta(days=1) else [])
    for status in statuses:
        rows = await _get_pages(session, CONTRACTS_URL, {
            "underlying_symbols": ticker, "status": status, "limit": "10000",
            "expiration_date_gte": lo_d.isoformat(), "expiration_date_lte": hi_d.isoformat(),
            "strike_price_gte": f"{strikes[0]:.2f}", "strike_price_lte": f"{strikes[1]:.2f}"}, "option_contracts")
        for c in rows:
            try:
                exp = date.fromisoformat(str(c["expiration_date"]))
                right = "C" if str(c.get("type", "")).lower().startswith("c") else "P"
                # Only standard contracts: an adjusted one (after a split or a
                # special dividend) delivers something other than 100 shares.
                standard = str(c.get("root_symbol") or ticker).upper() == ticker.upper() and \
                    str(c.get("size") or "100") == "100"
                if not standard:
                    continue
                found.setdefault(exp, []).append((str(c["symbol"]), float(c["strike_price"]), right))
            except (KeyError, TypeError, ValueError):
                continue
    keep = set(oiv.monthly_expiries(found))
    return {e: v for e, v in found.items() if e in keep}


# The free plan serves consolidated (SIP) bars -- and option bars -- once they
# are fifteen minutes old. A date-only `end` of today reads as the whole day, still in progress, and
# the vendor refuses it (403): the evening update asked for exactly that and
# wrote nothing from 16 Oct 2026, every name refused.
SIP_DELAY = timedelta(minutes=16)
NY = ZoneInfo("America/New_York")


def sip_end(end: date, now: Optional[datetime] = None) -> str:
    """`end` for a SIP request: the date itself once that whole day in New York
    is more than the free plan's delay in the past, else now less the delay.

    The vendor reads a date-only `end` as the end of that day in New York, so
    at midnight UTC yesterday's date still reaches hours ahead -- comparing
    against the UTC date refused every request from 00:00 to 04:00 UTC."""
    now = now or datetime.now(timezone.utc)
    day_end = datetime.combine(end + timedelta(days=1), time.min, tzinfo=NY).astimezone(timezone.utc)
    if day_end <= now - SIP_DELAY:
        return end.isoformat()
    return (now - SIP_DELAY).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _raw_closes(session: aiohttp.ClientSession, ticker: str, start: date, end: date) -> Dict[date, float]:
    pages = await _get_pages(session, STOCK_BARS_URL.format(ticker=ticker), {
        "timeframe": "1Day", "adjustment": "raw", "feed": "sip", "limit": "10000",
        "start": start.isoformat(), "end": sip_end(end)}, "bars")
    out: Dict[date, float] = {}
    for b in pages:
        try:
            out[date.fromisoformat(str(b["t"])[:10])] = float(b["c"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


async def _option_bars(session: aiohttp.ClientSession, symbols: Sequence[str], start: date, end: date
                       ) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for i in range(0, len(symbols), SYMBOLS_PER_REQUEST):
        pages = await _get_pages(session, OPTION_BARS_URL, {
            "symbols": ",".join(symbols[i:i + SYMBOLS_PER_REQUEST]), "timeframe": "1Day", "limit": "10000",
            # Option bars carry the same fifteen-minute rule as stock bars.
            "start": start.isoformat(), "end": sip_end(end)}, "bars")
        for merged in pages:
            for sym, rows in merged.items():
                out.setdefault(sym, []).extend(rows)
    return out


async def history(session: aiohttp.ClientSession, ticker: str, start: date, end: date) -> List[Tuple[Any, ...]]:
    """Rows for option_iv_history over [start, end] for one ticker."""
    closes = await _raw_closes(session, ticker, start - timedelta(days=5), end)
    if not closes:
        return []
    per_day: Dict[date, List[Tuple[date, float, str, float, float]]] = {}
    for year, month in _months(start, end):
        nominal = third_friday(year, month)
        lo = max(start, nominal - timedelta(days=op.IV_MAX_DAYS))
        hi = min(end, nominal - timedelta(days=op.IV_MIN_DAYS - 1))
        spots = [c for d, c in closes.items() if lo <= d <= hi]
        if lo > hi or not spots:
            continue
        band = (min(spots) * (1 - STRIKE_BAND), max(spots) * (1 + STRIKE_BAND))
        found = await _contracts(session, ticker, year, month, band)
        if not found:
            continue
        exp = max(found)
        chosen = {sym: (k, right) for sym, k, right in found[exp]}
        bars = await _option_bars(session, sorted(chosen), lo, hi)
        for sym, rows in bars.items():
            k, right = chosen[sym]
            for b in rows:
                try:
                    d = date.fromisoformat(str(b["t"])[:10])
                    per_day.setdefault(d, []).append((exp, k, right, float(b["c"]), float(b.get("v") or 0.0)))
                except (KeyError, TypeError, ValueError):
                    continue
    out: List[Tuple[Any, ...]] = []
    for d in sorted(per_day):
        spot = closes.get(d)
        if not spot or not start <= d <= end:
            continue
        m = oiv.day_measures(d, spot, per_day[d])
        if m:
            out.append((ticker, d, m.get("atm_iv"), m.get("iv_skew"), m.get("skew_expiry_days"),
                        m.get("cp_iv_spread"), m.get("cp_pairs"), m.get("call_volume"), m.get("put_volume"),
                        m.get("put_call_ratio"), m.get("contracts"), spot))
    return out


async def _universe(db: Any, raw: Any) -> List[str]:
    """The most traded names, chosen once and kept (UNIVERSE_KEY)."""
    kept = await raw.get(UNIVERSE_KEY)
    if kept:
        return (kept.decode() if isinstance(kept, bytes) else str(kept)).split(",")
    names = [str(r["ticker"]).upper() for r in await db.query(_UNIVERSE_SQL, UNIVERSE) or []]
    if names:
        await raw.set(UNIVERSE_KEY, ",".join(names))
    return names


async def _backfill(session: aiohttp.ClientSession, raw: Any) -> None:
    db = await get_timescale()
    names = await _universe(db, raw)
    done = {x.decode() if isinstance(x, bytes) else str(x) for x in (await raw.smembers(DONE_KEY) or [])}
    todo = [t for t in names if t not in done]
    end = datetime.now(timezone.utc).date() - timedelta(days=1)
    logger.info("Option IV history: %s of %s names to load", len(todo), len(names))
    for n, t in enumerate(todo, 1):
        try:
            rows = await history(session, t, HISTORY_START, end)
            if rows:
                await db.execute_many(_UPSERT_SQL, rows)
            await raw.sadd(DONE_KEY, t)
            if n % 25 == 0:
                logger.info("Option IV history: %s/%s names (%s: %s days)", n, len(todo), t, len(rows))
        except Exception as e:
            swallowed("collector.tradfi.option_history.backfill", e, logger, detail=t)
            await asyncio.sleep(30)


async def _tail(session: aiohttp.ClientSession, raw: Any) -> None:
    """Each weekday evening, the last TAIL_DAYS sessions for the loaded names."""
    while True:
        now = datetime.now(timezone.utc)
        today = now.date()
        last = await raw.get(TAIL_KEY)
        last_s = last.decode() if isinstance(last, bytes) else last
        if now.hour >= TAIL_HOUR_UTC and now.weekday() < 5 and last_s != today.isoformat():
            db = await get_timescale()
            names = sorted({x.decode() if isinstance(x, bytes) else str(x)
                            for x in (await raw.smembers(DONE_KEY) or [])})
            for t in names:
                try:
                    rows = await history(session, t, today - timedelta(days=TAIL_DAYS), today)
                    if rows:
                        await db.execute_many(_UPSERT_SQL, rows)
                except Exception as e:
                    swallowed("collector.tradfi.option_history.tail", e, logger, detail=t)
            await raw.set(TAIL_KEY, today.isoformat())
            logger.info("Option IV history: evening update for %s names", len(names))
        await asyncio.sleep(900)


async def run_forever() -> None:
    if not _headers()["APCA-API-KEY-ID"]:
        logger.warning("Option IV history off: no Alpaca credentials")
        return
    raw = getattr(await get_redis(), "raw", None)
    if raw is None:
        logger.warning("Option IV history off: no Redis")
        return
    async with aiohttp.ClientSession(headers=_headers()) as session:
        while True:
            try:
                await _backfill(session, raw)
                break
            except Exception as e:
                # A busy database times the universe query out; try again later.
                swallowed("collector.tradfi.option_history.start", e, logger)
                await asyncio.sleep(RETRY_SEC)
        await _tail(session, raw)


__all__ = ["DONE_KEY", "HISTORY_START", "history", "run_forever"]
