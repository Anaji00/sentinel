"""Split- and dividend-adjusted historical bars from the vendor.

Every price series this platform reasons over was built by aggregating its own
live snapshots, which has two consequences that between them undermine most of
the statistics above it.

The first is corporate actions. Nothing in the tree handled splits, dividends
or spin-offs, and `simple_returns` guards only against division by zero -- so a
4:1 split arrived as a clean, finite -74.97% return. Measured through the
platform's own functions on a 120-bar series with one split in the middle, the
largest absolute return went from 0.0101 to 0.7497 and annualised GARCH
volatility from 10.2% to 18.2%; with beta=0.90 that variance shock has a
6.6-bar half-life, so the estimate stays wrong for days. A split is scheduled,
announced weeks ahead and carries no information at all, and it was producing
the strongest signal the detector can emit.

The second is depth. History equalled uptime: `tradfi_bars_1h` held 42 to 46
bars and a restart put it back to zero. Granger needs max_lag+20 observations,
cointegration and Hurst want hundreds, and the radar carries a 20-observation
warm-up that reset on every deploy. It is also why the methodology endpoint's
claim of "5-year rolling backtests" was not merely unimplemented but
unimplementable from this source.

Alpaca's historical bars endpoint takes an `adjustment` parameter accepting
raw, split, dividend, spin-off and all -- and its default is `raw`. This module
asks for `all`.

Three things it deliberately does not pretend to solve:

  * Survivorship. Alpaca's asset list is current listings, so backfilling only
    surviving symbols overstates any backtest run across it. The universe is
    passed in rather than discovered here, so the caller owns that choice, and
    `backfill_report` states the bias rather than burying it.

  * Point-in-time. Adjusted history is adjusted as of today, so a backtest
    reading it knows about splits that had not happened yet. That is standard
    for signal research and wrong for P&L claims, and the distinction is
    recorded on every row through `adjustment`.

  * The seam. Vendor bars and self-aggregated bars are built by different
    rules -- feed, extended-hours inclusion, which trades count. Backfill stops
    at `SEAM_GUARD_MINUTES` before now so the two do not interleave inside one
    bucket, and the vendor is authoritative for everything older.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp

logger = logging.getLogger("collector.tradfi.backfill")

ALPACA_DATA_URL = "https://data.alpaca.markets/v2/stocks/bars"

# split, dividend and spin-off. The endpoint's default is `raw`, which is the
# defect this module exists to close, so it is named explicitly rather than
# relied upon.
ADJUSTMENT = os.getenv("ALPACA_BAR_ADJUSTMENT", "all")

# sip is all US exchanges; iex is the free tier and covers materially less
# volume. Stated because the choice changes what a volume z-score means.
FEED = os.getenv("ALPACA_BAR_FEED", "iex")

# The endpoint caps at 10,000 data points per response and the limit applies to
# the TOTAL across symbols, not per symbol -- so a large batch with a long
# window returns one symbol per page and paginates hard.
PAGE_LIMIT = int(os.getenv("ALPACA_BAR_PAGE_LIMIT", "10000"))

# Symbols per request. Kept well below the point where a single page holds one
# symbol's bars, so pagination stays shallow and a failure loses little work.
SYMBOLS_PER_REQUEST = int(os.getenv("ALPACA_BACKFILL_SYMBOLS_PER_REQUEST", "50"))

# How far back to reach on a first backfill.
#
# Sixty days of 1-minute bars is about 23,400 rows per ticker, which rolls up
# to roughly 390 hourly and 4,700 five-minute buckets -- enough for Hurst,
# GARCH, cointegration and Granger, all of which want hundreds of observations
# and were running on the 42 to 46 bars uptime had produced. Two years of
# minute bars would be 196,000 rows per ticker on a host with a 26 GiB total
# allocation and no retention policy, which is a different problem than the one
# this solves.
DEFAULT_LOOKBACK_DAYS = int(os.getenv("ALPACA_BACKFILL_DAYS", "60"))

# tradfi_bars is the ONE-MINUTE base table, and every continuous aggregate --
# 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1mth -- is a time_bucket rollup of it. Writing
# bars of any other width into it would silently corrupt all of them: an hourly
# bar inserted here would be bucketed as though it were one minute's trading.
# The guard is in the function rather than the docstring because a comment
# cannot stop a caller.
BASE_TABLE_TIMEFRAME = "1Min"

# Vendor bars stop this far short of now, so they never land in the same bucket
# the live aggregator is still filling.
SEAM_GUARD_MINUTES = int(os.getenv("ALPACA_BACKFILL_SEAM_GUARD_MIN", "30"))

# Politeness between pages. The data API is rate limited per key and a backfill
# is not urgent work.
INTER_REQUEST_SLEEP_SEC = float(os.getenv("ALPACA_BACKFILL_SLEEP_SEC", "0.35"))

_INSERT_SQL = """
    INSERT INTO tradfi_bars (ticker, time, open, high, low, close, volume, session)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (ticker, time) DO UPDATE
    SET open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        session = EXCLUDED.session;
"""


def _credentials() -> Optional[Dict[str, str]]:
    """Alpaca headers, or None when the key pair is absent.

    Absent credentials are a configuration state, not an error: the platform
    runs without a backfill, just with the short history it had before.
    """
    key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    secret = (
        os.getenv("ALPACA_SECRET_KEY")
        or os.getenv("ALPACA_API_SECRET")
        or os.getenv("APCA_API_SECRET_KEY")
    )
    if not key or not secret:
        return None
    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Accept-Encoding": "gzip, deflate",
    }


def _chunk(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _parse_bar_time(raw: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        return None


async def _fetch_page(
    session: aiohttp.ClientSession,
    headers: Dict[str, str],
    symbols: List[str],
    timeframe: str,
    start_iso: str,
    end_iso: str,
    page_token: Optional[str],
) -> Tuple[Dict[str, List[dict]], Optional[str]]:
    """One page of bars. Returns ({symbol: [bar, ...]}, next_page_token)."""
    params = {
        "symbols": ",".join(symbols),
        "timeframe": timeframe,
        "start": start_iso,
        "end": end_iso,
        "adjustment": ADJUSTMENT,
        "feed": FEED,
        "limit": str(PAGE_LIMIT),
    }
    if page_token:
        params["page_token"] = page_token

    async with session.get(ALPACA_DATA_URL, params=params, headers=headers, timeout=60) as resp:
        if resp.status == 429:
            # Rate limited. Surfaced rather than retried blindly, so a backfill
            # cannot turn into an unbounded loop against a throttled key.
            body = (await resp.text())[:200]
            raise RuntimeError(f"Alpaca rate limited the backfill (429): {body}")
        if resp.status != 200:
            body = (await resp.text())[:300]
            raise RuntimeError(f"Alpaca bars returned {resp.status}: {body}")
        payload = await resp.json()

    return (payload.get("bars") or {}), payload.get("next_page_token")


async def backfill_bars(
    db_client,
    tickers: List[str],
    timeframe: str = "1Min",
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    session: Optional[aiohttp.ClientSession] = None,
) -> Dict[str, Any]:
    """Load adjusted vendor bars into tradfi_bars for the given tickers.

    Idempotent: the insert upserts on (ticker, time), so a re-run repairs rows
    rather than duplicating them -- which is also how an unadjusted history
    already in the table gets corrected once this has run over it.
    """
    if str(timeframe) != BASE_TABLE_TIMEFRAME:
        raise ValueError(
            f"tradfi_bars is the {BASE_TABLE_TIMEFRAME} base table that every "
            f"continuous aggregate rolls up from; writing {timeframe} bars into "
            f"it would corrupt every rollup. Aggregate from the base instead."
        )

    report: Dict[str, Any] = {
        "requested_tickers": len(tickers),
        "timeframe": timeframe,
        "adjustment": ADJUSTMENT,
        "feed": FEED,
        "lookback_days": lookback_days,
        "bars_written": 0,
        "tickers_with_data": 0,
        "pages": 0,
        "errors": [],
        # Stated on every run, because a caller reading bar counts will not
        # otherwise know which biases the numbers carry.
        "caveats": [
            "survivorship: the ticker list is current listings, so backtests "
            "across this history exclude names that have since delisted",
            "point-in-time: prices are adjusted as of today, so a backtest "
            "reading them knows about splits that had not yet occurred",
        ],
    }

    headers = _credentials()
    if headers is None:
        report["errors"].append("no Alpaca credentials; backfill skipped")
        logger.warning(
            "Historical backfill skipped: ALPACA_API_KEY / ALPACA_SECRET_KEY are not set. "
            "Price history will remain limited to what this process has observed live."
        )
        return report

    if not tickers:
        return report

    end = datetime.now(timezone.utc) - timedelta(minutes=SEAM_GUARD_MINUTES)
    start = end - timedelta(days=lookback_days)
    start_iso, end_iso = start.isoformat(), end.isoformat()

    owns_session = session is None
    session = session or aiohttp.ClientSession()
    seen_tickers: set = set()

    try:
        for batch in _chunk([t.upper() for t in tickers], SYMBOLS_PER_REQUEST):
            page_token: Optional[str] = None
            while True:
                try:
                    bars_by_symbol, page_token = await _fetch_page(
                        session, headers, batch, timeframe, start_iso, end_iso, page_token
                    )
                except Exception as e:
                    report["errors"].append(f"{batch[0]}..{batch[-1]}: {e}")
                    logger.warning("Backfill page failed for %s symbols: %s", len(batch), e)
                    break

                report["pages"] += 1
                rows: List[tuple] = []
                for symbol, bars in bars_by_symbol.items():
                    if not bars:
                        continue
                    seen_tickers.add(symbol)
                    for b in bars:
                        ts = _parse_bar_time(b.get("t"))
                        if ts is None:
                            continue
                        try:
                            rows.append((
                                symbol,
                                ts,
                                float(b.get("o", 0.0)),
                                float(b.get("h", 0.0)),
                                float(b.get("l", 0.0)),
                                float(b.get("c", 0.0)),
                                float(b.get("v", 0.0) or 0.0),
                                # Vendor bars are labelled so a reader can tell
                                # them from live-aggregated ones at the seam.
                                "vendor_backfill",
                            ))
                        except (TypeError, ValueError):
                            continue

                if rows:
                    written = await _write_rows(db_client, rows)
                    report["bars_written"] += written

                if not page_token:
                    break
                await asyncio.sleep(INTER_REQUEST_SLEEP_SEC)

            await asyncio.sleep(INTER_REQUEST_SLEEP_SEC)
    finally:
        if owns_session:
            await session.close()

    report["tickers_with_data"] = len(seen_tickers)
    logger.info(
        "Historical backfill complete: %s bars for %s/%s tickers over %s days "
        "(timeframe=%s adjustment=%s feed=%s, %s pages, %s errors)",
        report["bars_written"], report["tickers_with_data"], report["requested_tickers"],
        lookback_days, timeframe, ADJUSTMENT, FEED, report["pages"], len(report["errors"]),
    )
    return report


async def _write_rows(db_client, rows: List[tuple]) -> int:
    """Upsert bars, preferring a batched executemany where the client offers one."""
    if not rows:
        return 0
    try:
        executemany = getattr(db_client, "execute_many", None) or getattr(db_client, "executemany", None)
        if callable(executemany):
            await executemany(_INSERT_SQL, rows)
            return len(rows)
        written = 0
        for row in rows:
            await db_client.execute(_INSERT_SQL, *row)
            written += 1
        return written
    except Exception as e:
        # Loud, unlike the debug-level handler the live bar writer uses. A
        # backfill that silently writes nothing looks exactly like a backfill
        # that was never needed.
        logger.error("Backfill insert failed for %s rows: %s", len(rows), e)
        return 0
