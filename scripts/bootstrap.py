#!/usr/bin/env python
"""
scripts/bootstrap.py

First-run backfill of genuine historical market data.

A new operator starting the stack sees empty panels until the collectors have
been running long enough to fill them -- the correlation engine needs 120 bars
before it computes anything, and the charts show "AWAITING LIVE DATA STREAM"
until then. That first impression is the platform looking broken.

This fills the gap with *real* history fetched from the same free upstreams the
collectors poll, rather than inventing it. That distinction matters here more
than usual: `scripts/seed_live_pipeline.py` writes seven fabricated events --
an invented tanker, invented anomaly scores -- into the same `events` table
real ingestion writes to, with `source: "collector.ais"`, which makes them
indistinguishable from measurements. This script exists so nobody needs that
one to get a populated dashboard.

    python scripts/bootstrap.py               # default watchlist, 30 days
    python scripts/bootstrap.py --days 90
    python scripts/bootstrap.py --tickers NVDA,AAPL,SPY

Idempotent: bars are upserted on (ticker, time), so re-running extends coverage
rather than duplicating it.
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env", override=False)

# Run from the host, where the compose service names do not resolve.
os.environ.setdefault("POSTGRES_HOST", "127.0.0.1")
os.environ.setdefault("REDIS_HOST", "127.0.0.1")

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils.candles import candle_cache_key  # noqa: E402
from shared.utils.http_client import guarded_get  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("bootstrap")

# Matches the correlation engine's discovery universe, so a first run produces
# the pairs it will actually analyse rather than an unrelated sample.
DEFAULT_TICKERS = [
    "NVDA", "TSM", "AAPL", "MSFT", "AMD", "AVGO", "QQQ", "SPY",
    "GOOGL", "AMZN", "META", "INTC", "XOM", "CVX", "LMT",
]

# The discovery engine needs 120 observations before it computes a correlation;
# 30 daily bars is well short of that but enough to render every chart and give
# the graph something to draw. Raise with --days for a usable correlation run.
DEFAULT_DAYS = 30


async def fetch_daily_bars(ticker: str, days: int) -> List[Dict[str, Any]]:
    """Historical daily OHLCV from Yahoo's public chart API.

    Goes through the guarded client, so a rate-limited or failing upstream
    trips the shared circuit breaker instead of being hammered by a loop over
    fifteen tickers.
    """
    range_ = "3mo" if days <= 90 else ("1y" if days <= 365 else "2y")
    payload = await guarded_get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?range={range_}&interval=1d",
        source="yahoo",
        service="bootstrap",
        timeout=15.0,
    )
    if not payload:
        return []

    try:
        result = (payload.get("chart") or {}).get("result") or []
        if not result:
            return []
        chart = result[0]
        stamps = chart.get("timestamp") or []
        quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]

        opens, highs = quote.get("open") or [], quote.get("high") or []
        lows, closes = quote.get("low") or [], quote.get("close") or []
        volumes = quote.get("volume") or []

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        bars: List[Dict[str, Any]] = []
        for i, ts in enumerate(stamps):
            def at(seq):
                return seq[i] if i < len(seq) else None

            o, h, l, c = at(opens), at(highs), at(lows), at(closes)
            # A gap bar is skipped, never forward-filled: an invented price is
            # exactly what this script exists to avoid.
            if None in (o, h, l, c):
                continue
            when = datetime.fromtimestamp(ts, tz=timezone.utc)
            if when < cutoff:
                continue
            bars.append({
                "ticker": ticker, "time": when,
                "open": float(o), "high": float(h),
                "low": float(l), "close": float(c),
                "volume": float(at(volumes) or 0.0),
            })
        return bars
    except (KeyError, IndexError, TypeError) as e:
        logger.warning("  %-6s unexpected payload shape: %s", ticker, e)
        return []


async def write_bars(db: Any, bars: List[Dict[str, Any]]) -> int:
    """Upserts bars into the tradfi_bars hypertable."""
    if not bars:
        return 0
    written = 0
    for b in bars:
        try:
            await db.execute(
                """
                INSERT INTO tradfi_bars (ticker, time, open, high, low, close, volume, session)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'REGULAR')
                ON CONFLICT (ticker, time) DO UPDATE
                  SET open = EXCLUDED.open, high = EXCLUDED.high,
                      low = EXCLUDED.low, close = EXCLUDED.close,
                      volume = EXCLUDED.volume;
                """,
                b["ticker"], b["time"], b["open"], b["high"],
                b["low"], b["close"], b["volume"],
            )
            written += 1
        except Exception as e:
            logger.warning("  write failed for %s @ %s: %s", b["ticker"], b["time"], e)
    return written


async def warm_candle_cache(redis: Any, ticker: str, bars: List[Dict[str, Any]]) -> None:
    """Populates the Redis candle cache the agents read from.

    Without this the quant engine still finds no history: it reads candles from
    Redis, not from the hypertable, so a database-only backfill leaves it as
    cold as before.
    """
    if not redis or not bars:
        return
    import json

    try:
        key = candle_cache_key(ticker, "1d")
        pipe = redis.raw.pipeline()
        pipe.delete(key)
        for b in bars:
            pipe.rpush(key, json.dumps({
                "timestamp": b["time"].isoformat(),
                "open": b["open"], "high": b["high"],
                "low": b["low"], "close": b["close"], "volume": b["volume"],
            }))
        pipe.expire(key, 7 * 24 * 3600)
        await pipe.execute()
    except Exception as e:
        logger.warning("  cache warm failed for %s: %s", ticker, e)


async def bootstrap(tickers: List[str], days: int) -> int:
    logger.info("")
    logger.info("SENTINEL BOOTSTRAP — backfilling %d days for %d tickers", days, len(tickers))
    logger.info("Source: Yahoo Finance public chart API (real historical bars)")
    logger.info("")

    db = await get_timescale()
    if not db:
        logger.error("TimescaleDB unreachable. Is the stack running? (make analyst)")
        return 1
    try:
        redis = await get_redis()
    except Exception:
        redis = None
        logger.warning("Redis unreachable; database will be filled but the agent cache will not.")

    total_bars = 0
    empty: List[str] = []

    for ticker in tickers:
        bars = await fetch_daily_bars(ticker, days)
        if not bars:
            empty.append(ticker)
            logger.info("  %-6s no data returned", ticker)
            continue
        written = await write_bars(db, bars)
        await warm_candle_cache(redis, ticker, bars)
        total_bars += written
        logger.info("  %-6s %3d bars  %s -> %s",
                    ticker, written,
                    bars[0]["time"].date(), bars[-1]["time"].date())
        # Courtesy pacing. The circuit breaker handles a failing upstream; this
        # keeps a healthy one from seeing fifteen bursts in under a second.
        await asyncio.sleep(0.3)

    logger.info("")
    logger.info("Backfilled %d bars across %d tickers.", total_bars, len(tickers) - len(empty))
    if empty:
        logger.info("No data for: %s", ", ".join(empty))

    if total_bars == 0:
        logger.error("")
        logger.error("Nothing was written. The upstream may be unreachable or rate limiting.")
        logger.error("The dashboard will keep showing AWAITING LIVE DATA until collectors fill it.")
        return 1

    logger.info("")
    logger.info("Charts and the graph will now render. Correlation discovery needs 120")
    logger.info("observations per series -- re-run with --days 180 for a usable first pass.")
    logger.info("")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill real historical market data.")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS,
                    help=f"Days of history to fetch (default {DEFAULT_DAYS}).")
    ap.add_argument("--tickers", type=str, default="",
                    help="Comma-separated tickers. Defaults to the discovery universe.")
    args = ap.parse_args()

    tickers = ([t.strip().upper() for t in args.tickers.split(",") if t.strip()]
               if args.tickers else DEFAULT_TICKERS)
    return asyncio.run(bootstrap(tickers, max(1, args.days)))


if __name__ == "__main__":
    sys.exit(main())
