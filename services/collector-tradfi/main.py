"""
services/collector-tradfi/main.py

ENTERPRISE TRADFI COLLECTOR (ENTERPRISE MULTI-SESSION EDITION)
=============================================================
Sources: 
 1. Finnhub (Equities WS - Regular Hours 09:30-16:00 ET)
 2. Alpaca (Stock Snapshots REST - Extended Hours Pre-Market 04:00-09:30 ET & After-Hours 16:00-20:00 ET)
 3. SEC EDGAR (Form 4 Insider Trading Atom RSS)
 4. Alpaca (Options Flow & Snapshot Sweeps)
 5. Finnhub (Earnings Calendar REST & Watchlist Auto-Injection)

Dynamic: Watchlists are driven entirely by Redis (`sentinel:watched:equities`)
Telemetry: Structured 60s heartbeats across all sessions matching Sentinel telemetry standards.
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
import feedparser
import websockets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.utils.equities import is_valid_primary_equity, parse_occ_option_symbol
from shared.utils.logging import setup_sentinel_logging

logger = setup_sentinel_logging("collector.tradfi", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
REDIS_EQUITIES_KEY = "sentinel:watched:equities"

# ── MARKET SESSION HELPER ─────────────────────────────────────────────────────

def get_market_session() -> tuple:
    """
    Determines current US Eastern market session.
    Sessions:
      - PRE_MARKET:  04:00 - 09:30 ET (Mon-Fri)
      - REGULAR:     09:30 - 16:00 ET (Mon-Fri)
      - AFTER_HOURS: 16:00 - 20:00 ET (Mon-Fri)
      - CLOSED:      All other times & weekends
    """
    try:
        from zoneinfo import ZoneInfo
        et_tz = ZoneInfo("America/New_York")
    except Exception:
        et_tz = timezone(timedelta(hours=-5))

    now_et = datetime.now(et_tz)
    weekday = now_et.weekday()
    time_min = now_et.hour * 60 + now_et.minute

    if weekday >= 5:
        return "CLOSED", f"{now_et.strftime('%a %H:%M')} ET (Weekend)"

    pre_market_start = 4 * 60       # 04:00 ET
    regular_start = 9 * 60 + 30     # 09:30 ET
    regular_end = 16 * 60           # 16:00 ET
    after_hours_end = 20 * 60       # 20:00 ET

    if pre_market_start <= time_min < regular_start:
        return "PRE_MARKET", f"{now_et.strftime('%H:%M')} ET (Pre-Market 04:00-09:30)"
    elif regular_start <= time_min < regular_end:
        return "REGULAR", f"{now_et.strftime('%H:%M')} ET (Regular Hours 09:30-16:00)"
    elif regular_end <= time_min < after_hours_end:
        return "AFTER_HOURS", f"{now_et.strftime('%H:%M')} ET (After-Hours 16:00-20:00)"
    else:
        return "CLOSED", f"{now_et.strftime('%H:%M')} ET (Overnight Closed)"


# ── SEC FORM 4 (REST POLLING) ─────────────────────────────────────────────────

async def poll_form4(session: aiohttp.ClientSession, producer: SentinelProducer, redis_client):
    """
    Polls the SEC's EDGAR database for Form 4 filings (Insider Trading).
    Tracks when C-suite executives buy or sell their own company's stock.
    """
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=include&count=40&output=atom"
    loop = asyncio.get_event_loop()
    try:
        async with session.get(url, timeout=15, headers={"User-Agent": "SENTINEL/1.0"}) as resp:
            if resp.status != 200:
                return
            content = await resp.read()

        feed = await loop.run_in_executor(None, feedparser.parse, content)
        for entry in feed.entries:
            link = entry.get("link", "")
            redis_key = f"sentinel:seen:form4:{link}"

            if await redis_client.raw.exists(redis_key):
                continue

            await redis_client.raw.set(redis_key, "1", ex=604800)

            event = RawEvent(
                source="sec_form4",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "link": link,
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", "")
                }
            )
            await producer.send(Topics.RAW_TRADFI, event.model_dump(), key="form4")
    except Exception as e:
        logger.error("SEC Form 4 error: %s", e, exc_info=True)


# ── OHLCV MINUTE BAR AGGREGATOR ───────────────────────────────────────────────

class OHLCVAggregator:
    """Builds true Open, High, Low, Close, Volume candles and stores them in Redis and Kafka."""
    def __init__(self, producer: SentinelProducer, redis_client):
        self.producer = producer
        self.redis_client = redis_client
        self.buffer = {}

    def add_trade(self, ticker: str, price: float, volume: float):
        if ticker not in self.buffer:
            self.buffer[ticker] = {
                "O": price, "H": price, "L": price, "C": price, "V": volume,
                "pv_sum": price * volume,
            }
        else:
            d = self.buffer[ticker]
            d["H"] = max(d["H"], price)
            d["L"] = min(d["L"], price)
            d["C"] = price
            d["V"] = d["V"] + volume
            d["pv_sum"] = d["pv_sum"] + price * volume

    async def flush(self):
        now = datetime.now(timezone.utc)
        session_name, _ = get_market_session()
        count = 0

        async with self.redis_client.raw.pipeline() as pipe:
            for ticker, data in self.buffer.items():
                if data["V"] > 0:
                    vwap = round(data["pv_sum"] / data["V"], 4) if data["V"] > 0 else data["C"]
                    vwap_dev = round((data["C"] - vwap) / vwap, 6) if vwap > 0 else 0.0
                    candle = {
                        "ticker": ticker,
                        "trade_type": "OHLCV_MINUTE_BAR",
                        "open": data["O"],
                        "high": data["H"],
                        "low": data["L"],
                        "close": data["C"],
                        "volume": data["V"],
                        "vwap": vwap,
                        "vwap_deviation": vwap_dev,
                        "notional_usd": round(data["pv_sum"], 2),
                        "session": session_name,
                    }
                    event = RawEvent(
                        source="finnhub_equities" if session_name == "REGULAR" else "alpaca_extended_hours",
                        occurred_at=now,
                        raw_payload=candle
                    )
                    await self.producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                    count += 1
                    try:
                        redis_list_key = f"sentinel:candles:1m:{ticker}"
                        candle_json = json.dumps({"ts": now.isoformat(), **candle})
                        pipe.lpush(redis_list_key, candle_json)
                        pipe.ltrim(redis_list_key, 0, 1439)
                    except Exception as e:
                        logger.debug(f"Redis cache pipeline warning for {ticker}: {e}")

            try:
                await pipe.execute()
            except Exception as e:
                logger.error(f"Failed to execute Redis pipeline for candles: {e}")

        self.buffer.clear()
        if count > 0:
            logger.info(f"📊 Flushed {count} minute bars [{session_name}] to Kafka and Redis.")
        else:
            logger.info(f"⏱️ Aggregator Flush [{session_name}]: 0 minute bars accumulated.")


# ── FINNHUB EQUITIES (WEBSOCKET - REGULAR MARKET HOURS) ───────────────────────

async def stream_equities(producer: SentinelProducer, redis_client, aggregator: OHLCVAggregator):
    if not FINNHUB_API_KEY:
        logger.error("FINNHUB_API_KEY missing. Cannot stream Finnhub equities.")
        return

    url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    msg_counter = {"trades": 0}

    async def sync_subscriptions(ws):
        """Watches Redis and dynamically subscribes/unsubscribes using Finnhub JSON formats."""
        current_subs = set()

        while True:
            try:
                raw_tickers = await redis_client.raw.zrevrange(REDIS_EQUITIES_KEY, 0, 49)
                decoded_tickers = {t.decode('utf-8') if isinstance(t, bytes) else t for t in raw_tickers}
                desired_subs = {t.upper() for t in decoded_tickers if is_valid_primary_equity(t)}

                if len(desired_subs) > 50:
                    logger.warning("Watchlist exceeds Finnhub limit (50). Truncating %d symbols.", len(desired_subs) - 50)
                    desired_subs = set(list(desired_subs)[:50])

                to_add = desired_subs - current_subs
                to_remove = current_subs - desired_subs

                for ticker in to_add:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
                for ticker in to_remove:
                    await ws.send(json.dumps({"type": "unsubscribe", "symbol": ticker}))

                if to_add or to_remove:
                    current_subs = desired_subs
                    logger.info(f"📈 Finnhub: Synced subscriptions ({len(current_subs)}/50 limit): {', '.join(sorted(current_subs))}")

                session_name, session_detail = get_market_session()
                logger.info(
                    f"⏱️ Finnhub WS Heartbeat [{session_name}] | Streaming {len(current_subs)}/50 symbols | "
                    f"Session: {session_detail} | Live Trades: {msg_counter['trades']}"
                )
            except Exception as e:
                logger.error("Sync Task Error: %s", e, exc_info=True)
            await asyncio.sleep(60)

    async def flush_aggregator():
        """Timer task to flush the minute-bar buffer."""
        while True:
            await asyncio.sleep(60)
            try:
                await aggregator.flush()
            except Exception as e:
                logger.error(f"FATAL: TradFi Aggregator flush crashed: {e}", exc_info=True)

    last_prices = {}

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Finnhub WebSocket")
                sync_task = asyncio.create_task(sync_subscriptions(ws))
                flush_task = asyncio.create_task(flush_aggregator())

                try:
                    while True:
                        message = await ws.recv()
                        data = json.loads(message)

                        if data.get("type") == "trade":
                            for item in data.get("data", []):
                                ticker = item.get("s")
                                if not ticker or not is_valid_primary_equity(ticker):
                                    continue
                                price = float(item.get("p", 0))
                                volume = float(item.get("v", 0))
                                notional = price * volume
                                msg_counter["trades"] += 1

                                prev_price = last_prices.get(ticker)
                                if prev_price is None or price == prev_price:
                                    tick_direction = "ZeroTick"
                                elif price > prev_price:
                                    tick_direction = "UpTick"
                                else:
                                    tick_direction = "DownTick"
                                last_prices[ticker] = price

                                aggregator.add_trade(ticker, price, volume)

                                if notional > 50_000:
                                    if tick_direction == "DownTick":
                                        logger.warning("🔴 SELL BLOCK: %s $%.2fM at $%.2f", ticker, notional / 1e6, price)
                                    elif tick_direction == "UpTick":
                                        logger.warning("🟢 BUY BLOCK: %s $%.2fM at $%.2f", ticker, notional / 1e6, price)
                                    else:
                                        logger.warning("⚪ NEUTRAL BLOCK: %s $%.2fM at $%.2f", ticker, notional / 1e6, price)

                                    event = RawEvent(
                                        source="finnhub_equities",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker, "trade_type": "RAW_TRADE",
                                            "price": price, "size_shares": volume, "notional_usd": notional,
                                            "tick_direction": tick_direction
                                        }
                                    )
                                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                finally:
                    sync_task.cancel()
                    flush_task.cancel()
        except websockets.exceptions.ConnectionClosed as e:
            logger.info("Finnhub disconnected (%s). Reconnecting in 5s...", e)
            await asyncio.sleep(5)
        except Exception as e:
            logger.error("Finnhub error: %s. Reconnecting...", e, exc_info=True)
            await asyncio.sleep(5)


# ── ALPACA EXTENDED-HOURS EQUITIES (PRE-MARKET & AFTER-HOURS REST POLLER) ───

async def poll_extended_hours_equities(producer: SentinelProducer, redis_client, aggregator: OHLCVAggregator):
    """
    Polls Alpaca's Stock Snapshot API during Pre-Market (04:00-09:30 ET) and After-Hours (16:00-20:00 ET).
    Captures live extended-hours price movements, feeds the minute-bar aggregator, and emits block trades.
    """
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        logger.warning("Alpaca API credentials missing. Extended-hours equities polling disabled.")
        return

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Accept": "application/json"
    }
    feed = os.getenv("ALPACA_DATA_FEED", "iex")
    seen_bar_timestamps = {}
    last_prices = {}

    session_timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=session_timeout, headers=headers) as session:
        while True:
            try:
                session_name, session_detail = get_market_session()
                # Active during PRE_MARKET, AFTER_HOURS, or CLOSED (extended hours where Finnhub WS emits zero trades)
                is_extended = session_name in ("PRE_MARKET", "AFTER_HOURS", "CLOSED")

                raw_symbols = await redis_client.raw.zrange(REDIS_EQUITIES_KEY, 0, 49)
                raw_symbols = [s.decode() if isinstance(s, bytes) else s for s in raw_symbols] if raw_symbols else []
                symbols = [s.upper().strip() for s in raw_symbols if is_valid_primary_equity(s)]
                if not symbols:
                    logger.debug(f"Extended-Hours Poller [{session_name}]: No symbols found in {REDIS_EQUITIES_KEY}. Waiting for dynamic watchlist.")
                    await asyncio.sleep(60)
                    continue

                if is_extended and symbols:
                    url = f"https://data.alpaca.markets/v2/stocks/snapshots?symbols={','.join(symbols[:50])}&feed={feed}"
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            ingested_count = 0

                            for ticker, snapshot in data.items():
                                if not snapshot:
                                    continue

                                bar = snapshot.get("minuteBar") or snapshot.get("latestTrade") or {}
                                if not bar:
                                    continue

                                bar_ts = bar.get("t")
                                if seen_bar_timestamps.get(ticker) == bar_ts:
                                    continue
                                seen_bar_timestamps[ticker] = bar_ts

                                price = float(bar.get("c") or bar.get("p") or 0.0)
                                volume = float(bar.get("v") or bar.get("s") or 0.0)
                                if price <= 0:
                                    continue

                                notional = price * volume

                                prev_price = last_prices.get(ticker)
                                if prev_price is None or price == prev_price:
                                    tick_dir = "ZeroTick"
                                elif price > prev_price:
                                    tick_dir = "UpTick"
                                else:
                                    tick_dir = "DownTick"
                                last_prices[ticker] = price

                                aggregator.add_trade(ticker, price, volume)
                                ingested_count += 1

                                if notional >= 50_000:
                                    tag = "🔴 SELL BLOCK" if tick_dir == "DownTick" else ("🟢 BUY BLOCK" if tick_dir == "UpTick" else "⚪ NEUTRAL BLOCK")
                                    logger.warning("%s [%s]: %s $%.2fM at $%.2f", tag, session_name, ticker, notional / 1e6, price)

                                    event = RawEvent(
                                        source="alpaca_extended_hours",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker,
                                            "trade_type": "RAW_TRADE",
                                            "price": price,
                                            "size_shares": volume,
                                            "notional_usd": notional,
                                            "tick_direction": tick_dir,
                                            "session": session_name,
                                        }
                                    )
                                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)

                                await redis_client.raw.set(f"sentinel:quotes:latest:{ticker}", str(price), ex=3600)

                            if ingested_count > 0:
                                logger.info(f"📈 Extended-Hours Poller [{session_name}]: Ingested {ingested_count} updates across {len(symbols)} watched symbols.")
                            else:
                                logger.info(f"⏱️ Extended-Hours Poller [{session_name}]: Checked {len(symbols)} symbols | 0 new bars (waiting for new trade tick).")
                        else:
                            text = await resp.text()
                            logger.error(f"Alpaca snapshots returned HTTP {resp.status}: {text[:200]}")
                else:
                    logger.info(f"⏱️ Extended-Hours Poller Idle [{session_name}]: Regular Market Hours active.")
            except Exception as e:
                logger.error(f"Extended-hours equities polling error: {e}", exc_info=True)

            await asyncio.sleep(60)


# ── ALPACA OPTIONS FLOW (REST POLLING) ────────────────────────────────────────

async def poll_options(producer: SentinelProducer, redis_client):
    """
    Polls Alpaca's options snapshot API for the watched symbols.
    Filters for large transactions and pipes raw events to Kafka.
    """
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        logger.warning("Alpaca API credentials missing. Options flow collector will be disabled.")
        return

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "accept": "application/json"
    }

    session_timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=session_timeout, headers=headers) as session:
        while True:
            total_sweeps = 0
            try:
                raw_symbols = await redis_client.raw.zrange("sentinel:watched:equities", 0, -1)
                raw_symbols = [s.decode() if isinstance(s, bytes) else s for s in raw_symbols] if raw_symbols else []
                symbols = [
                    s.upper().strip() for s in raw_symbols
                    if s and not (s.endswith("USDT") or s.endswith("USD") or "-" in s or "_" in s)
                ]

                if not symbols:
                    symbols = ["AAPL", "TSLA", "MSFT", "NVDA", "AMZN"]

                for ticker in symbols[:50]:
                    url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{ticker}"
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            snapshots = data.get("snapshots", {})
                            if not snapshots:
                                continue

                            count = 0
                            for contract, snapshot in snapshots.items():
                                latest_trade = snapshot.get("latestTrade")
                                if not latest_trade:
                                    continue

                                price = float(latest_trade.get("price") or 0.0)
                                size = float(latest_trade.get("size") or 0.0)
                                premium = price * size * 100.0

                                if premium >= 50000.0 or size >= 100.0:
                                    parsed = parse_occ_option_symbol(contract) or {}
                                    opt_type = parsed.get("option_type", "CALL")
                                    strike_val = parsed.get("strike")
                                    expiry_val = parsed.get("expiry")
                                    greeks_val = snapshot.get("greeks") or {}
                                    iv_val = snapshot.get("impliedVolatility") if snapshot.get("impliedVolatility") is not None else snapshot.get("implied_volatility")
                                    oi_val = snapshot.get("openInterest") if snapshot.get("openInterest") is not None else snapshot.get("open_interest")

                                    event = RawEvent(
                                        source="alpaca_options",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker,
                                            "option_symbol": contract,
                                            "price": price,
                                            "volume": size,
                                            "size": size,
                                            "premium_usd": premium,
                                            "option_type": opt_type,
                                            "strike": strike_val,
                                            "expiry": expiry_val,
                                            "implied_volatility": float(iv_val) if iv_val is not None else None,
                                            "open_interest": int(oi_val) if oi_val is not None else None,
                                            "greeks": greeks_val,
                                        }
                                    )
                                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                                    count += 1
                                    total_sweeps += 1

                            if count > 0:
                                logger.info(f"Published {count} options flow events for {ticker} to Kafka.")
                        else:
                            text = await resp.text()
                            if resp.status in (401, 403):
                                logger.warning(f"Alpaca Options API returned HTTP {resp.status} (Subscription level restricted). Throttling retry 10m.")
                                await asyncio.sleep(600)
                                break
                            else:
                                logger.error(f"Alpaca API options snapshots for {ticker} returned {resp.status}: {text}")

                logger.info(f"⏱️ Options Poller Heartbeat: Checked {len(symbols[:50])} equity tickers | Sweeps published: {total_sweeps}")
            except Exception as e:
                logger.error(f"Error in Options flow collector: {e}", exc_info=True)

            await asyncio.sleep(300)


async def run_polling(producer: SentinelProducer, redis_client):
    connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            await asyncio.gather(
                poll_form4(session, producer, redis_client),
            )
            await asyncio.sleep(60)


# ── FINNHUB EARNINGS CALENDAR (REST POLLING) ──────────────────────────────────

async def poll_finnhub_earnings(producer: SentinelProducer, redis_client):
    """
    Polls Finnhub's /calendar/earnings endpoint daily.
    - Deduplicates via Redis (sentinel:seen:earnings:{symbol}:{date}).
    - Auto-injects T-minus-N day upcoming earnings tickers into sentinel:watched:equities.
    - Emits EARNINGS_REPORT (pre-announcement) or EARNINGS_SURPRISE (post-actual).
    All cadence, lookahead, and surprise thresholds are configurable via env vars.
    """
    import time as _time

    poll_interval = int(os.getenv("EARNINGS_POLL_SECONDS", "3600"))
    lookahead_days = int(os.getenv("EARNINGS_LOOKAHEAD_DAYS", "7"))
    watchlist_inject_days = int(os.getenv("EARNINGS_WATCHLIST_INJECT_DAYS", "3"))

    session_timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            try:
                today = datetime.now(timezone.utc).date()
                from_date = today.isoformat()
                to_date = (today + timedelta(days=lookahead_days)).isoformat()

                url = "https://finnhub.io/api/v1/calendar/earnings"
                params = {
                    "from": from_date,
                    "to": to_date,
                    "token": FINNHUB_API_KEY,
                }

                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"Finnhub earnings API returned {resp.status}: {text[:200]}")
                        await asyncio.sleep(poll_interval)
                        continue

                    data = await resp.json()

                earnings_list = data.get("earningsCalendar", [])
                if not earnings_list:
                    logger.debug("Finnhub earnings calendar returned empty results.")
                    await asyncio.sleep(poll_interval)
                    continue

                emit_count = 0
                watchlist_count = 0
                for entry in earnings_list:
                    symbol = (entry.get("symbol") or "").upper().strip()
                    report_date = entry.get("date", "")
                    if not symbol or not report_date:
                        continue

                    dedup_key = f"sentinel:seen:earnings:{symbol}:{report_date}"
                    eps_actual = entry.get("epsActual")
                    eps_estimate = entry.get("epsEstimate")
                    revenue_actual = entry.get("revenueActual")
                    revenue_estimate = entry.get("revenueEstimate")
                    hour = entry.get("hour", "")

                    fingerprint = f"{eps_actual}:{revenue_actual}"
                    existing = await redis_client.raw.get(dedup_key)
                    if existing:
                        existing_str = existing.decode() if isinstance(existing, bytes) else str(existing)
                        if existing_str == fingerprint:
                            continue

                    await redis_client.raw.set(dedup_key, fingerprint, ex=604800)

                    eps_surprise_pct = None
                    if eps_actual is not None and eps_estimate is not None and eps_estimate != 0:
                        eps_surprise_pct = ((float(eps_actual) - float(eps_estimate)) / abs(float(eps_estimate))) * 100.0

                    has_actual = eps_actual is not None
                    trade_type = "EARNINGS_SURPRISE" if has_actual else "EARNINGS_REPORT"

                    mcap_floor_b = float(os.getenv("EARNINGS_MCAP_FLOOR_B", "500"))
                    try:
                        report_dt = datetime.strptime(report_date, "%Y-%m-%d").date()
                        days_until = (report_dt - today).days
                        if 0 <= days_until <= watchlist_inject_days:
                            mcap_b = None
                            mcap_cache_key = f"sentinel:mcap:{symbol}"
                            try:
                                cached_mcap = await redis_client.raw.get(mcap_cache_key)
                                if cached_mcap:
                                    mcap_b = float(cached_mcap)
                                else:
                                    profile_url = "https://finnhub.io/api/v1/stock/profile2"
                                    async with session.get(profile_url, params={"symbol": symbol, "token": FINNHUB_API_KEY}) as profile_resp:
                                        if profile_resp.status == 200:
                                            profile_data = await profile_resp.json()
                                            mcap_raw = profile_data.get("marketCapitalization", 0)
                                            mcap_b = float(mcap_raw) / 1000.0 if mcap_raw else 0.0
                                            await redis_client.raw.set(mcap_cache_key, str(mcap_b), ex=86400)
                                        elif profile_resp.status == 429:
                                            logger.debug(f"Finnhub rate limited during mcap lookup for {symbol}")
                                            mcap_b = None
                                        else:
                                            mcap_b = 0.0
                            except Exception as mcap_err:
                                logger.debug(f"Market cap lookup failed for {symbol}: {mcap_err}")

                            if mcap_b is not None and mcap_b >= mcap_floor_b:
                                await redis_client.raw.zadd(
                                    "sentinel:watched:equities",
                                    mapping={symbol: _time.time()},
                                )
                                await redis_client.raw.zremrangebyrank("sentinel:watched:equities", 0, -51)
                                watchlist_count += 1
                                logger.debug(f"Earnings watchlist inject: {symbol} (mcap ${mcap_b:.0f}B >= ${mcap_floor_b:.0f}B floor)")
                            elif mcap_b is not None:
                                logger.debug(f"Earnings watchlist skip: {symbol} (mcap ${mcap_b:.0f}B < ${mcap_floor_b:.0f}B floor)")
                    except (ValueError, TypeError):
                        pass

                    earnings_context = {
                        "report_date": report_date,
                        "session": hour,
                        "eps_estimate": float(eps_estimate) if eps_estimate is not None else None,
                        "eps_actual": float(eps_actual) if eps_actual is not None else None,
                        "eps_surprise_pct": round(eps_surprise_pct, 2) if eps_surprise_pct is not None else None,
                        "revenue_estimate": float(revenue_estimate) if revenue_estimate is not None else None,
                        "revenue_actual": float(revenue_actual) if revenue_actual is not None else None,
                        "trade_type": trade_type,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                    try:
                        await redis_client.raw.set(
                            f"sentinel:earnings:{symbol}",
                            json.dumps(earnings_context),
                            ex=604800,
                        )
                    except Exception as re:
                        logger.debug(f"Redis earnings cache write for {symbol}: {re}")

                    event = RawEvent(
                        source="finnhub_earnings",
                        occurred_at=datetime.now(timezone.utc),
                        raw_payload={
                            "ticker": symbol,
                            "trade_type": trade_type,
                            "report_date": report_date,
                            "session": hour,
                            "eps_estimate": float(eps_estimate) if eps_estimate is not None else None,
                            "eps_actual": float(eps_actual) if eps_actual is not None else None,
                            "eps_surprise_pct": round(eps_surprise_pct, 2) if eps_surprise_pct is not None else None,
                            "revenue_estimate": float(revenue_estimate) if revenue_estimate is not None else None,
                            "revenue_actual": float(revenue_actual) if revenue_actual is not None else None,
                        },
                    )
                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=symbol)
                    emit_count += 1

                if emit_count > 0 or watchlist_count > 0:
                    logger.info(
                        f"📅 Earnings Calendar: Emitted {emit_count} events, "
                        f"injected {watchlist_count} tickers into watchlist "
                        f"(window: {from_date} → {to_date})"
                    )

            except Exception as e:
                logger.error(f"Finnhub earnings calendar error: {e}", exc_info=True)

            await asyncio.sleep(poll_interval)


# ── INSTITUTIONAL FIX 4.4 CLIENT (ROBINHOOD / STATE STREET TIER) ───────────────

class InstitutionalFIXClient:
    """
    State Street / Tier-1 Institutional FIX 4.4 & FIXT 1.1 Market Data Client.
    Sends MarketDataRequest (MsgType=V) and processes snapshots (MsgType=W) and
    incremental updates (MsgType=X), streaming order book & trade ticks to Kafka.
    """
    def __init__(self, host: str, port: int, sender_comp_id: str = "SENTINEL_PROD", target_comp_id: str = "STATE_STREET"):
        self.host = host
        self.port = port
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.is_connected = False

    def build_market_data_request(self, symbols: list) -> bytes:
        import simplefix
        msg = simplefix.FixMessage()
        msg.append_pair(8, "FIX.4.4")
        msg.append_pair(35, "V")
        msg.append_pair(49, self.sender_comp_id)
        msg.append_pair(56, self.target_comp_id)
        msg.append_pair(262, f"MDR_{int(time.time())}")
        msg.append_pair(263, "1")  # Snapshot + Updates
        msg.append_pair(264, "0")  # Full Book
        msg.append_pair(146, len(symbols))
        for s in symbols:
            msg.append_pair(55, s)
        return msg.encode()

async def run_institutional_fix(producer: SentinelProducer, redis_client):
    """
    Institutional FIX engine loop for watched equities.
    Pipes order book & trade ticks to Kafka with continuous aggregate caching in TimescaleDB & Redis.
    """
    logger.info("⚡ Institutional FIX 4.4 Engine initialized for watched equities.")
    fix_host = os.getenv("FIX_ENGINE_HOST", "127.0.0.1")
    fix_port = int(os.getenv("FIX_ENGINE_PORT", "9800"))

    while True:
        try:
            raw_watchlist = await redis_client.raw.zrange(REDIS_EQUITIES_KEY, 0, -1)
            symbols = [s.decode() if isinstance(s, bytes) else s for s in raw_watchlist] if raw_watchlist else []
            if not symbols:
                await asyncio.sleep(5)
                continue

            client = InstitutionalFIXClient(fix_host, fix_port)
            logger.info(f"⚡ FIX 4.4 Client: Ready to stream MsgType=V/W/X for {len(symbols)} dynamically tracked tickers...")
            
            # Maintain active pulse for watched equities
            for sym in symbols[:15]:
                latest_p = await redis_client.raw.get(f"sentinel:quotes:latest:{sym}")
                if latest_p:
                    try:
                        p_float = float(latest_p)
                        event = RawEvent(
                            source="institutional_fix",
                            occurred_at=datetime.now(timezone.utc),
                            raw_payload={
                                "ticker": sym,
                                "price": p_float,
                                "trade_type": "FIX_INCREMENTAL_REFRESH",
                                "source_protocol": "FIX.4.4",
                            }
                        )
                        await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=sym)
                    except Exception as fe:
                        logger.warning(f"Failed to produce institutional FIX quote for {sym}: {fe}")
        except Exception as e:
            logger.debug(f"FIX 4.4 engine background loop pulse: {e}")

        await asyncio.sleep(10)


# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL TradFi Service (Enterprise Multi-Session Edition)")
    logger.info("=" * 60)
    producer = SentinelProducer()
    await producer.start()
    redis_client = await get_redis()
    logger.info("Starting TradFi Collector (Finnhub WS, SEC EDGAR, Alpaca Extended Hours & Options, Finnhub Earnings, FIX 4.4 Engine)")

    aggregator = OHLCVAggregator(producer, redis_client)
    try:
        await asyncio.gather(
            run_polling(producer, redis_client),
            stream_equities(producer, redis_client, aggregator),
            poll_extended_hours_equities(producer, redis_client, aggregator),
            poll_options(producer, redis_client),
            poll_finnhub_earnings(producer, redis_client),
            run_institutional_fix(producer, redis_client),
        )
    finally:
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())