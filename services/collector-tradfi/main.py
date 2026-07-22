"""
services/collector-tradfi/main.py

ENTERPRISE TRADFI COLLECTOR (FINNHUB EDITION)
=============================================
Sources: Finnhub (Equities WS), SEC (Form 4 RSS)
Dynamic: Watchlists are driven entirely by Redis (`sentinel:watched:equities`)
Limits: Safely enforces Finnhub's 50-symbol free-tier maximum dynamically.
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
import feedparser
import websockets
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.utils.equities import is_valid_primary_equity

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("collector.tradfi")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
REDIS_EQUITIES_KEY = "sentinel:watched:equities"

# ── SEC FORM 4 (REST POLLING) ─────────────────────────────────────────────────

async def poll_form4(session: aiohttp.ClientSession, producer: SentinelProducer, redis_client):
    """
    Polls the SEC's EDGAR database for Form 4 filings (Insider Trading).
    Tracks when C-suite executives buy or sell their own company's stock.
    """
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=include&count=40&output=atom"
    loop = asyncio.get_event_loop()
    try:
        # SEC requires a descriptive User-Agent
        async with session.get(url, timeout=15, headers={"User-Agent": "SENTINEL/1.0"}) as resp:
            if resp.status != 200: return
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

# ── FINNHUB EQUITIES (WEBSOCKET) ──────────────────────────────────────────────

class OHLCVAggregator:
    """Builds true Open, High, Low, Close, Volume candles and stores them in Redis."""
    def __init__(self, producer: SentinelProducer, redis_client):
        self.producer = producer
        self.redis_client = redis_client
        self.buffer = {}
    
    def add_trade(self, ticker: str, price: float, volume: float):
        if ticker not in self.buffer:
            # First trade of the minute sets the Open, High, Low, and Close
            self.buffer[ticker] = {
                "O": price, "H": price, "L": price, "C": price, "V": volume
            }
        else:
            d = self.buffer[ticker]
            d["H"] = max(d["H"], price)
            d["L"] = min(d["L"], price)
            d["C"] = price
            d["V"] = d["V"] + volume
    async def flush(self):
        now = datetime.now(timezone.utc)
        count = 0

        async with self.redis_client.raw.pipeline() as pipe:
            for ticker, data in self.buffer.items():
                if data["V"] > 0:
                    vwap = round(data["C"] * data["V"] / data["V"], 4) if data["V"] > 0 else data["C"]
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
                        "notional_usd": data["C"] * data["V"]
                    }
                    event = RawEvent(
                        source="finnhub_equities",
                        occurred_at=now,
                        raw_payload=candle
                    )
                    await self.producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                    count += 1
                    try:
                        redis_list_key= f"sentinel:candles:1m:{ticker}"
                        candle_json = json.dumps({"ts": now.isoformat(), **candle})
                        
                        pipe.lpush(redis_list_key, candle_json)
                        # Keep only the last 1440 minutes (24 hours) of candles
                        pipe.ltrim(redis_list_key, 0, 1439)
                    except Exception as e:
                        logger.debug(f"Redis cache pipeline warning for {ticker}: {e}")
            
            try:
                await pipe.execute()
            except Exception as e:
                logger.error(f"Failed to execute Redis pipeline for candles: {e}")
                
        self.buffer.clear()
        if count > 0:
            logger.info(f"Flushed {count} minute bars to Kafka and Redis.")
                    

async def stream_equities(producer: SentinelProducer, redis_client):
    if not FINNHUB_API_KEY: 
        logger.error("FINNHUB_API_KEY missing. Cannot stream equities.")
        return
        
    url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    aggregator = OHLCVAggregator(producer, redis_client)
    
    async def sync_subscriptions(ws):
        """Watches Redis and dynamically subscribes/unsubscribes using Finnhub JSON formats."""
        current_subs = set()

        while True:
            try:
                raw_tickers = await redis_client.raw.zrevrange(REDIS_EQUITIES_KEY, 0, 49)  # Get top 50 tickers by score (timestamp)
                decoded_tickers = {t.decode('utf-8') if isinstance(t, bytes) else t for t in raw_tickers}
                desired_subs = {t.upper() for t in decoded_tickers if is_valid_primary_equity(t)} if decoded_tickers else {"NOW", "INTC"}
                
                # PROTECT THE FREE TIER: Strictly enforce the 50 symbol limit
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
                    logger.info("Finnhub: Synced subs. Currently tracking %d/50 limit.", len(current_subs))
                else:
                    logger.info(f"Finnhub Heartbeat: Monitoring {len(current_subs)} symbols (Awaiting live trades).")
                    # Show exactly which tickers are currently being streamed
                    logger.info("Active Tickers: %s", ", ".join(current_subs))
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
                                
                                # Determine tick direction (Proxy for Buy/Sell Aggressor)
                                prev_price = last_prices.get(ticker)
                                if prev_price is None or price == prev_price:
                                    tick_direction = "ZeroTick"
                                elif price > prev_price:
                                    tick_direction = "UpTick"
                                else:
                                    tick_direction = "DownTick"
                                last_prices[ticker] = price
                                
                                # 1. Feed the aggregator for downstream ML analysis
                                aggregator.add_trade(ticker, price, volume)
                                
                                # 2. Instant Block Trade Detection (> $500k)
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

# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def poll_options(producer: SentinelProducer, redis_client):
    """
    Polls Alpaca's options snapshot API for the watched symbols.
    Filters for large transactions and pipes raw events to Kafka.
    """
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_API_SECRET")
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
            try:
                # Fetch watched symbols from Redis
                raw_symbols = await redis_client.raw.zrange("sentinel:watched:equities", 0, -1)
                raw_symbols = [s.decode() if isinstance(s, bytes) else s for s in raw_symbols] if raw_symbols else []
                # Filter out crypto pairs (Alpaca options API only accepts US equity tickers)
                symbols = [
                    s.upper().strip() for s in raw_symbols 
                    if s and not (s.endswith("USDT") or s.endswith("USD") or "-" in s or "_" in s)
                ]
                
                # Fallback list of major US equity tickers
                if not symbols:
                    symbols = ["AAPL", "TSLA", "MSFT", "NVDA", "AMZN"]
                
                logger.info(f"Options Poller: Fetching options chains for {len(symbols)} equity tickers ({symbols[:5]})...")
                
                for ticker in symbols[:50]:  # Cap to top 50 to honor API rates
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
                                premium = price * size * 100.0  # standard options contract multiplier
                                
                                # Sweep/large options block filter: premium >= $50,000 or contract size >= 100
                                if premium >= 50000.0 or size >= 100.0:
                                    event = RawEvent(
                                        source="alpaca_options",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker,
                                            "option_symbol": contract,
                                            "price": price,
                                            "volume": size,
                                            "premium_usd": premium
                                        }
                                    )
                                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                                    count += 1
                            
                            if count > 0:
                                logger.info(f"Published {count} options flow events for {ticker} to Kafka.")
                        else:
                            text = await resp.text()
                            logger.error(f"Alpaca API options snapshots for {ticker} returned {resp.status}: {text}")
                            
            except Exception as e:
                logger.error(f"Error in Options flow collector: {e}", exc_info=True)
                
            await asyncio.sleep(300)

async def run_polling(producer: SentinelProducer, redis_client):
    connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            # Removed Unusual Whales, only polling SEC EDGAR now.
            await asyncio.gather(
                poll_form4(session, producer, redis_client),
            )
            await asyncio.sleep(60)

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL TradFi Service")
    logger.info("=" * 60)
    producer = SentinelProducer()
    await producer.start()
    redis_client = await get_redis()
    logger.info("Starting TradFi Collector (Finnhub, SEC & Alpaca Options)")
    try:
        await asyncio.gather(
            run_polling(producer, redis_client),
            stream_equities(producer, redis_client),
            poll_options(producer, redis_client)
        )
    finally:
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())