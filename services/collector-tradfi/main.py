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

            if await loop.run_in_executor(None, redis_client.exists, redis_key): 
                continue
                
            await loop.run_in_executor(None, redis_client.set, redis_key, "1", 604800)

            event = RawEvent(
                source="sec_form4", 
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "link": link, 
                    "title": entry.get("title", ""), 
                    "summary": entry.get("summary", "")
                }
            )
            producer.send(Topics.RAW_TRADFI, event.model_dump(), key="form4")
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
    def flush(self):
        now = datetime.now(timezone.utc)

        for ticker, data in self.buffer.items():
            if data["V"] > 0:
                candle = {
                    "ticker": ticker,
                    "trade_type": "OHLCV_MINUTE_BAR",
                    "open": data["O"],
                    "high": data["H"],
                    "low": data["L"],
                    "close": data["C"],
                    "volume": data["V"],
                    "notional_usd": data["C"] * data["V"]
                
                }
                event = RawEvent(
                    source="finnhub_equities",
                    occurred_at=now,
                    raw_payload=candle
                )
                self.producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)

                redis_list_key= f"sentinel:candles:1m:{ticker}"
                candle_json = json.dumps({"ts": now.isoformat(), **candle})
                try:
                    self.redis_client.raw.lpush(redis_list_key, candle_json)
                    # Keep only the last 1440 minutes (24 hours) of candles
                    self.redis_client.raw.ltrim(redis_list_key, 0, 1439)
                except Exception as e:
                    logger.error("Redis error: %s", e, exc_info=True)
        self.buffer.clear()
                    

async def stream_equities(producer: SentinelProducer, redis_client):
    if not FINNHUB_API_KEY: 
        logger.error("FINNHUB_API_KEY missing. Cannot stream equities.")
        return
        
    url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    aggregator = OHLCVAggregator(producer, redis_client)
    
    async def sync_subscriptions(ws):
        """Watches Redis and dynamically subscribes/unsubscribes using Finnhub JSON formats."""
        loop = asyncio.get_event_loop()
        current_subs = set()

        while True:
            try:
                raw_tickers = await loop.run_in_executor(None, redis_client.raw.smembers, REDIS_EQUITIES_KEY)
                desired_subs = {t.upper() for t in raw_tickers} if raw_tickers else {"SPY", "QQQ"}
                
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
            aggregator.flush()

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
                                price = float(item.get("p", 0))
                                volume = float(item.get("v", 0))
                                notional = price * volume
                                
                                # 1. Feed the aggregator for downstream ML analysis
                                aggregator.add_trade(ticker, price, volume)
                                
                                # 2. Instant Block Trade Detection (> $500k)
                                if notional > 100_000:
                                    logger.warning("🐳 EQUITY BLOCK: %s $%.2fM at $%.2f", ticker, notional / 1e6, price)
                                    event = RawEvent(
                                        source="finnhub_equities",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker, "trade_type": "BLOCK_TRADE",
                                            "price": price, "size_shares": volume, "notional_usd": notional
                                        }
                                    )
                                    producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                finally:
                    sync_task.cancel()
                    flush_task.cancel()
        except Exception as e:
            logger.error("Finnhub error: %s. Reconnecting...", e, exc_info=True)
            await asyncio.sleep(5)

# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

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
    redis_client = get_redis()
    logger.info("Starting TradFi Collector (Finnhub & SEC Only)")
    try:
        await asyncio.gather(
            run_polling(producer, redis_client),
            stream_equities(producer, redis_client)
        )
    finally:
        producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())