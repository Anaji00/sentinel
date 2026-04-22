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
            producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key="form4")
    except Exception as e:
        logger.error(f"SEC Form 4 error: {e}")

# ── FINNHUB EQUITIES (WEBSOCKET) ──────────────────────────────────────────────

class MinuteBarAggregator:
    """Aggregates raw Finnhub ticks into 60-second volume bars for ML baseline scoring."""
    def __init__(self, producer: SentinelProducer):
        self.producer = producer
        self.buffer = {}

    def add_trade(self, ticker: str, price: float, volume: float):
        if ticker not in self.buffer:
            self.buffer[ticker] = {"volume": 0, "close_price": price}
        self.buffer[ticker]["volume"] += volume
        self.buffer[ticker]["close_price"] = price # Continually update to the latest price

    def flush(self):
        """Called every 60 seconds to push aggregates to Kafka and clear the buffer."""
        for ticker, data in self.buffer.items():
            if data["volume"] > 0:
                event = RawEvent(
                    source="finnhub_equities",
                    occurred_at=datetime.now(timezone.utc),
                    raw_payload={
                        "ticker": ticker,
                        "trade_type": "VOLUME_MINUTE_BAR",
                        "volume": data["volume"],
                        "price": data["close_price"],
                        "notional_usd": data["volume"] * data["close_price"]
                    }
                )
                self.producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key=ticker)
        self.buffer.clear()

async def stream_equities(producer: SentinelProducer, redis_client):
    if not FINNHUB_API_KEY: 
        logger.error("FINNHUB_API_KEY missing. Cannot stream equities.")
        return
        
    url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    aggregator = MinuteBarAggregator(producer)
    
    async def sync_subscriptions(ws):
        """Watches Redis and dynamically subscribes/unsubscribes using Finnhub JSON formats."""
        loop = asyncio.get_event_loop()
        current_subs = set()
        
        while True:
            raw_tickers = await loop.run_in_executor(None, redis_client.smembers, REDIS_EQUITIES_KEY)
            desired_subs = {t.upper() for t in raw_tickers} if raw_tickers else {"SPY"}
            
            # PROTECT THE FREE TIER: Strictly enforce the 50 symbol limit
            if len(desired_subs) > 50:
                logger.warning(f"Watchlist exceeds Finnhub limit (50). Truncating {len(desired_subs) - 50} symbols.")
                desired_subs = set(list(desired_subs)[:50])
            
            to_add = desired_subs - current_subs
            to_remove = current_subs - desired_subs
            
            for ticker in to_add:
                await ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
            for ticker in to_remove:
                await ws.send(json.dumps({"type": "unsubscribe", "symbol": ticker}))
                
            if to_add or to_remove:
                current_subs = desired_subs
                logger.info(f"Finnhub: Synced subs. Currently tracking {len(current_subs)}/50 limit.")
            
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
                                if notional > 500_000:
                                    logger.warning(f"🐳 EQUITY BLOCK: {ticker} ${notional/1e6:.2f}M at ${price}")
                                    event = RawEvent(
                                        source="finnhub_equities",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker, "trade_type": "BLOCK_TRADE",
                                            "price": price, "size_shares": volume, "notional_usd": notional
                                        }
                                    )
                                    producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key=ticker)
                finally:
                    sync_task.cancel()
                    flush_task.cancel()
        except Exception as e:
            logger.error(f"Finnhub error: {e}. Reconnecting...")
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