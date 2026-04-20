"""
services/collector-prediction/main.py

ENTERPRISE PREDICTION MARKET COLLECTOR
======================================
Ingests Polymarket and Kalshi.
Features: 
- Real-time WebSocket streaming
- In-memory rolling averages to detect unusual bet sizing (Whale tracking)
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import deque

import websockets
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s"
)
logger = logging.getLogger("collector.prediction")

KALSHI_BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"

# ── ANOMALY DETECTION ENGINE ─────────────────────────────────────────────────

class WhaleDetector:
    """
    Maintains a fast, in-memory sliding window of trade sizes per asset.
    Flags trades that significantly deviate from the baseline volume.
    """
    def __init__(self, window_size = 100, multiplier_threshold = 5.0):
        self.history = {}
        self.window_size = window_size
        self.multiplier_threshold = multiplier_threshold
    
    def is_unusual(self, assed_id: str, trade_size_usd: float) -> bool:
        if assed_id not in self.history:
            self.history[assed_id] = deque(maxlen=self.window_size)
        
        history = self.history[assed_id]
        if len(history) < 10:
            history.append(trade_size_usd)
            return False
        
        avg_size = sum(history) / len(history)
        history.append(trade_size_usd)

# If the trade is X times larger than the recent average, it's a whale

        if trade_size_usd > (avg_size * self.threshold) and trade_size_usd > 1000:
            return True
        return False
    
whale_detector = WhaleDetector()

# ── POLYMARKET STREAM ─────────────────────────────────────────────────────────        

async def stream_polymarket(producer: SentinelProducer, redis_client):
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    redis_key = "sentinel:polymarket:watched_slugs"
    
    # Reverse lookup map: {token_id: "human readable label"}
    id_to_label = {}

    async def update_subscriptions(ws, session):
        """Background task: syncs active markets from Redis to the WS connection."""
        base_url = "https://gamma-api.polymarket.com/events/slug/"
        loop = asyncio.get_event_loop()
        
        while True:
            raw_slugs = await loop.run_in_executor(None, redis_client.smembers, redis_key)
            watched_slugs = [s for s in raw_slugs] if raw_slugs else []
            new_assets = []

            for slug in watched_slugs:
                try:
                    async with session.get(f"{base_url}{slug}", timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for market in data.get("markets", []):
                                if market.get("closed"): continue

                                question = market.get("question", "")
                                tokens = market.get("clonTokenIds", [])
                                if isinstance(tokens: str):
                                    tokens = json.loads(tokens)
                                
                                for i, token_id in enumerate(tokens):
                                    if token_id not in id_to_label:
                                        id_to_label[token_id] = f"{slug} | {question} | Outcome {i}"
                                        new_assets.append(token_id)
                except Exception as e:
                    logger.error(f"Gamma API error for {slug}: {e}")
            
            if new_assets:
                await ws.send(json.dumps({"assests": new_assets, "type": "market"}))
                logger.info(f"Polymarket: Subscribed to {len(new_assets)} new outcome tokens.")
            
            await asyncio.sleep(300)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Polymarket CLOB")

                async with aiohttp.ClientSession() as session:
                    injector_task = asyncio.create_task(update_subscriptions(ws, session))

                    try:
                        while True:
                            message = await ws.recv()
                            data = json.loads(message)
                            events = data if isinstance(data, list) else data

                            for event in events:
                                if event.get("event_type") == "trade":
                                    asset_id = event.get("asset_id")
                                    price = float(event.get("price", 0.0))
                                    size = float(event.get("size", 0.0))
                                    notional_usd = price * size

                                    label = id_to_label.get(asset_id, "UNKNOWN")
                                    is_whale = whale_detector.is_unusual(asset_id, notional_usd)

                                    raw_event = RawEvent(
                                        source="polymarket",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "asset_label": label,
                                            "side": event.get("side", ""),
                                            "price": price,
                                            "size_shares": size,
                                            "notional_usd": notional_usd,
                                            "is_whale_bet": is_whale
                                        }
                                    )

                                    if is_whale:
                                        logger.warning(f"🚨 UNUSUAL BET DETECTED: ${notional_usd:.2f} on {label}")
                                    producer.send(Topics.RAW_FINANCIAL, raw_event.model_dump(), key="polymarket")
                    finally:
                        injector_task.cancel()

        except Exception as e:
            logger.error(f"Polymarket WS Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

# ── KALSHI POLLER ─────────────────────────────────────────────────────────────

async def poll_kalshi(producer: SentinelProducer, redis_client):
    """
    Polls Kalshi REST API for active event volumes.
    Kalshi focuses heavily on US legislation, economics, and Fed rates.
    """
    # Note: For production Kalshi, you need mutual TLS (mTLS) or an API key.
    # This assumes a public/unauthenticated discovery tier or basic auth setup.
    session_timeout = aiohttp.ClientTimeout(total=10)
    
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            try:
                # Example: Fetching markets sorted by recent volume
                url = f"{KALSHI_BASE_URL}/markets?status=active&limit=50"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data.get("markets", [])
                        
                        for market in markets:
                            ticker = market.get("ticker")
                            vol = market.get("volume", 0)
                            
                            # Simple delta check using Redis to see if volume spiked
                            redis_key = f"sentinel:kalshi:vol:{ticker}"
                            loop = asyncio.get_event_loop()
                            prev_vol = await loop.run_in_executor(None, redis_client.get, redis_key)
                            
                            if prev_vol:
                                delta = vol - int(prev_vol)
                                if delta > 5000: # Configurable spike threshold
                                    logger.warning(f"📈 KALSHI VOLUME SPIKE: {ticker} (+{delta} contracts)")
                                    
                                    event = RawEvent(
                                        source="kalshi",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker,
                                            "title": market.get("title"),
                                            "volume_delta": delta,
                                            "total_volume": vol,
                                            "yes_bid": market.get("yes_bid"),
                                            "no_bid": market.get("no_bid")
                                        }
                                    )
                                    producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key=ticker)
                            
                            # Update state
                            await loop.run_in_executor(None, redis_client.set, redis_key, vol, 3600)
                            
            except Exception as e:
                logger.error(f"Kalshi polling error: {e}")
            
            await asyncio.sleep(60) # Poll every 60 seconds
# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL PREDICTION MARKET & WHALE COLLECTOR")
    logger.info("=" * 60)

    producer = SentinelProducer()
    redis_client = get_redis()
    try:
        await asyncio.gather(
            stream_polymarket(producer, redis_client)
            poll_kalshi(producer, redis_client)

        )

    except KeyboardInterrupt:
        logger.info("Shutting down prediction collector...")
    finally:
        producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())