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
from collections import deque  # 'deque' is a double-ended queue. Think of it as a list with a maximum size that automatically pushes old items out when new ones come in.

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

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

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
    
    # FIX: Corrected typo 'assed_id' to 'asset_id' for readability
    def is_unusual(self, asset_id: str, trade_size_usd: float) -> bool:
        # 1. INITIALIZATION: If we haven't seen this asset before, create a new 'conveyor belt' (deque) for it.
        if asset_id not in self.history:
            # maxlen automatically drops the oldest trade once we hit the window_size (e.g., 100 trades).
            self.history[asset_id] = deque(maxlen=self.window_size)
        
        history = self.history[asset_id]
        
        # 2. WARM-UP PERIOD: We need a minimum number of trades to establish a meaningful "average".
        # If we have less than 10 trades, we just record the trade and assume it's normal to prevent false alarms.
        if len(history) < 10:
            history.append(trade_size_usd)
            return False
        
        # 3. CALCULATE BASELINE: Calculate the average size of the recent trades in our window.
        avg_size = sum(history) / len(history)
        
        # NOTE: We add the new trade to our history AFTER calculating the average!
        # If we added it before, a massive $1,000,000 trade would instantly inflate the average and hide itself.
        history.append(trade_size_usd)

        # ── WHALE LOGIC ─────────────────────────────────────────────────────────
        # If the trade is X times larger than the recent average, and greater than $1,000, it's considered a whale.

        # FIX: Changed 'self.threshold' to 'self.multiplier_threshold' to match __init__
        if trade_size_usd > (avg_size * self.multiplier_threshold) and trade_size_usd > 1000:
            return True
        return False
    
whale_detector = WhaleDetector()

# ── POLYMARKET STREAM ─────────────────────────────────────────────────────────        

async def stream_polymarket(producer: SentinelProducer, redis_client):
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    redis_key = "sentinel:polymarket:watched_slugs"
    
    # Polymarket WebSockets only send us meaningless ID numbers like "123456".
    # We use this dictionary to map those numbers back to human-readable text like "Will Trump win? | Yes".
    # Reverse lookup map: {token_id: "human readable label"}
    id_to_label = {}

    async def update_subscriptions(ws, session):
        """Background task: syncs active markets from Redis to the WS connection."""
        base_url = "https://gamma-api.polymarket.com/markets"
        loop = asyncio.get_event_loop()
        
        while True:
            try:
                raw_slugs = await loop.run_in_executor(None, redis_client.raw.smembers, redis_key)
                watched_slugs = [s.decode() if isinstance(s, bytes) else s for s in raw_slugs] if raw_slugs else [
                    "us-x-iran-permanent-peace-deal-by"
                ]
                new_assets = []

                if watched_slugs:
                    logger.info(f"Polymarket Watchlist: {watched_slugs}")

                for slug in watched_slugs:
                    try:
                        url = f"{base_url}?event_slug={slug}"
                        async with session.get(url, timeout=10) as resp:
                            if resp.status == 200:
                                markets = await resp.json()
                                for market in markets:
                                    if market.get("closed"): continue

                                    question = market.get("question", "")
                                    # FIX: Corrected 'clonTokenIds' to 'clobTokenIds'
                                    tokens = market.get("clobTokenIds", [])
                                    # FIX: Syntax Error. Replaced colon with a comma in isinstance()
                                    if isinstance(tokens, str):
                                        tokens = json.loads(tokens)
                                    
                                    for i, token_id in enumerate(tokens):
                                        if token_id not in id_to_label:
                                            id_to_label[token_id] = f"{slug} | {question} | Outcome {i}"
                                            new_assets.append(token_id)
                            else:
                                logger.error(f"Gamma API error for {slug}: HTTP {resp.status}")

                    except Exception as e:
                        logger.error(f"Gamma API error for {slug}: {e}")
                
                # If we found new outcomes to watch, tell the open WebSocket to start sending them to us.
                if new_assets:
                    # FIX: Corrected API key typo 'assests' to 'assets'
                    await ws.send(json.dumps({"assets": new_assets, "type": "market"}))
                    logger.info(f"Polymarket: Subscribed to {len(new_assets)} new outcome tokens.")
                logger.info(f"Polymarket Heartbeat: Watching {len(id_to_label)} active outcomes across {len(watched_slugs)} slugs.")
            
            except Exception as e:
                logger.error(f"Polymarket sync error: {e}")

            await asyncio.sleep(300)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Polymarket CLOB")

                async with aiohttp.ClientSession() as session:
                    injector_task = asyncio.create_task(update_subscriptions(ws, session))

                    try:
                        # ── MAIN WEBSOCKET LOOP ──────────────────────────────────────────
                        # This infinite loop constantly listens for live trades.
                        while True:
                            message = await ws.recv()
                            data = json.loads(message)
                            events = data if isinstance(data, list) else [data]

                            for event in events:
                                if event.get("event_type") == "trade":
                                    asset_id = event.get("asset_id")
                                    price = float(event.get("price", 0.0))
                                    size = float(event.get("size", 0.0))
                                    notional_usd = price * size

                                    label = id_to_label.get(asset_id, "UNKNOWN")
                                    is_whale = whale_detector.is_unusual(asset_id, notional_usd)

                                    if is_whale:
                                        logger.warning(f"🚨 POLY MARKET WHALE UNUSUAL BET: ${notional_usd:.2f} on {label}")
                                    
                                        raw_event = RawEvent(
                                            source="polymarket",
                                            occurred_at=datetime.now(timezone.utc),
                                            raw_payload={
                                                "asset_label": label,
                                                "side": event.get("side", ""),
                                                "price": price,
                                                "size_shares": size,
                                                "notional_usd": notional_usd,
                                                "is_whale_bet": True
                                            }
                                        )
                                        producer.send(Topics.RAW_PREDICTION, raw_event.model_dump(), key="polymarket")
                    finally:
                        # If the websocket disconnects, kill the background sync task so it doesn't run forever in the void.
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
            
            markets = []
                # Example: Fetching markets sorted by recent volume
            try:   
                url = f"{KALSHI_BASE_URL}/markets?status=open&limit=50"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data.get("markets", [])
                        
                        for market in markets:
                            ticker = market.get("ticker")
                            vol = market.get("volume", 0)
                            
                            # ── VOLUME SPIKE DETECTION ─────────────────────────────────────
                            # Kalshi only tells us "Total Volume". To find out if there was a spike,
                            # we use Redis to remember the volume from 60 seconds ago and calculate the difference (delta).
                            redis_key = f"sentinel:kalshi:vol:{ticker}"
                            loop = asyncio.get_event_loop()
                            # run_in_executor pushes this synchronous Redis call to a background thread to prevent freezing.
                            prev_vol = await loop.run_in_executor(None, redis_client.get, redis_key)
                            
                            if prev_vol:
                                delta = vol - int(prev_vol)
                                
                                yes_bid = market.get("yes_bid", 50)
                                price_usd = yes_bid / 100  # Kalshi prices are often in percentage format (e.g., 75 means $0.75)
                                notional_delta = delta * price_usd

                                if notional_delta >= 1000:  # If there's a spike of $1,000 or more in the last minute, flag it.
                                    logger.warning(f"📈 KALSHI USD SPIKE: {ticker} (+${notional_delta:,.2f})")
                               
                                    event = RawEvent(
                                        source="kalshi",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker,
                                            "title": market.get("title"),
                                            "volume_delta": delta,
                                            "notional_usd": notional_delta,
                                            "total_volume": vol,
                                            "price": price_usd,
                                            "is_anomaly": True
                                        }
                                    )
                                    producer.send(Topics.RAW_PREDICTION, event.model_dump(), key=ticker)
                            
                            # Update state
                            await loop.run_in_executor(None, redis_client.set, redis_key, str(vol), 3600)
                        logger.info(f"Kalshi Heartbeat: Scanned {len(markets)} active markets. No spikes > $1,000 detected.")
                    else:
                        text = await resp.text()
                        logger.error(f"Kalshi API Rejected Connection: HTTP {resp.status} - {text}")    
            except Exception as e:
                logger.error(f"Kalshi polling error: {e}", exc_info=True)
            
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
            stream_polymarket(producer, redis_client),
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