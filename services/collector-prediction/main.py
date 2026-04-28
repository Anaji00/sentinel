"""
services/collector-prediction/main.py

ENTERPRISE PREDICTION MARKET COLLECTOR
======================================
Ingests Polymarket and Kalshi.
Features: 
- Real-time WebSocket streaming
- Stateless architecture: Forwards high-signal data to Kafka for central ML scoring.
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("collector.prediction")

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
# ── POLYMARKET STREAM ─────────────────────────────────────────────────────────        

async def stream_polymarket(producer: SentinelProducer, redis_client):
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    redis_key = "sentinel:polymarket:watched_slugs"
    id_to_label = {}

    async def update_subscriptions(ws, session):
        base_url = "https://gamma-api.polymarket.com/events"
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
                        url = f"{base_url}?slug={slug}"
                        async with session.get(url, timeout=10) as resp:
                            if resp.status == 200:
                                events_data = await resp.json()
                                for market in events_data.get("markets", []):
                                    if market.get("closed"): continue

                                    question = market.get("question", "")
                                    tokens = market.get("clobTokenIds", [])
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
                
                if new_assets:
                    await ws.send(json.dumps({"asset_ids": new_assets, "type": "market"}))
                    logger.info(f"Polymarket: Subscribed to {len(new_assets)} new outcome tokens.")
                logger.info(f"Polymarket Heartbeat: Watching {len(id_to_label)} active outcomes.")
            
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
                        while True:
                            message = await ws.recv()
                            data = json.loads(message)
                            
                            # FIX: Properly cast single dictionaries to a list to prevent dropping trades
                            events = data if isinstance(data, list) else [data]

                            
                            for event in events:
                                if event.get("event_type") == "trade":
                                    asset_id = event.get("asset_id")
                                    price = float(event.get("price", 0.0))
                                    size = float(event.get("size", 0.0))
                                    notional_usd = price * size

                                    # Static filter: Only forward trades > $1000 to save bandwidth
                                    if notional_usd >= 1000:
                                        label = id_to_label.get(asset_id, "UNKNOWN")
                                        
                                        raw_event = RawEvent(
                                            source="polymarket",
                                            occurred_at=datetime.now(timezone.utc),
                                            raw_payload={
                                                "asset_id": asset_id,  # Passed for Redis baselining
                                                "asset_label": label,
                                                "side": event.get("side", ""),
                                                "price": price,
                                                "size_shares": size,
                                                "notional_usd": notional_usd
                                            }
                                        )
                                        producer.send(Topics.RAW_PREDICTION, raw_event.model_dump(), key="polymarket")
                    finally:
                        injector_task.cancel()

        except Exception as e:
            logger.error(f"Polymarket WS Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

# ── KALSHI POLLER ─────────────────────────────────────────────────────────────

async def poll_kalshi(producer: SentinelProducer, redis_client):
    session_timeout = aiohttp.ClientTimeout(total=10)
    
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            markets = []
            try:   
                url = f"{KALSHI_BASE_URL}/markets?status=active&limit=50"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data.get("markets", [])
                        
                        max_delta_seen = 0
                        for market in markets:
                            ticker = market.get("ticker")
                            vol = market.get("volume", 0)
                            
                            redis_key = f"sentinel:kalshi:vol:{ticker}"
                            loop = asyncio.get_event_loop()
                            prev_vol = await loop.run_in_executor(None, redis_client.get, redis_key)
                            
                            if prev_vol:
                                delta = vol - int(prev_vol)
                                yes_bid = market.get("yes_bid", 50)
                                price_usd = yes_bid / 100 
                                notional_delta = delta * price_usd
                                
                                if notional_delta > max_delta_seen:
                                    max_delta_seen = notional_delta

                                # Static filter: Only forward deltas > $1000 to save bandwidth
                                if notional_delta >= 500:  
                                    event = RawEvent(
                                        source="kalshi",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker,
                                            "title": market.get("title"),
                                            "volume_delta": delta,
                                            "notional_usd": notional_delta,
                                            "total_volume": vol,
                                            "price": price_usd
                                        }
                                    )
                                    producer.send(Topics.RAW_PREDICTION, event.model_dump(), key=ticker)
                            
                            await loop.run_in_executor(None, redis_client.set, redis_key, str(vol), 3600)
                        logger.info(f"Kalshi Heartbeat: Scanned {len(markets)} active markets. Max 1-min delta was ${max_delta_seen:,.2f}")
                    else:
                        text = await resp.text()
                        logger.error(f"Kalshi API Rejected Connection: HTTP {resp.status} - {text}")    
            except Exception as e:
                logger.error(f"Kalshi polling error: {e}", exc_info=True)
            
            await asyncio.sleep(60)

# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
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