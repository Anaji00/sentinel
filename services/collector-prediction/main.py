"""
services/collector-prediction/main.py

ENTERPRISE PREDICTION MARKET COLLECTOR
======================================
Ingests Polymarket and Kalshi.
Stateless Mode: Pipes raw volume and trades directly to Kafka.
Anomaly scoring (Whales/EMA) is handled downstream by the Enrichment service.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s"
)
logger = logging.getLogger("collector.prediction")

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# ── POLYMARKET STREAM ─────────────────────────────────────────────────────────        

async def stream_polymarket(producer: SentinelProducer, redis_client):
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    redis_key = "sentinel:polymarket:watched_slugs"
    id_to_label = {}

    async def update_subscriptions(ws, session):
        base_url = "https://gamma-api.polymarket.com/markets"
        loop = asyncio.get_event_loop()
        
        while True:
            try:
                raw_slugs = await loop.run_in_executor(None, redis_client.raw.smembers, redis_key)
                watched_slugs = [s.decode() if isinstance(s, bytes) else s for s in raw_slugs] if raw_slugs else [
                    "us-x-iran-permanent-peace-deal-by"
                ]
                
                logger.info(f"Heartbeat | Polymarket sync. Tracked slugs ({len(watched_slugs)}): {watched_slugs}")
                new_assets = []

                for slug in watched_slugs:
                    try:
                        url = f"{base_url}?event_slug={slug}"
                        async with session.get(url, timeout=10) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                
                                # FIX: Gamma API returns a list of Events, which contain Markets.
                                markets = []
                                if isinstance(data, list):
                                    for item in data:
                                        if isinstance(item, dict) and "markets" in item:
                                            markets.extend(item["markets"])
                                        elif isinstance(item, dict):
                                            markets.append(item)
                                else:
                                    continue

                                for market in markets:
                                    if not isinstance(market, dict) or market.get("closed"): 
                                        continue

                                    question = market.get("question", "")
                                    tokens = market.get("clobTokenIds", [])
                                    if isinstance(tokens, str):
                                        try:
                                            tokens = json.loads(tokens)
                                        except:
                                            continue
                                    
                                    for i, token_id in enumerate(tokens):
                                        if token_id not in id_to_label:
                                            id_to_label[token_id] = f"{slug} | {question} | Outcome {i}"
                                            new_assets.append(token_id)
                            else:
                                logger.error(f"Gamma API error for {slug}: HTTP {resp.status}")

                    except Exception as e:
                        logger.error(f"Gamma API connection error for {slug}: {e}")
                
                if new_assets:
                    await ws.send(json.dumps({"asset_jds": new_assets, "type": "market"}))
                    logger.info(f"Polymarket: Subscribed to {len(new_assets)} new outcome tokens.")
                
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
                            events = data if isinstance(data, list) else [data]

                            for event in events:
                                if event.get("event_type") == "trade":
                                    asset_id = event.get("asset_id")
                                    price = float(event.get("price", 0.0))
                                    size = float(event.get("size", 0.0))
                                    notional_usd = price * size

                                    label = id_to_label.get(asset_id, "UNKNOWN")
                                    
                                    logger.info(f"Polymarket Trade Found | {size} shares @ ${price} | {label}")

                                    # STATELESS: Pipe directly to Kafka. Let Enrichment score anomalies.
                                    raw_event = RawEvent(
                                        source="polymarket",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
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

async def poll_kalshi(producer: SentinelProducer):
    """
    Polls Kalshi REST API for active event volumes.
    STATELESS: Sends raw volume to Kafka for Enrichment to calculate Deltas/Spikes.
    """
    session_timeout = aiohttp.ClientTimeout(total=10)
    
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            try:   
                # FIX: Removed status=open to prevent 400 Bad Request error
                url = f"{KALSHI_BASE_URL}/markets?limit=50"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data.get("markets", [])
                        
                        for market in markets:
                            ticker = market.get("ticker")
                            vol = market.get("volume", 0)
                            yes_bid = market.get("yes_bid", 50)
                            price_usd = yes_bid / 100 
                            
                            event = RawEvent(
                                source="kalshi",
                                occurred_at=datetime.now(timezone.utc),
                                raw_payload={
                                    "ticker": ticker,
                                    "title": market.get("title"),
                                    "total_volume": vol,
                                    "price": price_usd
                                }
                            )
                            producer.send(Topics.RAW_PREDICTION, event.model_dump(), key=ticker)
                            
                    else:
                        text = await resp.text()
                        logger.error(f"Kalshi API Rejected Connection: HTTP {resp.status} - {text}")    
            except Exception as e:
                logger.error(f"Kalshi polling error: {e}", exc_info=True)
            
            await asyncio.sleep(60) 

# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL PREDICTION MARKET COLLECTOR (STATELESS)")
    logger.info("=" * 60)

    producer = SentinelProducer()
    redis_client = get_redis()
    try:
        await asyncio.gather(
            stream_polymarket(producer, redis_client),
            poll_kalshi(producer)
        )

    except KeyboardInterrupt:
        logger.info("Shutting down prediction collector...")
    finally:
        producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())