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
        markets_url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&order=volume&ascending=false&limit=100"
        
        while True:
            try:
                # 1. Fetch manual slugs from Redis
                raw_slugs = await redis_client.raw.smembers(redis_key)
                watched_slugs = [s.decode() if isinstance(s, bytes) else s for s in raw_slugs] if raw_slugs else []
                
                # 2. Fetch dynamic active slugs from Polymarket
                try:
                    async with session.get(markets_url, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            # Extract unique slugs from the active markets list
                            dynamic_slugs = list(set(m.get("slug") for m in data if m.get("slug")))
                            
                            # Add dynamic slugs to Redis for visibility to other services
                            if dynamic_slugs:
                                await redis_client.raw.sadd(redis_key, *dynamic_slugs)
                                watched_slugs.extend([s for s in dynamic_slugs if s not in watched_slugs])
                        else:
                            body = await resp.text()
                            logger.error(f"Polymarket dynamic market lookup returned status {resp.status}: {body[:200]}")
                except Exception as e:
                    logger.error(f"Failed to fetch dynamic slugs from Polymarket: {e!r}")
                
                # Fallback if both Redis and API are empty
                if not watched_slugs:
                    watched_slugs = ["us-x-iran-permanent-peace-deal-by"]
                
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
                                    
                                    outcomes = market.get("outcomeNames", [])
                                    if isinstance(outcomes, str):
                                        try:
                                            outcomes = json.loads(outcomes)
                                        except:
                                            outcomes = []
                                    
                                    for i, token_id in enumerate(tokens):
                                        if token_id not in id_to_label:
                                            outcome_name = outcomes[i] if (outcomes and i < len(outcomes)) else f"Outcome {i}"
                                            id_to_label[token_id] = f"{slug} | {question} | {outcome_name}"
                                            new_assets.append(token_id)
                            else:
                                logger.error(f"Gamma API error for {slug}: HTTP {resp.status}")

                    except Exception as e:
                        logger.error(f"Gamma API connection error for {slug}: {e}")
                
                if new_assets:
                    await ws.send(json.dumps({"assets_ids": new_assets, "type": "market"}))
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
                                if event.get("event_type") in ("trade", "last_trade_price"):
                                    asset_id = event.get("asset_id")
                                    price = float(event.get("price", 0.0))
                                    size = float(event.get("size", 0.0))
                                    notional_usd = price * size

                                    label = id_to_label.get(asset_id, "UNKNOWN")
                                    
                                    # Extract outcome name to calculate yes/no probabilities
                                    outcome_name = label.split(" | ")[-1] if " | " in label else ""
                                    yes_prob = None
                                    no_prob = None
                                    if outcome_name.strip().lower() == "yes":
                                        yes_prob = price
                                        no_prob = round(1.0 - price, 4)
                                    elif outcome_name.strip().lower() == "no":
                                        no_prob = price
                                        yes_prob = round(1.0 - price, 4)

                                    logger.info(f"Polymarket Trade Found | {size} shares @ ${price} | {label} | YesProb: {yes_prob} | NoProb: {no_prob}")

                                    # STATELESS: Pipe directly to Kafka. Let Enrichment score anomalies.
                                    raw_event = RawEvent(
                                        source="polymarket",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "asset_label": label,
                                            "side": event.get("side", ""),
                                            "price": price,
                                            "size_shares": size,
                                            "notional_usd": notional_usd,
                                            "yes_probability": yes_prob,
                                            "no_probability": no_prob,
                                            "outcome_name": outcome_name
                                        }
                                    )
                                    await producer.send(Topics.RAW_PREDICTION, raw_event.model_dump(), key="polymarket")
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
                # Query up to 1000 markets to ensure we catch most active elections and categories
                url = f"{KALSHI_BASE_URL}/markets?limit=1000"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data.get("markets", [])
                        
                        # 1. Filter only open, non-expired markets with trading activity
                        open_markets = []
                        for market in markets:
                            if market.get("status") != "open":
                                continue
                                
                            exp_ts = market.get("expiration_ts")
                            if exp_ts:
                                try:
                                    exp_dt = datetime.fromisoformat(exp_ts.replace('Z', '+00:00'))
                                    if exp_dt < datetime.now(timezone.utc):
                                        continue
                                except Exception:
                                    pass
                            
                            if market.get("volume", 0) <= 0:
                                continue
                            
                            open_markets.append(market)
                            
                        # 2. Sort open markets by volume descending to dynamically keep top active bets
                        open_markets.sort(key=lambda m: m.get("volume", 0), reverse=True)
                        
                        # 3. Publish the top 100 most active markets
                        for market in open_markets[:100]:
                            ticker = market.get("ticker")
                            vol = market.get("volume", 0)
                            
                            # Support both legacy cent-based integers and modern dollar-based fields
                            yes_bid = market.get("yes_bid")
                            no_bid = market.get("no_bid")
                            yes_bid_dollars = market.get("yes_bid_dollars")
                            no_bid_dollars = market.get("no_bid_dollars")
                            
                            # Standardize probability values (0.0 to 1.0)
                            if yes_bid_dollars is not None:
                                yes_prob = float(yes_bid_dollars)
                            elif yes_bid is not None:
                                yes_prob = yes_bid / 100.0
                            else:
                                yes_prob = None
                                
                            if no_bid_dollars is not None:
                                no_prob = float(no_bid_dollars)
                            elif no_bid is not None:
                                no_prob = no_bid / 100.0
                            else:
                                no_prob = None
                                
                            price_usd = yes_prob if yes_prob is not None else 0.50
                            
                            event = RawEvent(
                                source="kalshi",
                                occurred_at=datetime.now(timezone.utc),
                                raw_payload={
                                    "ticker": ticker,
                                    "title": market.get("title"),
                                    "total_volume": vol,
                                    "price": price_usd,
                                    "yes_bid": yes_bid_dollars if yes_bid_dollars is not None else yes_bid,
                                    "no_bid": no_bid_dollars if no_bid_dollars is not None else no_bid,
                                    "yes_probability": yes_prob,
                                    "no_probability": no_prob
                                }
                            )
                            await producer.send(Topics.RAW_PREDICTION, event.model_dump(), key=ticker)
                            
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
    await producer.start()
    redis_client = await get_redis()
    try:
        await asyncio.gather(
            stream_polymarket(producer, redis_client),
            poll_kalshi(producer)
        )

    except KeyboardInterrupt:
        logger.info("Shutting down prediction collector...")
    finally:
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())