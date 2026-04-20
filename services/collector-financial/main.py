"""
services/collector-financial/main.py
 
FINANCIAL COLLECTOR — Phase 2
==============================
Ingests options flow, dark pool prints, COT reports, and insider trades.
Pushes RawEvents to Kafka topic: raw.financial
 
Data sources:
  Unusual Whales API — options sweeps, dark pool
    https://unusualwhales.com/api  (paid, ~$50/mo)
    Endpoint: GET /api/option-trades  (options sweeps)
    Endpoint: GET /api/darkpool       (dark pool prints)
 
  CFTC COT Reports — weekly, free, public
    https://www.cftc.gov/dea/options/deaoptsf.htm
    Published every Friday at 15:30 ET
 
  SEC EDGAR Form 4 — insider trades, free RSS
    https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&search_text=
 
Poll intervals:
  Options/dark pool: every 60s (real-time during market hours)
  COT: weekly (Friday 15:30 ET)
  Form 4: every 5 min
"""

# Import standard Python libraries for asynchronous operations, networking, logging, and system path management.
import asyncio
import aiohttp
import logging
import os
import sys
import json
import time
# Import datetime tools to accurately timestamp our events in UTC.
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import websockets

 
# Import third-party libraries for parsing RSS feeds and managing environment variables.
import feedparser
from dotenv import load_dotenv

# Set up the project's root directory to allow imports from the 'shared' folder.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")
 
# Import our custom Kafka producer and data models from the shared library.
from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis

# Configure the logging system to format messages with timestamps and log levels.
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Create a logger instance specific to this financial collector module.
logger = logging.getLogger("collector.financial")
 
# Load the API key for Unusual Whales from environment variables.
UNUSUAL_WHALES_KEY = os.getenv("UNUSUAL_WHALES_API_KEY")
# Define the base URL for the Unusual Whales API.
UW_BASE            = "https://api.unusualwhales.com/api"

# Define a set of stock tickers we are interested in. This acts as a filter to reduce noise.
# Watched tickers for options/dark pool collection. We can expand this later or make it dynamic.
WATCHED_TICKERS = { 
    "USO","BNO","UCO","XOP","OIH","UNG","BOIL",
    "LMT","RTX","NOC","GD","BA",
    "GLD","IAU","GOLD",
    "ZIM","DAC","SBLK","GOGL",
} # shipping, defense, energy, oil, gold tickers

# Define the polling intervals in seconds for different data sources.
OPTIONS_POLL_INTERVAL = 60  # seconds
FORM4_POLL_INTERVAL = 300   # 5 minutes

DEDUPE_TTL = 86400
# ── UNUSUAL WHALES — OPTIONS FLOW ─────────────────────────────────────────────

# Define an asynchronous function to poll for options trades.
async def poll_options(
        session: aiohttp.ClientSession,
        producer: SentinelProducer,
        seen_ids: set,
        redis_client,
):
    # If the API key is not configured, log an error and exit the function.
    if not UNUSUAL_WHALES_KEY:
        logger.error("UNUSUAL_WHALES_API_KEY is not set in environment variables.")
        return
    
    # Set up the authorization headers and the full API endpoint URL.
    headers = {"Authorization": f"Bearer {UNUSUAL_WHALES_KEY}"}
    url     = f"{UW_BASE}/option-trades?limit=100&min_premium=250000"
    loop = asyncio.get_event_loop()
    
    # Use a try...except block to gracefully handle network errors or timeouts.
    try:
        # Make an asynchronous GET request to the API.
        async with session.get(url, headers=headers, timeout = aiohttp.ClientTimeout(total = 15)) as resp:
            # If the request was successful (HTTP 200)...
            if resp.status == 200:
                # Parse the JSON response.
                data = await resp.json()
                # Safely get the list of trades, defaulting to an empty list if not found.
                trades = data.get("data") or []

                # Loop through each trade in the response.
                for trade in trades:
                    # Extract the unique ID and ticker for the trade.
                    trade_id = trade.get("id") or trade.get("alert_id", "")
                    ticker = (trade.get("ticker") or "").upper()
                    redis_key = f"sentinel:seen:u_trade:{trade_id}"

                    is_seen = await loop.run_in_executor(None, redis_client.exists, redis_key)
                    if is_seen or ticker not in WATCHED_TICKERS:
                        continue
                    
                    await loop.run_in_executor(None, redis_client.set, redis_key, 1, DEDUPE_TTL)
                    
                    # Mark this trade ID as seen.
                    seen_ids.add(trade_id)
                    # To prevent the 'seen_ids' set from growing indefinitely, clear it if it gets too large.
                    if len(seen_ids) > 50_000:  # Prevent unbounded growth
                        seen_ids.clear()
                    
                    # Extract the premium (cost) of the trade and cast it to a float.
                    premium = float(trade.get("premium") or trade.get("total_premium") or 0)

                    # Create a standardized RawEvent object with the trade data.
                    event = RawEvent(
                        source = "unusual_whales",
                        occurred_at = datetime.now(timezone.utc),
                        raw_payload = {
                            "ticker":           ticker,
                            "side":             (trade.get("put_call") or "").upper(),
                            "trade_type":       trade.get("trade_type", "SWEEP"),
                            "premium_usd":      premium,
                            "volume":           trade.get("volume"),
                            "open_interest":    trade.get("open_interest"),
                            "strike":           trade.get("strike_price"),
                            "expiry":           trade.get("expiry"),
                            "implied_volatility": trade.get("iv"),
                            "underlying_price": trade.get("underlying_price"),
                            "exchange":         trade.get("exchange"),
                            "otm_percentage":   trade.get("otm"),
                        },
                    )
                    # Send the event to the 'raw.financial' Kafka topic. The ticker is used as the key to ensure ordering.
                    producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key=ticker)
                    # Log a summary of the trade for real-time monitoring.
                    logger.info(f"Options: {ticker} ${premium/1e6:.1f}M "
                                f"{trade.get('put_call','?')} {trade.get('trade_type','?')}")
 
            # If we hit the API's rate limit (HTTP 429), log a warning and pause.
            elif resp.status == 429:
                logger.warning("Unusual Whales API rate limit hit. Backing off for 120s.")
                await asyncio.sleep(120)
            # For any other HTTP error, log the status and response text.
            else:
                logger.warning(f"Unusual Whales API returned status {resp.status}. Response: {await resp.text()}")
    # Handle request timeouts specifically.
    except asyncio.TimeoutError:
        logger.warning("Unusual Whales API request timed out.")
    # Catch any other exceptions that might occur.
    except Exception as e:
        logger.error(f"Error polling Unusual Whales API: {e}", exc_info=True)


# ── SEC FORM 4 — INSIDER TRADES ───────────────────────────────────────────────

async def poll_form4(
        # This function polls the SEC's public RSS feed for Form 4 (insider trading) filings.
        session: aiohttp.ClientSession,
        producer: SentinelProducer,
        redis_client,
):
    """
    SEC EDGAR Form 4 RSS feed — free, no auth.
    Covers all insider transactions filed with SEC.
    """
    # The URL for the SEC's real-time Form 4 filings RSS feed.
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&output=atom"
    loop = asyncio.get_event_loop()
    try:
        # Make an async GET request, pretending to be a browser with a User-Agent.
        async with session.get(
            url,
            timeout = aiohttp.ClientTimeout(total = 15),
            headers = {"User-Agent": "SENTINEL/1.0 research@sentinel.local"},
        ) as resp:
            # If the request fails, log it and skip this cycle.
            if resp.status != 200:
                return # Log and skip if we can't fetch the feed
            # Read the raw XML content of the feed.
            content = await resp.read()

        # how this works: 
        # get_event_loop() gets the current asyncio event loop. 
        # feedparser is a synchronous library that parses RSS feeds, 
        # and it can be slow if the feed is large. To avoid blocking the entire 
        # event loop while feedparser does its work, 
        # we run it in a separate thread using run_in_executor(). 
        # This allows our async code to remain responsive while 
        # feedparser processes the RSS feed in the background. 
        # Once feedparser is done, we get the parsed feed back in our 
        # async function and can continue processing it without having 
        # blocked other async tasks.
         # an event loop is a core part of asyncio that manages and schedules asynchronous tasks.
        # Offload the blocking 'feedparser.parse' call to a separate thread.
        feed = await loop.run_in_executor(None, feedparser.parse, content) # the "feed" is the parsed RSS feed returned by feedparser after processing the raw content. It contains entries representing individual insider trades.
        # Run_in_executor() is used to run the blocking 
        # feedparser.parse function in a separate thread, 
        # allowing our async code to remain responsive while 

        # Loop through each entry (filing) in the parsed feed.
        for entry in feed.entries[:40]:
            link = entry.get("link", "")
            title = entry.get("title", "")
            redis_key = f"sentinel:seen:sec_form4:{link}"

            is_seen = await loop.run_in_executor(None, redis_client.exists, redis_key)
            if is_seen:
                continue

            await loop.run_in_executor(None, redis_client.set, redis_key, "1", 604800)

            

            # Title format: "4 - SMITH JOHN (Reporting) APPLE INC (Subject)"
            # Extract ticker from summary or title if possible
            # NOTE: The actual extraction logic is handled later in the 'financial.py' enricher.
            # This collector's job is just to grab the raw data.

            # Create a RawEvent with the filing's data.
            event = RawEvent(
                source = "sec_form4",
                occurred_at = datetime.now(timezone.utc),
                raw_payload = {
                    "link":  link,
                    "title": title,
                    "summary": entry.get("summary", ""),
                },
            
            )
            # Send the event to Kafka. 'form4' is used as the key.
            producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key="form4")

    except Exception as e:
        logger.error(f"Error polling SEC Form 4 feed: {e}", exc_info=True)

async def stream_binance_liquidations(producer: SentinelProducer):
    """
    Connects to Binance Futures WebSocket to stream real-time forced liquidations.
    Crucial for detecting cascading market leverage wipes.
    """
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"

    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info("Connected to Binance Futures WebSocket")
                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    order = data.get("o", {})
                    symbol = order.get("s")
                    side = order.get("S")
                    price = float(order.get("p", 0))
                    qty = float(order.get("q", 0))

                    event = RawEvent(
                        source = "binance_futures",
                        occurred_at = datetime.now(timezone.utc),
                        raw_payload = {
                            "asset": symbol,
                            "trade_type": "LIQUIDATION",
                            "side": side,
                            "price": price,
                            "size_tokens": qty,
                            "notional_usd": price * qty
                        }
                    )
                    producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key=symbol)
        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("Binance WS closed. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Binance WS error: {e}. Reconnecting in 5s...", exc_info=True)
            await asyncio.sleep(5)

# ── POLYMARKET ODDS (WEBSOCKET) ───────────────────────────────────────────────

async def poll_gamma_api(session: aiohttp.ClientSession, sub_queue: asyncio.Queue, id_to_label: dict, redis_client):
    """
    Periodically polls the Gamma API for watched event slugs stored in Redis.
    """
    base_url = "https://gamma-api.polymarket.com/events/slug/"
    redis_key = "sentinel:polymarket:watched_slugs"
    loop = asyncio.get_event_loop()
    while True:
        new_assets = []
        # 1. Dynamically fetch the current watchlist from Redis
        # (Using a Redis SET ensures no duplicate slugs are added)
        raw_slugs = await loop.run_in_executor(None, redis_client.smembers, redis_key)
        watched_slugs = [s for s in raw_slugs] if raw_slugs else []

        if not watched_slugs:
            logger.debug("No watched Polymarket slugs found in Redis. Idling...")
            await asyncio.sleep(60)
            continue

        for slug in watched_slugs:
            try:
                async with session.get(f"{base_url}{slug}", timeout = 10) as resp:
                    if resp.status != 200:
                        continue
                    event_data = await resp.json
                    markets = event_data.get("markets", [])
                    
                    for market in markets:
                        if market.get("closed"):
                            continue

                        question = market.get("question", "Unknown Market")
                        outcomes = market.get("outcomes", [])

                        raw_tokens = market.get("clobTokenIds", "[]")
                        token_ids = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens

                        for i, token_id in enumerate(token_ids):
                            if token_id not in id_to_label:
                                outcome_label = outcomes[i] if i < len(outcomes) else f"Outcome_{i}"
                                label = f"{slug} | {question} | {outcome_label}"
                                id_to_label[token_id] = label
                                new_assets.append(token_id)
            except Exception as e:
              logger.error(f"Error fetching Gamma API for slug {slug}: {e}")  

        if new_assets:
            logger.info(f"Discovered {len(new_assets)} new Polymarket assets. Queueing for WS subscription.")
            await sub_queue.put(new_assets)
        
        await asyncio.sleep(300)




        
async def stream_polymarket_odds(producer: SentinelProducer):
    """
    Connects to the Polymarket Central Limit Order Book (CLOB) WebSocket.
    Streams real-time trades and price updates for prediction markets.
    """

    # Asset IDs are long numeric strings (ERC-1155 token IDs) assigned by Polymarket to specific outcome tokens.
    # We pull these from environment variables so the ingestion engine can track dynamic markets 
    # without hardcoding hashes.
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    asset_ids_str = os.getenv("POLYMARKET_ASSET_IDS", "")
    assets = [a.strip() for a in asset_ids_str.split(",")] if asset_ids_str else []

    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info("Connected to Polymarket CLOB WebSocket")
                while True:
                    if assets:
                        sub_payload = {
                            "assets": assets,
                            "type": "market"
                        }
                        await ws.send(json.dumps(sub_payload))
                        logger.info(f"Subscribed to {len(assets)} Polymarket assets")
                    else:
                        logger.warning("No POLYMARKET_ASSET_IDS set. Connected, but awaiting subscriptions.")

                    while True:
                        message = await ws.recv()
                        data = json.loads(message)
                        events = data if isinstance(data, list) else [data]
                        for event in events:
                            event_type = event.get("event_type")
                            if event_type == "trade":
                                asset_id = event.get("asset_id")
                                price = float(event.get("price", 0))
                                size = float(event.get("size", 0))
                                side = event.get("side", "")

                                raw_event = RawEvent(
                                    source = "polymarket_clob",
                                    occurred_at = datetime.now(timezone.utc),
                                    raw_payload = {
                                        "asset_id": asset_id,
                                        "trade_type": "PREDICTION_MARKET_TRADE",
                                        "side": side,
                                        "price": price,
                                        "size_shares": size,
                                        "notional_usd": price * size,
                                        "market_id": event.get("market")
                                    }
                                )
                                producer.send(Topics.RAW_FINANCIAL, raw_event.model_dump(), key=asset_id)
                            elif event_type == "last_price":
                                asset_id = event.get("asset_id")
                                price = float(event.get("price", 0))
                                raw_event = RawEvent(
                                    source="polymarket_clob",
                                    occurred_at=datetime.now(timezone.utc),
                                    raw_payload={
                                        "asset_id": asset_id,
                                        "trade_type": "PRICE_UPDATE",
                                        "price": price,
                                        "market_id": event.get("market")
                                    }
                                )
                                producer.send(Topics.RAW_FINANCIAL, raw_event.model_dump(), key=asset_id)


        except websockets.exceptions.ConnectionClosed:
            logger.warning("Polymarket CLOB WS closed. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Polymarket CLOB WS error: {e}. Reconnecting in 5s...", exc_info=True)
            await asyncio.sleep(5)
# ── MAIN ──────────────────────────────────────────────────────────────────────

async def collect(producer: SentinelProducer, redis_client):
    connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
    
    async def run_polling():

    # Create a single, long-lived session to reuse connections.
        async with aiohttp.ClientSession(connector=connector) as session:
            cycle = 0
            # Start the main, infinite collection loop.
            while True:
                cycle += 1
                # Use asyncio.gather to run all polling functions concurrently.
                # This is much faster than running them one after another.
                await asyncio.gather(
                    poll_options(session, producer, redis_client),
                    poll_form4(session, producer, redis_client),
                )
                # Wait for the defined interval before starting the next cycle.
                await asyncio.sleep(OPTIONS_POLL_INTERVAL)
                logger.debug(f"Completed financial collection cycle {cycle}")

    logger.info("Gathering all financial streams...")
    await asyncio.gather(
        run_polling(),
        stream_binance_liquidations(producer),
        stream_polymarket_odds(producer),
    )
async def main():
    # Log a startup banner with configuration details.
    logger.info("=" * 60)
    logger.info("SENTINEL  Financial Collector")
    logger.info(f"Unusual Whales: {'configured' if UNUSUAL_WHALES_KEY else 'NOT configured'}")
    logger.info(f"Watched tickers: {len(WATCHED_TICKERS)}")
    logger.info("=" * 60)
    
    # Create an instance of our Kafka producer.
    producer = SentinelProducer()
    redis_client = get_redis()
    try:
        # Start the main collection logic.
        await collect(producer, redis_client)
    # Allow the user to shut down gracefully with Ctrl+C.
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Ensure the producer is closed cleanly, flushing any buffered messages.
        producer.close()
 
 
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())