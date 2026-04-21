"""
services/collector-tradfi/main.py

ENTERPRISE TRADFI COLLECTOR
===========================
Sources: Alpaca (Equities WS), Unusual Whales (Options REST), SEC (Form 4 RSS)
Dynamic: Watchlists are driven entirely by Redis (`sentinel:watched:equities`)
"""

import asyncio      # Core Python library for running concurrent tasks (doing multiple things at once).
import aiohttp      # An asynchronous HTTP client (like the 'requests' library, but non-blocking).
import json
import logging
import os
import sys
import feedparser   # Used for parsing RSS feeds (like the SEC's Form 4 updates).
import websockets   # Used for keeping a continuous, open connection to a server for real-time data.
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

UNUSUAL_WHALES_KEY = os.getenv("UNUSUAL_WHALES_API_KEY")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

# This is the key we look for in Redis to know WHICH stocks to track (e.g., AAPL, TSLA).
REDIS_EQUITIES_KEY = "sentinel:watched:equities"
# Time-To-Live (TTL) for deduplication. 86400 seconds = 24 hours.
# If we see a trade ID, we remember it for 24 hours to avoid processing it twice.
DEDUPE_TTL = 86400

# ── UNUSUAL WHALES & SEC (REST POLLING) ───────────────────────────────────────

async def poll_options(session: aiohttp.ClientSession, producer: SentinelProducer, redis_client):
    # If the user hasn't provided an API key in the .env file, just skip this function.
    if not UNUSUAL_WHALES_KEY:
        return
    headers = {"Authorization": f"Bearer {UNUSUAL_WHALES_KEY}"}
    # We only care about "Whale" trades — options trades where the premium paid was over $250,000.
    url = "https://api.unusualwhales.com/api/option-trades?limit=100&min_premium=250000"
    loop = asyncio.get_event_loop()
    try:
        # 1. DYNAMIC FETCH: Get active watchlist from Redis
        # Because Redis is a synchronous library (it blocks the code while waiting for a response),
        # we use 'run_in_executor' to run it in a background thread so it doesn't freeze our async app.
        raw_tickers = await loop.run_in_executor(None, redis_client.smembers, REDIS_EQUITIES_KEY)
        active_tickers = {t.upper() for t in raw_tickers} if raw_tickers else {"QQQ", "SPY", "USO", "GOOG", "ASML"}
        
        # Make the actual HTTP GET request asynchronously.
        async with session.get(url, headers=headers, timeout = 15) as resp:
            if resp.status == 200:
                trades = (await resp.json()).get("data") or []
                for trade in trades:
                    ticker = (trade.get("ticker") or "").upper()

                    # If this trade isn't for a stock we are currently watching, ignore it.
                    if ticker not in active_tickers:
                        continue

                    trade_id = trade.get("id") or trade.get("alert_id", "")
                    redis_key = f"sentinel:seen:uw_trade:{trade_id}"

                    # ── DEDUPLICATION CHECK ─────────────────────────────────────
                    # If the key is already in Redis, we've seen this trade. Skip it!
                    if await loop.run_in_executor(None, redis_client.set, redis_key, DEDUPE_TTL):
                        continue
                    # Mark it as seen.
                    await loop.run_in_executor(None, redis_client.set, redis_key, 1, DEDUPE_TTL)

                    premium = float(trade.get("premium") or trade.get("total_premium") or trade.get("total_premium") or 0)
                    
                    # Standardize the data into our internal format (RawEvent).
                    event = RawEvent(
                        source = "unusual_whales",
                        occurred_at = datetime.now(timezone.utc),
                        raw_payload = {
                            "ticker": ticker, 
                            "trade_type": "OPTIONS_SWEEP",
                            "side": (trade.get("put_call") or "").upper(),
                            "premium_usd": premium,
                            "volume": trade.get("volume"),
                            "strike": trade.get("strike_price"),
                            "expiry": trade.get("expiry"),
                        }
                    )
                    # Send the standardized event to the Kafka message broker.
                    producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key=ticker)
                
            elif resp.status == 429:
                # HTTP 429 means "Too Many Requests". We must back off or they will ban our IP.
                logger.warning("Unusual Whales API rate limit hit. Backing off for 120s.")
                await asyncio.sleep(60)
        
    except Exception as e:
        logger.error(f"Error polling Unusual Whales API: {e}", exc_info=True)

async def poll_form4(session: aiohttp.ClientSession, producer: SentinelProducer, redis_client):
    # The SEC provides a free RSS feed of all recent insider trading forms (Form 4).
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=include&count=40&output=atom"
    loop = asyncio.get_event_loop()
    try:
        async with session.get(
            url,
            timeout = 15,
            # The SEC requires a User-Agent header so they know who is scraping them.
            headers = {"User-Agent": "SENTINEL/1.0"},
        ) as resp:
            if resp.status != 200: return
            content = await resp.read()
        
        # feedparser is slow and synchronous, so we offload it to a background thread.
        feed = await loop.run_in_executor(None, feedparser.parse, content)

        for entry in feed.entries:
            link = entry.get("link", "")
            redis_key = f"sentinel:seen:form4:{link}"

            if await loop.run_in_executor(None, redis_client.exists, redis_key): continue
            await loop.run_in_executor(None, redis_client.set, redis_key, "1", 604800)

            event = RawEvent(
                source="sec_form4",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={"link": link, "title": entry.get("title", ""), "summary": entry.get("summary", "")}
            )
            producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key="form4")
    except Exception as e:
        logger.error(f"SEC Form 4 error: {e}")

# ── ALPACA EQUITIES (WEBSOCKET) ───────────────────────────────────────────────

async def stream_equities(producer: SentinelProducer, redis_client):
    if not ALPACA_API_KEY: return
    # Connect to Alpaca's IEX (Investors Exchange) real-time data stream.
    url = "wss://stream.data.alpaca.markets/v2/iex"

    async def sync_subscriptions(ws):
        """Background task that watches Redis and updates Alpaca WS subs on the fly."""
        loop = asyncio.get_event_loop()
        current_subs = set()
        while True:
            # Check Redis to see what stocks the system currently cares about.
            raw_tickers = await loop.run_in_executor(None, redis_client.smembers, REDIS_EQUITIES_KEY)
            desired_subs = {t.upper() for t in raw_tickers} if raw_tickers else {"SPY", "QQQ", "USO", "GOOG", "ASML"}

            # Set math: Find out which tickers are new and need to be subscribed to.
            new_subs = desired_subs - current_subs
            if new_subs:
                # Send a JSON message to the WebSocket telling the server to start sending us these stocks.
                await ws.send(json.dumps({"action": "subscribe", "trades": list(new_subs)}))
                current_subs.update(new_subs)
                logger.info(f"Alpaca: Added dynamic subscriptions for {new_subs}")
                
            await asyncio.sleep(60)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                # 1. Authenticate with Alpaca as soon as the connection opens.
                await ws.send(json.dumps({"action": "auth", "key": ALPACA_API_KEY, "secret": ALPACA_SECRET_KEY}))
                auth_reply = await json.loads(await ws.recv())
                
                # 2. Start the background loop that manages our subscriptions.
                sync_task = asyncio.create_task(sync_subscriptions(ws))

                try:
                    # 3. Infinite loop: constantly listen for new messages from the WebSocket.
                    while True:
                        message = await ws.recv()
                        for item in json.loads(message):
                            # "T" == "t" means this is a "Trade" event (not a quote or bar).
                            if item.get("T") == "t":
                               ticker, price, size = item.get("S"), item.get("p"), item.get("s")
                               notional = price * size
                               
                               # ── BLOCK TRADE DETECTION ────────────────────────────
                               # A block trade is a massive single transaction (>$500k).
                               # We flag these because they represent institutional (whale) activity.

                               if notional > 500_000:
                                   logger.warning(f"🐳 EQUITY BLOCK: {ticker} ${notional/1e6:.2f}M at ${price}")
                                   event = RawEvent(
                                       source="alpaca_equities",
                                       occurred_at=datetime.now(timezone.utc),
                                       raw_payload={
                                           "ticker": ticker, "trade_type": "BLOCK_TRADE",
                                           "price": price, "size_shares": size, "notional_usd": notional
                                       }
                                   )
                                   producer.send(Topics.RAW_FINANCIAL, event.model_dump(), key=ticker)
                finally:
                    # If the websocket disconnects, kill the background subscription sync task.
                    sync_task.cancel()
        
        except Exception as e:
            logger.error(f"Alpaca error: {e}. Reconnecting...")
            await asyncio.sleep(5)


# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def run_polling(producer: SentinelProducer, redis_client):
    # Create one single HTTP session and reuse it. This is much faster than opening a new connection every time.
    connector = aiohttp.TCPConnector(limit = 5, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            # Run both polling functions simultaneously.
            await asyncio.gather(
                poll_options(session, producer, redis_client),
                poll_form4(session, producer, redis_client),
            )
            # Wait 60 seconds before polling again.
            await asyncio.sleep(60)

async def main():
    producer = SentinelProducer()
    redis_client = get_redis()
    try:
        # gather() runs all these infinite loops side-by-side concurrently.
        await asyncio.gather(
            run_polling(producer, redis_client),
            stream_equities(producer, redis_client),
        )
        
    finally:
        # Always ensure we close the Kafka connection properly when the app shuts down.
        producer.close()

if __name__ == "__main__":
    asyncio.run(main())



                                       
                
                
                
    