"""
services/collector-radar/main.py
The Full-Market Firehose. Listens to all US Equities. Feeds the Finnhub high-res tracker.
"""
import asyncio
import json
import logging
import os
import sys
import websockets
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.db import get_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("collector.radar")

ALPACA_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET")
BLOCK_THRESHOLD = 500_000  # $500k USD

async def stream_all_equities(redis_client):
    if not ALPACA_KEY or not ALPACA_SECRET:
        logger.error("ALPACA_API_KEY and ALPACA_API_SECRET must be set in environment variables.")
        return
    
    url = "wss://stream.data.alpaca.markets/v2/iex"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                await ws.send(json.dumps({"action": "authenticate", "key": ALPACA_KEY, "secret": ALPACA_SECRET}))
                auth = await ws.recv()
                logger.info(f"Authenticated with Alpaca: {auth}")

                await ws.send(json.dumps({"action": "subscribe", "trades": ["*"]}))
                sub_resp = await ws.recv()
                logger.info(f"RADAR SCANNING FOR TICKERS: {sub_resp}")

                msg_count = 0
                while True:
                    payload = (json.loads(await ws.recv()))
                    for event in payload:
                        if event.get("T") == "t":  # Trade event
                            sym = event.get("S")
                            price = float(event.get("p"))
                            size = float(event.get("s"))
                            notional = price * size
                            msg_count += 1
                            if msg_count % 10000 == 0:
                                logger.info(f"📡 Radar Heartbeat: Scanned {msg_count} market-wide trades.")
                            if notional >= BLOCK_THRESHOLD:
                                logger.warning(f"🚨 RADAR DETECTED BLOCK: {sym} | ${notional/1e6:.2f}M. Routing to Finnhub.")
                                # Store in Redis for Finnhub to pick up (with a TTL of 1 hour)
                                redis_client.raw.sadd("sentinel:watched:equities", sym)
        except Exception as e:
            logger.error(f"RADAR STREAM ERROR: {e}", exc_info=True)
            await asyncio.sleep(5)  # Backoff before reconnecting
            
async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL RADAR ONLINE")
    logger.info("=" * 60)
    redis_client = get_redis()
    await stream_all_equities(redis_client)

if __name__ == "__main__":
    asyncio.run(main())