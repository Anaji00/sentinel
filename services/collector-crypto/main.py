"""
services/collector-crypto/main.py

ENTERPRISE CRYPTO COLLECTOR
===========================
Sources: Binance Futures (Liquidations), Ethereum RPC (Mempool Whale Tracking)
Dynamic: Monitors `sentinel:watched:wallets` for suspect address tracking.
"""

import asyncio      # Used to run multiple tasks (like two WebSockets) concurrently.
import json
import logging
import os
import sys
import websockets   # Used for persistent, real-time connections to data providers.
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
logger = logging.getLogger("collector.crypto")

ETH_WSS_URL = os.getenv("ETH_RPC_WSS_URL")
# Any transfer above this USD amount is considered a "Whale" move.
WHALE_THRESHOLD_USD = 50_000

async def stream_binance_liquidations(producer: SentinelProducer):
    # Connects to the Binance Futures stream for "Forced Orders" (Liquidations).
    # A liquidation happens when a trader's leveraged position loses too much money and the exchange forcefully closes it.
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    while True:
        try:
            logger.info("Heartbeat | Connecting to Binance liquidations stream...")
            # ping_interval=20 sends a heartbeat every 20 seconds so the server doesn't disconnect us.
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Binance Liquidations")
                while True:
                    data = json.loads(await ws.recv())
                    order = data.get("o", {})
                    # Extract the symbol (e.g., "BTCUSDT"), side (Buy/Sell), price, and quantity.
                    symbol, side, price, qty = order.get("s"), order.get("S"), float(order.get("p", 0)), float(order.get("q", 0))
                    logger.info(f"Binance Liquidation | {symbol} | {side} | ${price:,.2f} | Size: {qty}")

                    # Standardize the raw data into our internal RawEvent format.
                    event = RawEvent(
                        source="binance_futures",
                        occurred_at=datetime.now(timezone.utc),
                        raw_payload={
                            "asset": symbol, "trade_type": "LIQUIDATION",
                            "side": side, "price": price, "size_tokens": qty, "notional_usd": price * qty
                        }
                    )
                    # Send the event to the Kafka queue for the enrichment service to process.
                    producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
        except Exception as e:
            # If the connection drops, log the error, wait 5 seconds, and try to reconnect (Infinite retry).
            logger.error(f"Binance WS error: {e}. Reconnecting...")
            await asyncio.sleep(5)

async def stream_onchain_whales(producer: SentinelProducer, redis_client):
    if not ETH_WSS_URL: return
    
    # ── BLOCKCHAIN TARGETS ────────────────────────────────────────────────────
    # We only care about massive stablecoin movements, so we watch the smart contracts for USDT and USDC.
    CONTRACTS = ["0xdac17f958d2ee523a2206206994597c13d831ec7", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]
    
    # This is the Keccak-256 hash of the "Transfer(address,address,uint256)" event signature.
    # It tells the Ethereum node: "Only send me logs when a token is transferred."
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    loop = asyncio.get_event_loop()

    while True:
        try:
            suspects_for_log = await loop.run_in_executor(None, redis_client.smembers, "sentinel:watched:wallets")
            logger.info(f"Heartbeat | Connecting to ETH RPC. Tracking {len(CONTRACTS)} contracts, {len(suspects_for_log)} wallets.")

            async with websockets.connect(ETH_WSS_URL, ping_interval=30) as ws:
                # Subscribe to the Ethereum node using JSON-RPC format.
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", 
                                          "params": ["logs", {"address": CONTRACTS, "topics": [TRANSFER_TOPIC]}]}))
                
                logger.info("Connected to Ethereum RPC Whale Tracker")
                while True:
                    message = await ws.recv()
                    log = json.loads(message).get("params", {}).get("result", {})
                    
                    # Check if the log is valid and contains enough "topics" (indexed parameters).
                    if log and log.get("data") != "0x" and len(log.get("topics", [])) >= 3:
                        # Ethereum addresses in logs are padded with zeros to 32 bytes.
                        # We slice [26:] to grab just the actual 40-character wallet address.
                        sender = "0x" + log["topics"][1][26:]
                        receiver = "0x" + log["topics"][2][26:]
                        # "data" contains the amount transferred in Hexadecimal. We convert it to an integer.
                        amount_usd = int(log["data"], 16) / (10 ** 6) # USDT/USDC are 6 decimals
                        
                        # 1. DYNAMIC CHECK: Is this a suspect wallet flagged by the Reasoning agent?
                        # We use 'run_in_executor' so this synchronous Redis call doesn't freeze our WebSockets.
                        suspects = await loop.run_in_executor(None, redis_client.smembers, "sentinel:watched:wallets")
                        is_suspect = sender in suspects or receiver in suspects

                        # We only care if it's a massive transfer OR if it involves a known bad actor.
                        if amount_usd >= WHALE_THRESHOLD_USD or is_suspect:
                            token = "USDT" if log.get("address").lower() == CONTRACTS[0] else "USDC"
                            logger.info(f"🐋 WHALE/SUSPECT TX | ${amount_usd:,.2f} {token} | {sender} -> {receiver}")
                            
                            event = RawEvent(
                                source="ethereum_rpc",
                                occurred_at=datetime.now(timezone.utc),
                                raw_payload={
                                    "asset": token, "trade_type": "WHALE_TRANSFER",
                                    "notional_usd": amount_usd, "sender_wallet": sender, 
                                    "receiver_wallet": receiver, "is_suspect_wallet": is_suspect
                                }
                            )
                            producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=token)
        except Exception as e:
            logger.error(f"ETH RPC error: {e}. Reconnecting...")
            await asyncio.sleep(5)

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL Crypt0 Service")
    logger.info("=" * 60)
    producer = SentinelProducer()
    redis_client = get_redis()
    try:
        logger.info("Starting Crypto Collector")
        # asyncio.gather runs both infinite loops side-by-side at the exact same time.
        await asyncio.gather(
            stream_binance_liquidations(producer),
            stream_onchain_whales(producer, redis_client)
        )
    finally:
        producer.close()

if __name__ == "__main__":
    asyncio.run(main())