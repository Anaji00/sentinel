"""
services/collector-crypto/main.py

ENTERPRISE CRYPTO COLLECTOR (HYBRID EDITION)
==============================================
Sources: 
1. Coinbase Advanced Trade (Large Trade Tape Reader - Top 10 Coins)
2. Coinbase Advanced Trade (1m OHLCV Candles - Market Structure)
3. Binance Futures (Global Liquidations Firehose)
4. Ethereum RPC (Mempool Whale Tracking & Sanctioned Wallet Monitoring)
"""

import asyncio
import json
import logging
import os
import sys
import time
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
logger = logging.getLogger("collector.crypto")

ETH_WSS_URL = os.getenv("ETH_RPC_WSS_URL")
WHALE_THRESHOLD_USD = 250_000

# Coinbase Advanced Trade WebSocket URI
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
COINBASE_PRODUCTS = [
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", 
    "ADA-USD", "AVAX-USD", "DOT-USD", "LINK-USD", "BCH-USD"
]

# ── 1. COINBASE SPOT TRADES & OHLCV CANDLES ───────────────────────────────────

async def stream_coinbase_market_data(producer: SentinelProducer):
    """
    Coinbase API Architecture: 
    Consolidates the Tape Reader (Large Trades) and Market Structure (1m Candles)
    into a single efficient WebSocket connection using the 'market_trades' channel.
    """
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": COINBASE_PRODUCTS,
        "channel": "market_trades"
    }

    # In-memory aggregator for 1-minute candles
    candles = {p: {"o": None, "h": 0, "l": float('inf'), "c": 0, "v": 0} for p in COINBASE_PRODUCTS}
    last_candle_emit = time.time()
    msg_count = 0

    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL, ping_interval=20) as ws:
                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"Connected to Coinbase Advanced Trade WS -> {len(COINBASE_PRODUCTS)} pairs")
                
                while True:
                    try:
                        # Use wait_for so the loop continues ticking even if trades are slow,
                        # ensuring our 60-second candle emitter fires accurately on time.
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        data = json.loads(msg)

                        # Coinbase Schema Routing
                        if data.get("channel") == "market_trades":
                            for event in data.get("events", []):
                                for trade in event.get("trades", []):
                                    symbol = trade["product_id"]
                                    price = float(trade["price"])
                                    qty = float(trade["size"])
                                    side = trade["side"]
                                    notional = price * qty

                                    # 1. Update In-Memory Candle
                                    c = candles[symbol]
                                    if c["o"] is None: c["o"] = price
                                    c["h"] = max(c["h"], price)
                                    c["l"] = min(c["l"], price)
                                    c["c"] = price
                                    c["v"] += qty

                                    # Heartbeat Logging
                                    msg_count += 1
                                    if msg_count % 5000 == 0:
                                        logger.info(f"💓 Spot Heartbeat: Processed {msg_count} live Coinbase trades.")

                                    # 2. Large Trade Check (Tape Reader)
                                    if notional >= WHALE_THRESHOLD_USD:
                                        raw_event = RawEvent(
                                            source="coinbase_spot", occurred_at=datetime.now(timezone.utc),
                                            raw_payload={
                                                # Normalize Coinbase "BTC-USD" to standard "btcusdt" format
                                                "asset": symbol.replace("-USD", "USDT").lower(), 
                                                "trade_type": "LARGE_SPOT_TRADE",
                                                "side": side, "price": price, 
                                                "size_tokens": qty, "notional_usd": notional
                                            }
                                        )
                                        # [CRITICAL FIX]: Await the Kafka producer
                                        await producer.send(Topics.RAW_CRYPTO, raw_event.model_dump(), key=symbol)

                    except asyncio.TimeoutError:
                        pass # Normal timeout, loop continues to check the candle timer

                    # 3. Emit OHLCV Candles every 60 seconds
                    now = time.time()
                    if now - last_candle_emit >= 60.0:
                        for sym, c in candles.items():
                            if c["o"] is not None:
                                raw_event = RawEvent(
                                    source="coinbase_candles", occurred_at=datetime.now(timezone.utc),
                                    raw_payload={
                                        "asset": sym.replace("-USD", "USDT").lower(), 
                                        "trade_type": "OHLCV",
                                        "open": c["o"], "high": c["h"],
                                        "low": c["l"], "close": c["c"],
                                        "volume": c["v"]
                                    }
                                )
                                # [CRITICAL FIX]: Await the Kafka producer
                                await producer.send(Topics.RAW_CRYPTO, raw_event.model_dump(), key=sym)
                                
                                # Reset candle for the next minute
                                candles[sym] = {"o": None, "h": 0, "l": float('inf'), "c": 0, "v": 0}
                        
                        last_candle_emit = now

        except Exception as e:
            logger.error(f"Coinbase WS error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


# ── 2. BINANCE FUTURES LIQUIDATIONS ───────────────────────────────────────────

async def stream_binance_liquidations(producer: SentinelProducer):
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    msg_count = 0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Binance Liquidations (Futures)")
                while True:
                    data = json.loads(await ws.recv())
                    order = data.get("o", {})
                    if not order:
                        continue

                    symbol = order.get("s", "")
                    side = order.get("S", "")
                    price = float(order.get("p", 0))
                    qty = float(order.get("q", 0))
                    
                    msg_count += 1
                    if msg_count % 50 == 0:
                        logger.info(f"💓 Liq Heartbeat: Processed {msg_count} liquidation events.")

                    event = RawEvent(
                        source="binance_futures", occurred_at=datetime.now(timezone.utc),
                        raw_payload={
                            # Normalize Binance 'BTCUSDT' to match downstream 'btcusdt' correlation models
                            "asset": symbol.lower(), 
                            "trade_type": "LIQUIDATION", "side": side, 
                            "price": price, "size_tokens": qty, "notional_usd": price * qty
                        }
                    )
                    # [CRITICAL FIX]: Await the Kafka producer
                    await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
        except Exception as e:
            logger.error(f"Binance Liq WS error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


# ── 3. ON-CHAIN WHALE TRACKING ────────────────────────────────────────────────

async def stream_onchain_whales(producer: SentinelProducer, redis_client):
    if not ETH_WSS_URL: 
        logger.warning("ETH_RPC_WSS_URL missing. On-chain tracking disabled.")
        return
        
    CONTRACTS = ["0xdac17f958d2ee523a2206206994597c13d831ec7", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    msg_count = 0
    while True:
        try:
            async with websockets.connect(ETH_WSS_URL, ping_interval=30) as ws:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", 
                    "params": ["logs", {"address": CONTRACTS, "topics": [TRANSFER_TOPIC]}]
                }))
                logger.info("Connected to Ethereum RPC Whale Tracker")
                
                while True:
                    message = await ws.recv()
                    log = json.loads(message).get("params", {}).get("result", {})
                    
                    if log and log.get("data") != "0x" and len(log.get("topics", [])) >= 3:
                        sender = "0x" + log["topics"][1][26:]
                        receiver = "0x" + log["topics"][2][26:]
                        amount_usd = int(log["data"], 16) / (10 ** 6)
                        
                        # [CRITICAL FIX]: Native Async Redis Checking using gather.
                        is_sender_suspect, is_receiver_suspect = await asyncio.gather(
                            redis_client.raw.sismember("sentinel:watched:wallets", sender),
                            redis_client.raw.sismember("sentinel:watched:wallets", receiver)
                        )
                        is_suspect = is_sender_suspect or is_receiver_suspect

                        msg_count += 1
                        if msg_count % 500 == 0:
                            logger.info(f"💓 ETH Heartbeat: Evaluated {msg_count} Stablecoin transfers.")

                        if amount_usd >= WHALE_THRESHOLD_USD or is_suspect:
                            token = "USDT" if log.get("address").lower() == CONTRACTS[0] else "USDC"
                            event = RawEvent(
                                source="ethereum_rpc", occurred_at=datetime.now(timezone.utc),
                                raw_payload={
                                    "asset": token, "trade_type": "WHALE_TRANSFER", "notional_usd": amount_usd, 
                                    "sender_wallet": sender, "receiver_wallet": receiver, "is_suspect_wallet": is_suspect
                                }
                            )
                            # [CRITICAL FIX]: Await the Kafka producer
                            await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=token)
                            
        except Exception as e:
            logger.error(f"ETH RPC error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL CRYPTO COLLECTOR ONLINE (HYBRID EDITION)")
    logger.info("=" * 60)
    
    producer = SentinelProducer()
    
    # Correctly await Kafka and Redis initialization
    await producer.start()
    redis_client = await get_redis()
    
    try:
        # Run all three WebSocket streams concurrently 
        await asyncio.gather(
            stream_coinbase_market_data(producer),
            stream_binance_liquidations(producer),
            stream_onchain_whales(producer, redis_client)
        )
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Await the socket closures safely
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())