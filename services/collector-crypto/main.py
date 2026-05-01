"""
services/collector-crypto/main.py

ENTERPRISE CRYPTO COLLECTOR
===========================
Sources: 
1. Binance Spot (Large Trade Tape Reader - Top 10 Coins)
2. Binance Futures (Liquidations)
3. Binance Spot (1m OHLCV Candles - Market Structure)
4. Ethereum RPC (Mempool Whale Tracking)
"""

import asyncio
import json
import logging
import os
import sys
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
# Lowered threshold to act as a noise-filter. ML Enrichment handles true anomaly detection.
WHALE_THRESHOLD_USD = 50_000

# ── 1. SPOT MARKET TAPE READER (LARGE TRADES) ─────────────────────────────────

async def stream_binance_large_trades(producer: SentinelProducer):
    TOP_10_PAIRS = [
        "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", 
        "dogeusdt", "adausdt", "avaxusdt", "dotusdt", "linkusdt"
    ]
    stream_params = "/".join([f"{pair}@aggTrade" for pair in TOP_10_PAIRS])
    url = f"wss://stream.binance.us:9443/stream?streams={stream_params}"
    
    msg_count = 0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Binance Large Trade Stream")
                while True:
                    payload = json.loads(await ws.recv())
                    data = payload.get("data", {})
                    if not data: continue
                        
                    symbol = data.get("s", "UNKNOWN")
                    price = float(data.get("p", 0.0))
                    qty = float(data.get("q", 0.0))
                    is_market_maker = data.get("m", False)
                    notional_usd = price * qty

                    # HEARTBEAT LOGGING: Used to prove the stream is alive without spamming logs
                    msg_count += 1
                    if msg_count % 5000 == 0:
                        logger.info(f"💓 Spot Heartbeat: Processed {msg_count} live trades.")
                    
                    if notional_usd >= WHALE_THRESHOLD_USD:
                        side = "SELL" if is_market_maker else "BUY"
                        event = RawEvent(
                            source="binance_spot", occurred_at=datetime.now(timezone.utc),
                            raw_payload={
                                "asset": symbol, "trade_type": "LARGE_SPOT_TRADE",
                                "side": side, "price": price, "size_tokens": qty, "notional_usd": notional_usd
                            }
                        )
                        producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
        except Exception as e:
            logger.error(f"Binance Spot WS error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# ── 2. OHLCV MARKET CANDLES ───────────────────────────────────────────────────

async def stream_binance_candles(producer: SentinelProducer):
    TOP_10_PAIRS = [
        "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", 
        "dogeusdt", "adausdt", "avaxusdt", "dotusdt", "linkusdt"
    ]
    stream_params = "/".join([f"{pair}@kline_1m" for pair in TOP_10_PAIRS])
    url = f"wss://stream.binance.us:9443/stream?streams={stream_params}"
    
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Binance 1m Candle Stream")
                while True:
                    payload = json.loads(await ws.recv())
                    data = payload.get("data", {})
                    if not data: continue
                    
                    kline = data.get("k", {})
                    # Only send data when the 1-minute candle physically closes
                    if kline.get("x", False):  
                        symbol = data.get("s")
                        event = RawEvent(
                            source="binance_candles", occurred_at=datetime.now(timezone.utc),
                            raw_payload={
                                "asset": symbol, "trade_type": "OHLCV",
                                "open": float(kline.get("o")), "high": float(kline.get("h")),
                                "low": float(kline.get("l")), "close": float(kline.get("c")),
                                "volume": float(kline.get("v"))
                            }
                        )
                        producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
        except Exception as e:
            logger.error(f"Binance Candle WS error: {e}. Reconnecting...")
            await asyncio.sleep(5)

# ── 3. FUTURES LIQUIDATIONS ───────────────────────────────────────────────────

async def stream_binance_liquidations(producer: SentinelProducer):
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    msg_count = 0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Binance Liquidations")
                while True:
                    data = json.loads(await ws.recv())
                    order = data.get("o", {})
                    symbol, side, price, qty = order.get("s"), order.get("S"), float(order.get("p", 0)), float(order.get("q", 0))
                    
                    msg_count += 1
                    if msg_count % 50 == 0:
                        logger.info(f"💓 Liq Heartbeat: Processed {msg_count} liquidation events.")

                    event = RawEvent(
                        source="binance_futures", occurred_at=datetime.now(timezone.utc),
                        raw_payload={
                            "asset": symbol, "trade_type": "LIQUIDATION", "side": side, 
                            "price": price, "size_tokens": qty, "notional_usd": price * qty
                        }
                    )
                    producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
        except Exception as e:
            logger.error(f"Binance Liq WS error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# ── 4. ON-CHAIN WHALE TRACKING ────────────────────────────────────────────────

async def stream_onchain_whales(producer: SentinelProducer, redis_client):
    if not ETH_WSS_URL: 
        logger.warning("ETH_RPC_WSS_URL environment variable is missing. On-chain whale tracking disabled.")
        return
        
    CONTRACTS = ["0xdac17f958d2ee523a2206206994597c13d831ec7", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    loop = asyncio.get_event_loop()
    msg_count = 0
    while True:
        try:
            async with websockets.connect(ETH_WSS_URL, ping_interval=30) as ws:
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["logs", {"address": CONTRACTS, "topics": [TRANSFER_TOPIC]}]}))
                logger.info("Connected to Ethereum RPC Whale Tracker")
                while True:
                    message = await ws.recv()
                    log = json.loads(message).get("params", {}).get("result", {})
                    
                    if log and log.get("data") != "0x" and len(log.get("topics", [])) >= 3:
                        sender = "0x" + log["topics"][1][26:]
                        receiver = "0x" + log["topics"][2][26:]
                        amount_usd = int(log["data"], 16) / (10 ** 6)
                        
                        raw_suspects = await loop.run_in_executor(None, redis_client.smembers, "sentinel:watched:wallets")
                        # BUG FIX: Safely decode bytes into utf-8 strings for strict matching
                        suspects = {s.decode('utf-8') if isinstance(s, bytes) else s for s in raw_suspects} if raw_suspects else set()
                        
                        is_suspect = sender in suspects or receiver in suspects

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
                            producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=token)
        except Exception as e:
            logger.error(f"ETH RPC error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL CRYPTO COLLECTOR ONLINE")
    logger.info("=" * 60)
    
    producer = SentinelProducer()
    redis_client = get_redis()
    try:
        # Run all 4 WebSocket streams simultaneously 
        await asyncio.gather(
            stream_binance_large_trades(producer),
            stream_binance_liquidations(producer),
            stream_binance_candles(producer),
            stream_onchain_whales(producer, redis_client)
        )
    finally:
        producer.close()

if __name__ == "__main__":
    asyncio.run(main())