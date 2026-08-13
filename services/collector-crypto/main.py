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

from shared.utils.logging import setup_sentinel_logging

logger = setup_sentinel_logging("collector.crypto", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

ETH_WSS_URL = os.getenv("ETH_RPC_WSS_URL") or os.getenv("ETH_WSS_URL")
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
    Consolidates the Tape Reader (Large Trades) and Market Structure (1m Candles)
    into a single efficient WebSocket connection using the 'market_trades' channel.
    """
    from shared.utils.websocket import ResilientWebSocketClient

    subscribe_msg = {
        "type": "subscribe",
        "product_ids": COINBASE_PRODUCTS,
        "channel": "market_trades"
    }

    candles = {p: {"o": None, "h": 0, "l": float('inf'), "c": 0, "v": 0} for p in COINBASE_PRODUCTS}
    msg_count = 0

    async def on_connect(ws):
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"Connected to Coinbase Advanced Trade WS -> {len(COINBASE_PRODUCTS)} pairs")

    async def on_message(raw_msg):
        nonlocal msg_count
        try:
            data = json.loads(raw_msg)
            if data.get("channel") == "market_trades":
                for event in data.get("events", []):
                    for trade in event.get("trades", []):
                        symbol = trade["product_id"]
                        price = float(trade["price"])
                        qty = float(trade["size"])
                        side = trade["side"]
                        notional = price * qty

                        c = candles[symbol]
                        if c["o"] is None: c["o"] = price
                        c["h"] = max(c["h"], price)
                        c["l"] = min(c["l"], price)
                        c["c"] = price
                        c["v"] += qty

                        msg_count += 1
                        if msg_count % 5000 == 0:
                            logger.info(f"💓 Spot Heartbeat: Processed {msg_count} live Coinbase trades.")

                        if notional >= WHALE_THRESHOLD_USD:
                            raw_event = RawEvent(
                                source="coinbase_spot", occurred_at=datetime.now(timezone.utc),
                                raw_payload={
                                    "asset": symbol.replace("-USD", "USDT").lower(), 
                                    "trade_type": "LARGE_SPOT_TRADE",
                                    "side": side, "price": price, 
                                    "size_tokens": qty, "notional_usd": notional
                                }
                            )
                            await producer.send(Topics.RAW_CRYPTO, raw_event.model_dump(), key=symbol)
        except Exception as e:
            logger.error(f"Error handling Coinbase WS message: {e}")

    async def candle_emitter():
        while True:
            await asyncio.sleep(60.0)
            try:
                for sym, c in list(candles.items()):
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
                        await producer.send(Topics.RAW_CRYPTO, raw_event.model_dump(), key=sym)
                        candles[sym] = {"o": None, "h": 0, "l": float('inf'), "c": 0, "v": 0}
            except Exception as err:
                logger.error(f"Coinbase candle emitter error: {err}")

    client = ResilientWebSocketClient(
        url=COINBASE_WS_URL,
        name="Coinbase_Spot",
        ping_interval=20.0,
        on_connect=on_connect,
        on_message=on_message
    )
    await client.start()
    asyncio.create_task(candle_emitter())

    while True:
        await asyncio.sleep(3600)


# ── 2. BINANCE FUTURES LIQUIDATIONS ───────────────────────────────────────────

async def stream_binance_liquidations(producer: SentinelProducer):
    from shared.utils.websocket import ResilientWebSocketClient
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    msg_count = 0

    async def on_message(raw_msg):
        nonlocal msg_count
        try:
            data = json.loads(raw_msg)
            order = data.get("o", {})
            if not order:
                return

            symbol = order.get("s", "")
            side = order.get("S", "")
            price = float(order.get("p", 0))
            qty = float(order.get("q", 0))
            
            msg_count += 1
            if msg_count % 500 == 0:
                logger.info(f"💓 Liq Heartbeat: Processed {msg_count} liquidation events.")

            event = RawEvent(
                source="binance_futures", occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "asset": symbol.lower(), 
                    "trade_type": "LIQUIDATION", "side": side, 
                    "price": price, "size_tokens": qty, "notional_usd": price * qty
                }
            )
            await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
        except Exception as e:
            logger.error(f"Error handling Binance Liquidation WS message: {e}")

    client = ResilientWebSocketClient(
        url=url,
        name="Binance_Liquidations",
        ping_interval=20.0,
        on_message=on_message
    )
    await client.start()

    while True:
        await asyncio.sleep(3600)


# ── 3. BINANCE FUTURES FUNDING RATES (!markPrice@arr@1s) ──────────────────────

# Dynamically tracked perp symbols observed from the markPrice stream.
# Used by the OI REST poller so no hardcoded symbol list is needed.
_observed_perp_symbols: set = set()

async def stream_binance_funding_rates(producer: SentinelProducer, redis_client):
    """
    Streams Binance Futures !markPrice@arr@1s for funding rate, mark price,
    index price, and computes perp-spot basis in bps.  Uses the /market routed
    endpoint (same tier as the liquidation stream).
    """
    from shared.utils.websocket import ResilientWebSocketClient

    url = "wss://fstream.binance.com/ws/!markPrice@arr"
    msg_count = 0
    # In-memory statistics cache: symbol -> {"mean": float, "var": float}
    # Eliminates 750+ Redis network round-trips/sec while preserving exact math
    funding_stats_cache = {}

    FUNDING_EMA_ALPHA = float(os.getenv("FUNDING_EMA_ALPHA", "0.05"))
    FUNDING_ZSCORE_TRIGGER = float(os.getenv("FUNDING_ZSCORE_TRIGGER", "2.0"))

    async def on_message(raw_msg):
        nonlocal msg_count
        try:
            items = json.loads(raw_msg)
            if not isinstance(items, list):
                items = [items]

            for data in items:
                if data.get("e") != "markPriceUpdate":
                    continue

                symbol = data.get("s", "")
                if not symbol:
                    continue

                # Track observed perp symbols dynamically for OI poller
                _observed_perp_symbols.add(symbol)

                try:
                    funding_rate = float(data.get("r", "0"))
                    mark_price = float(data.get("p", "0"))
                    index_price = float(data.get("i", "0"))
                    next_funding_time = int(data.get("T", 0))
                except (ValueError, TypeError):
                    continue

                if mark_price <= 0 or index_price <= 0:
                    continue

                basis_bps = ((mark_price - index_price) / index_price) * 10_000.0

                msg_count += 1
                if msg_count % 10000 == 0:
                    logger.info(f"💓 Funding Heartbeat: Processed {msg_count} markPrice updates across {len(_observed_perp_symbols)} symbols.")

                # Cache latest funding data per symbol in Redis for enricher/agent lookups
                funding_data = {
                    "funding_rate": funding_rate,
                    "mark_price": mark_price,
                    "index_price": index_price,
                    "basis_bps": round(basis_bps, 4),
                    "next_funding_time": next_funding_time,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                try:
                    await redis_client.raw.set(
                        f"sentinel:crypto:funding:{symbol}",
                        json.dumps(funding_data),
                        ex=3600,
                    )
                except Exception as re:
                    logger.debug(f"Redis funding cache write failed for {symbol}: {re}")

                # Determine if funding rate is extreme via in-memory EMA z-score (0ms overhead)
                is_extreme = False
                try:
                    if symbol not in funding_stats_cache:
                        funding_stats_cache[symbol] = {"mean": funding_rate, "var": 1e-10}

                    stats = funding_stats_cache[symbol]
                    ema_mean = stats["mean"]
                    ema_var = stats["var"]

                    std = max(abs(ema_var) ** 0.5, 1e-10)
                    z = (funding_rate - ema_mean) / std

                    # Update in-memory EMA statistics instantly
                    new_mean = FUNDING_EMA_ALPHA * funding_rate + (1 - FUNDING_EMA_ALPHA) * ema_mean
                    new_var = FUNDING_EMA_ALPHA * (funding_rate - ema_mean) ** 2 + (1 - FUNDING_EMA_ALPHA) * ema_var
                    funding_stats_cache[symbol] = {"mean": new_mean, "var": new_var}

                    is_extreme = abs(z) >= FUNDING_ZSCORE_TRIGGER
                except Exception:
                    pass

                # Only emit Kafka events for extreme funding rates to avoid flooding
                if is_extreme:
                    event = RawEvent(
                        source="binance_futures",
                        occurred_at=datetime.now(timezone.utc),
                        raw_payload={
                            "asset": symbol.lower(),
                            "trade_type": "CRYPTO_PERP_FUNDING",
                            "funding_rate": funding_rate,
                            "mark_price": mark_price,
                            "index_price": index_price,
                            "basis_bps": round(basis_bps, 4),
                            "next_funding_time": next_funding_time,
                        },
                    )
                    await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
                    logger.info(
                        f"⚡ FUNDING RATE EXTREME | {symbol} | Rate: {funding_rate:.6f} | "
                        f"Basis: {basis_bps:.2f}bps | Mark: {mark_price:.2f} | Index: {index_price:.2f}"
                    )

        except Exception as e:
            logger.error(f"Error handling Binance markPrice WS message: {e}")

    client = ResilientWebSocketClient(
        url=url,
        name="Binance_FundingRates",
        ping_interval=20.0,
        on_message=on_message,
    )
    await client.start()

    while True:
        await asyncio.sleep(3600)


# ── 4. BINANCE FUTURES OPEN INTEREST (REST POLLER) ────────────────────────────

async def poll_binance_open_interest(producer: SentinelProducer, redis_client):
    """
    REST poller for Binance Futures /fapi/v1/openInterest.
    Polls symbols discovered dynamically from the markPrice stream.
    Cadence is configurable via BINANCE_OI_POLL_SECONDS env var (default 300s/5min).
    """
    import aiohttp

    poll_interval = int(os.getenv("BINANCE_OI_POLL_SECONDS", "300"))
    base_url = "https://fapi.binance.com/fapi/v1/openInterest"

    # Wait for the funding stream to discover some symbols before starting
    await asyncio.sleep(30)

    session_timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            try:
                # Use dynamically observed symbols, not a hardcoded list
                symbols = list(_observed_perp_symbols)
                if not symbols:
                    logger.info("OI Poller: No perp symbols observed yet, waiting...")
                    await asyncio.sleep(60)
                    continue

                # Prioritize top symbols by fetching latest funding rate magnitude from Redis
                scored = []
                for sym in symbols:
                    try:
                        cached = await redis_client.raw.get(f"sentinel:crypto:funding:{sym}")
                        if cached:
                            fr = abs(json.loads(cached).get("funding_rate", 0))
                        else:
                            fr = 0.0
                    except Exception:
                        fr = 0.0
                    scored.append((sym, fr))
                scored.sort(key=lambda x: x[1], reverse=True)

                # Cap per-cycle to top 50 most active to respect API rate limits
                poll_batch = [s[0] for s in scored[:50]]

                count = 0
                for symbol in poll_batch:
                    try:
                        async with session.get(base_url, params={"symbol": symbol}) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                oi_value = float(data.get("openInterest", "0"))
                                if oi_value > 0:
                                    # Cache in Redis for enricher access
                                    await redis_client.raw.set(
                                        f"sentinel:crypto:oi:{symbol}",
                                        str(oi_value),
                                        ex=600,
                                    )
                                    event = RawEvent(
                                        source="binance_futures",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "asset": symbol.lower(),
                                            "trade_type": "OPEN_INTEREST",
                                            "open_interest": oi_value,
                                            "symbol": symbol,
                                        },
                                    )
                                    await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
                                    count += 1
                            elif resp.status == 429:
                                logger.warning("OI Poller: Binance rate limited, backing off 60s.")
                                await asyncio.sleep(60)
                                break
                            else:
                                text = await resp.text()
                                logger.debug(f"OI poll {symbol} returned {resp.status}: {text[:100]}")
                    except Exception as e:
                        logger.debug(f"OI poll error for {symbol}: {e}")

                    # Small delay between requests to respect rate limits
                    await asyncio.sleep(0.2)

                if count > 0:
                    logger.info(f"📊 OI Poller: Published {count}/{len(poll_batch)} open interest snapshots.")

            except Exception as e:
                logger.error(f"OI Poller cycle error: {e}", exc_info=True)

            await asyncio.sleep(poll_interval)


# ── 5. ON-CHAIN WHALE TRACKING ────────────────────────────────────────────────

async def stream_onchain_whales(producer: SentinelProducer, redis_client):
    from shared.utils.websocket import ResilientWebSocketClient
    if not ETH_WSS_URL: 
        logger.warning("ETH_RPC_WSS_URL missing. On-chain tracking disabled.")
        return
        
    CONTRACTS = ["0xdac17f958d2ee523a2206206994597c13d831ec7", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    msg_count = 0

    async def on_connect(ws):
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", 
            "params": ["logs", {"address": CONTRACTS, "topics": [TRANSFER_TOPIC]}]
        }))
        logger.info("Connected to Ethereum RPC Whale Tracker")

    async def on_message(raw_msg):
        nonlocal msg_count
        try:
            log = json.loads(raw_msg).get("params", {}).get("result", {})
            if log and log.get("data") != "0x" and len(log.get("topics", [])) >= 3:
                sender = "0x" + log["topics"][1][26:]
                receiver = "0x" + log["topics"][2][26:]
                amount_usd = int(log["data"], 16) / (10 ** 6)
                
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
                    await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=token)
        except Exception as e:
            logger.error(f"Error handling Ethereum logs WS message: {e}")

    client = ResilientWebSocketClient(
        url=ETH_WSS_URL,
        name="Ethereum_Whales",
        ping_interval=30.0,
        on_connect=on_connect,
        on_message=on_message
    )
    await client.start()

    while True:
        await asyncio.sleep(3600)


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
        # Run all WebSocket streams and REST pollers concurrently 
        await asyncio.gather(
            stream_coinbase_market_data(producer),
            stream_binance_liquidations(producer),
            stream_binance_funding_rates(producer, redis_client),
            poll_binance_open_interest(producer, redis_client),
            stream_onchain_whales(producer, redis_client),
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