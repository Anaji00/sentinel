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

import aiohttp
import asyncio
import json
import logging
import os
import sys
import time
import websockets
from datetime import datetime, timezone
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.utils.heartbeat import start_heartbeat_task

from shared.utils.logging import setup_sentinel_logging
from shared.utils.collector_metrics import CollectorMetrics

logger = setup_sentinel_logging("collector.crypto", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

ETH_WSS_URL = os.getenv("ETH_RPC_WSS_URL") or os.getenv("ETH_WSS_URL") or "wss://ethereum-rpc.publicnode.com"
ARB_WSS_URL = os.getenv("ARB_RPC_WSS_URL") or os.getenv("ARB_WSS_URL") or "wss://arbitrum-one-rpc.publicnode.com"
BASE_WSS_URL = os.getenv("BASE_RPC_WSS_URL") or os.getenv("BASE_WSS_URL") or "wss://base-rpc.publicnode.com"

# Tried in order when the configured endpoint cannot be reached. Each was
# verified from this host: ethereum.publicnode.com and eth.drpc.org both
# answered and agreed on block 25821116, while ethereum-rpc.publicnode.com,
# eth.llamarpc.com and mainnet.gateway.tenderly.co all failed the TLS handshake.
RPC_FALLBACKS = {
    "ethereum": ["wss://ethereum.publicnode.com", "wss://eth.drpc.org"],
    "arbitrum": ["wss://arbitrum-one-rpc.publicnode.com", "wss://arbitrum.drpc.org"],
    "base": ["wss://base-rpc.publicnode.com", "wss://base.drpc.org"],
}

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


# ── 3b. OKX PERPETUAL SURFACE (REST POLLER) ───────────────────────────────────
#
# Binance answers HTTP 451 -- "unavailable for legal reasons" -- to this host, on
# both fstream (websocket) and fapi (REST). The funding stream above therefore
# never connected, and because it is the only thing that fills
# _observed_perp_symbols, the open-interest poller sat on an empty set logging
# "No perp symbols observed yet" once a minute indefinitely.
#
# The measurable consequence: across 24 hours, every one of 34,000 stored crypto
# events had funding_rate, open_interest, basis_bps, mark_price and leverage
# null. The fields were declared on CryptoData, rendered by the frontend, and
# never once populated.
#
# OKX serves the same surface from here (checked: funding-rate, mark-price,
# open-interest and index-tickers all 200, 453 SWAP instruments).

OKX_BASE = "https://www.okx.com/api/v5"
OKX_POLL_INTERVAL_SEC = int(os.getenv("OKX_POLL_INTERVAL_SEC", "300"))
OKX_TOP_N = int(os.getenv("OKX_PERP_SYMBOLS", "12"))

# Emit only when funding moves enough to mean something. OKX pays funding every
# eight hours, so polling every five minutes mostly re-reads the same number;
# without a gate this would republish an unchanged rate ~96 times per interval.
OKX_FUNDING_DELTA_TRIGGER = float(os.getenv("OKX_FUNDING_DELTA_TRIGGER", "0.00005"))


async def _okx_get(session, path: str, params: str = "") -> list:
    """One OKX v5 read. Returns [] rather than raising: a poller that dies on a
    transient 5xx takes the whole perp surface down with it."""
    url = f"{OKX_BASE}{path}{params}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                logger.warning("OKX %s -> HTTP %s", path, resp.status)
                return []
            body = await resp.json()
            if str(body.get("code")) != "0":
                logger.warning("OKX %s -> code %s %s", path, body.get("code"), body.get("msg"))
                return []
            return body.get("data") or []
    except Exception as e:
        logger.warning("OKX %s failed: %s", path, e)
        return []


def _f(value, default=0.0) -> float:
    """OKX returns every number as a string, and empty string for 'not set'."""
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


async def poll_okx_perpetuals(producer: SentinelProducer, redis_client):
    """Funding rate, mark/index price, basis and open interest for OKX swaps."""
    last_funding: dict = {}

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                tickers = await _okx_get(session, "/market/tickers", "?instType=SWAP")
                # Rank by quote volume so the poll spends its budget on the
                # instruments anyone is actually trading.
                usdt = [t for t in tickers if str(t.get("instId", "")).endswith("-USDT-SWAP")]
                # Ranked by USD notional, not volCcy24h. volCcy24h counts base
                # units, so a token priced at $0.00000001265 reports 100 trillion
                # of them and sorts above everything: the first run of this poller
                # tracked SATS, PEPE, SHIB, BONK and FLOKI while ignoring BTC and
                # ETH. Multiplying by last price gives ETH $7.97B, BTC $5.26B,
                # SOL $0.96B -- the instruments a desk actually watches.
                usdt.sort(key=lambda t: _f(t.get("volCcy24h")) * _f(t.get("last")), reverse=True)
                top = usdt[:OKX_TOP_N]
                if not top:
                    await asyncio.sleep(OKX_POLL_INTERVAL_SEC)
                    continue

                marks = {m.get("instId"): _f(m.get("markPx"))
                         for m in await _okx_get(session, "/public/mark-price", "?instType=SWAP")}
                ois = {o.get("instId"): o
                       for o in await _okx_get(session, "/public/open-interest", "?instType=SWAP")}

                emitted = 0
                for ticker in top:
                    inst = ticker.get("instId")
                    if not inst:
                        continue

                    funding_rows = await _okx_get(session, "/public/funding-rate", f"?instId={inst}")
                    if not funding_rows:
                        continue
                    funding = funding_rows[0]
                    funding_rate = _f(funding.get("fundingRate"))

                    mark_price = marks.get(inst) or _f(ticker.get("last"))
                    index_rows = await _okx_get(
                        session, "/market/index-tickers", f"?instId={inst.replace('-SWAP', '')}"
                    )
                    index_price = _f(index_rows[0].get("idxPx")) if index_rows else 0.0

                    # Perp premium over spot. Undefined without an index price,
                    # so it is left at zero rather than divided by nothing.
                    basis_bps = (
                        ((mark_price - index_price) / index_price) * 10000.0
                        if index_price > 0 else 0.0
                    )

                    oi_row = ois.get(inst) or {}
                    open_interest = _f(oi_row.get("oi"))
                    open_interest_usd = _f(oi_row.get("oiUsd"))

                    payload = {
                        "funding_rate": funding_rate,
                        "mark_price": mark_price,
                        "index_price": index_price,
                        "basis_bps": round(basis_bps, 4),
                        "open_interest": open_interest,
                        "open_interest_usd": open_interest_usd,
                        "next_funding_time": int(_f(funding.get("fundingTime"))),
                        "max_funding_rate": _f(funding.get("maxFundingRate")),
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                    asset = inst.replace("-USDT-SWAP", "").upper()
                    try:
                        await redis_client.raw.set(
                            f"sentinel:crypto:funding:{inst}", json.dumps(payload), ex=3600
                        )
                        # Also under the bare asset. QuantTradingEngine's
                        # _fetch_funding_context() looks up "BTC" then "BTCUSDT"
                        # -- Binance's naming -- so a key written only as
                        # "BTC-USDT-SWAP" is invisible to the one agent that
                        # reads funding at all.
                        await redis_client.raw.set(
                            f"sentinel:crypto:funding:{asset}", json.dumps(payload), ex=3600
                        )
                        if open_interest:
                            # The key the enricher and other consumers already
                            # read; it was only ever written by the Binance
                            # poller, which cannot reach its API from here.
                            await redis_client.raw.set(
                                f"sentinel:crypto:oi:{asset}", str(open_interest), ex=3600
                            )
                    except Exception as re:
                        logger.debug("Redis funding cache write failed for %s: %s", inst, re)

                    previous = last_funding.get(inst)
                    moved = previous is None or abs(funding_rate - previous) >= OKX_FUNDING_DELTA_TRIGGER
                    last_funding[inst] = funding_rate
                    if not moved:
                        continue

                    event = RawEvent(
                        source="okx_swap",
                        occurred_at=datetime.now(timezone.utc),
                        raw_payload={
                            "asset": asset.lower(),
                            "pair": inst,
                            "trade_type": "CRYPTO_PERP_FUNDING",
                            **payload,
                        },
                    )
                    await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=inst)
                    emitted += 1

                if emitted:
                    logger.info(
                        "⚡ OKX PERP SURFACE | %s/%s instruments moved | top=%s",
                        emitted, len(top), top[0].get("instId"),
                    )
            except Exception as e:
                logger.error("OKX perpetual poller error: %s", e)

            await asyncio.sleep(OKX_POLL_INTERVAL_SEC)


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
                    logger.debug(
                    "OI Poller: no perp symbols observed yet. This is expected when "
                    "Binance is unavailable from this host (HTTP 451); OKX carries "
                    "open interest instead."
                )
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

# ── 5. MULTI-CHAIN ON-CHAIN WHALE TRACKING (§1.2) ───────────────────────────

async def _pick_reachable_rpc(chain_name: str, candidates: list) -> Optional[str]:
    """First RPC endpoint that completes a handshake and answers eth_blockNumber.

    Public RPC hosts go bad without notice and without changing their DNS.
    Measured here: wss://ethereum-rpc.publicnode.com began failing every
    handshake with [SSL: WRONG_VERSION_NUMBER] while arbitrum-one-rpc and
    base-rpc on the same provider stayed healthy -- so the Ethereum whale
    stream reconnected every 60 seconds and produced nothing, for as long as
    that host stayed broken, while the collector reported no errors of its own.

    A reachability check costs one round trip at startup and turns a silent
    permanent outage into a logged failover.
    """
    for url in [u for u in candidates if u]:
        try:
            async with websockets.connect(url, open_timeout=15, close_timeout=5) as ws:
                await ws.send(json.dumps(
                    {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
                ))
                reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                if reply.get("result"):
                    logger.info(
                        "%s RPC: using %s (block %s)",
                        chain_name.upper(), url, int(reply["result"], 16),
                    )
                    return url
        except Exception as e:
            logger.warning(
                "%s RPC endpoint unusable, trying next: %s (%s)",
                chain_name.upper(), url, type(e).__name__,
            )
    logger.error(
        "%s RPC: no reachable endpoint among %s candidates; this chain will be silent.",
        chain_name.upper(), len(candidates),
    )
    return None


async def _stream_chain_whales(chain_name: str, wss_url: str, contracts_map: dict, producer: SentinelProducer, redis_client):
    from shared.utils.websocket import ResilientWebSocketClient
    if not wss_url:
        return

    wss_url = await _pick_reachable_rpc(chain_name, [wss_url] + RPC_FALLBACKS.get(chain_name, []))
    if not wss_url:
        return

    contracts = list(contracts_map.keys())
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    msg_count = 0

    async def on_connect(ws):
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", 
            "params": ["logs", {"address": contracts, "topics": [TRANSFER_TOPIC]}]
        }))
        logger.info(f"Connected to {chain_name.upper()} RPC Whale Tracker ({len(contracts)} contracts)")

    async def on_message(raw_msg):
        nonlocal msg_count
        try:
            log = json.loads(raw_msg).get("params", {}).get("result", {})
            if log and log.get("data") != "0x" and len(log.get("topics", [])) >= 3:
                sender = "0x" + log["topics"][1][26:]
                receiver = "0x" + log["topics"][2][26:]
                addr_lower = log.get("address", "").lower()
                token_meta = contracts_map.get(addr_lower, {"symbol": "TOKEN", "decimals": 6})
                decimals = token_meta["decimals"]
                symbol = token_meta["symbol"]

                raw_val = int(log["data"], 16)
                amount = raw_val / (10 ** decimals)
                
                # Approximate USD value for stablecoins vs WBTC/ETH
                multiplier = 65000.0 if "BTC" in symbol else (3000.0 if "ETH" in symbol else 1.0)
                amount_usd = amount * multiplier
                
                is_sender_suspect, is_receiver_suspect = await asyncio.gather(
                    redis_client.raw.sismember("sentinel:watched:wallets", sender),
                    redis_client.raw.sismember("sentinel:watched:wallets", receiver)
                )
                is_suspect = is_sender_suspect or is_receiver_suspect

                msg_count += 1
                if msg_count % 250 == 0:
                    logger.info(f"💓 {chain_name.upper()} Heartbeat: Evaluated {msg_count} transfers.")

                if amount_usd >= WHALE_THRESHOLD_USD or is_suspect:
                    event = RawEvent(
                        source=f"{chain_name}_rpc",
                        occurred_at=datetime.now(timezone.utc),
                        raw_payload={
                            "asset": symbol,
                            "chain": chain_name,
                            "trade_type": "WHALE_TRANSFER",
                            "amount": amount,
                            "notional_usd": round(amount_usd, 2),
                            "sender_wallet": sender,
                            "receiver_wallet": receiver,
                            "is_suspect_wallet": is_suspect,
                        }
                    )
                    await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=symbol)
        except Exception as e:
            logger.debug(f"Error parsing {chain_name} log message: {e}")

    client = ResilientWebSocketClient(
        url=wss_url,
        name=f"{chain_name}_Whales",
        ping_interval=30.0,
        on_connect=on_connect,
        on_message=on_message
    )
    await client.start()
    while True:
        await asyncio.sleep(3600)


async def stream_onchain_whales(producer: SentinelProducer, redis_client):
    """Monitors whale transfers across Ethereum, Arbitrum, and Base."""
    eth_contracts = {
        "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "decimals": 6},
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6},
        "0x6b175474e89094c44da98b954eedeac495271d0f": {"symbol": "DAI", "decimals": 18},
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {"symbol": "WBTC", "decimals": 8},
    }
    arb_contracts = {
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": {"symbol": "USDT", "decimals": 6},
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831": {"symbol": "USDC", "decimals": 6},
        "0x912ce59144191c1204e64559fe8253a0e49e6548": {"symbol": "ARB", "decimals": 18},
    }
    base_contracts = {
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": {"symbol": "USDC", "decimals": 6},
        "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": {"symbol": "cbBTC", "decimals": 8},
    }

    try:
        await asyncio.gather(
            _stream_chain_whales("ethereum", ETH_WSS_URL, eth_contracts, producer, redis_client),
            _stream_chain_whales("arbitrum", ARB_WSS_URL, arb_contracts, producer, redis_client),
            _stream_chain_whales("base", BASE_WSS_URL, base_contracts, producer, redis_client),
        )
    except Exception as e:
        logger.debug(f"Multi-chain whale runner notice: {e}")


# ── 6. CROSS-EXCHANGE DIVERGENCE ENGINE (§1.3) ───────────────────────────────

async def stream_cross_exchange_divergence(producer: SentinelProducer, redis_client):
    """
    Monitors cross-exchange perpetual funding rate and mark price divergences
    between Binance, Bybit, and Kraken/Coinbase.
    """
    import aiohttp
    ASSETS = ["BTC", "ETH", "SOL"]
    logger.info("⚡ Cross-Exchange Funding & Basis Divergence Engine Online.")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # 1. Fetch Binance Funding Rates
                binance_rates = {}
                try:
                    async with session.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for item in data:
                                sym = item.get("symbol", "")
                                for a in ASSETS:
                                    if sym == f"{a}USDT":
                                        binance_rates[a] = {
                                            "funding_rate": float(item.get("lastFundingRate", 0)),
                                            "mark_price": float(item.get("markPrice", 0)),
                                        }
                except Exception as be:
                    logger.debug(f"Binance rate fetch warning: {be}")

                # 2. Fetch Bybit Linear Tickers
                bybit_rates = {}
                try:
                    async with session.get("https://api.bybit.com/v5/market/tickers?category=linear", timeout=10) as resp:
                        if resp.status == 200:
                            b_data = await resp.json()
                            for item in b_data.get("result", {}).get("list", []):
                                sym = item.get("symbol", "")
                                for a in ASSETS:
                                    if sym == f"{a}USDT":
                                        bybit_rates[a] = {
                                            "funding_rate": float(item.get("fundingRate", 0)),
                                            "mark_price": float(item.get("markPrice", 0)),
                                        }
                except Exception as ye:
                    logger.debug(f"Bybit rate fetch warning: {ye}")

                # 3. Compare divergences
                for asset in ASSETS:
                    b_info = binance_rates.get(asset)
                    by_info = bybit_rates.get(asset)
                    if b_info and by_info:
                        f_binance = b_info["funding_rate"]
                        f_bybit = by_info["funding_rate"]
                        funding_diff_bps = abs(f_binance - f_bybit) * 10000.0

                        p_binance = b_info["mark_price"]
                        p_bybit = by_info["mark_price"]
                        price_spread_pct = abs(p_binance - p_bybit) / p_binance * 100.0 if p_binance > 0 else 0.0

                        # Flag significant divergence (> 3.0 bps funding spread or > 0.35% price basis)
                        if funding_diff_bps >= 3.0 or price_spread_pct >= 0.35:
                            event = RawEvent(
                                source="crypto_divergence",
                                occurred_at=datetime.now(timezone.utc),
                                raw_payload={
                                    "asset": asset,
                                    "trade_type": "CROSS_EXCHANGE_FUNDING_DIVERGENCE",
                                    "binance_funding_rate": f_binance,
                                    "bybit_funding_rate": f_bybit,
                                    "funding_spread_bps": round(funding_diff_bps, 2),
                                    "price_spread_pct": round(price_spread_pct, 4),
                                    "binance_price": p_binance,
                                    "bybit_price": p_bybit,
                                    "divergence_score": min(1.0, 0.40 + (funding_diff_bps / 20.0)),
                                }
                            )
                            await producer.send(Topics.RAW_CRYPTO, event.model_dump(), key=asset)
                            logger.info(
                                f"⚡ Cross-Venue Divergence Detected | {asset} | "
                                f"Funding Spread: {funding_diff_bps:.1f} bps (Binance {f_binance*100:.4f}% vs Bybit {f_bybit*100:.4f}%)"
                            )

        except Exception as e:
            logger.debug(f"Cross exchange divergence loop notice: {e}")

        await asyncio.sleep(60)


# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL CRYPTO COLLECTOR ONLINE (HYBRID EDITION)")
    logger.info("=" * 60)
    
    producer = SentinelProducer(service_name="collector-crypto")
    await producer.start()
    redis_client = await get_redis()
    
    # §1.1 Universal heartbeat
    # Throughput counters. The heartbeat proves this process is alive;
    # these prove it is still producing.
    metrics = CollectorMetrics("collector-crypto")
    await metrics.start(redis_client)
    hb_task = asyncio.create_task(start_heartbeat_task(redis_client, "collector-crypto"))

    try:
        # Run all WebSocket streams, multi-chain whale trackers, and divergence engines concurrently
        await asyncio.gather(
            stream_coinbase_market_data(producer),
            stream_binance_liquidations(producer),
            stream_binance_funding_rates(producer, redis_client),
            poll_binance_open_interest(producer, redis_client),
            # The two above answer 451 from this host; OKX carries the same
            # surface and is what actually populates funding/OI/basis.
            poll_okx_perpetuals(producer, redis_client),
            stream_onchain_whales(producer, redis_client),
            stream_cross_exchange_divergence(producer, redis_client),
        )
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        hb_task.cancel()
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())