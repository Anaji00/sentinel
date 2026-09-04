"""
services/collector-tradfi/main.py

ENTERPRISE TRADFI COLLECTOR (ENTERPRISE MULTI-SESSION EDITION)
=============================================================
Sources: 
 1. Finnhub (Equities WS - Regular Hours 09:30-16:00 ET)
 2. Alpaca (Stock Snapshots REST - Extended Hours Pre-Market 04:00-09:30 ET & After-Hours 16:00-20:00 ET)
 3. SEC EDGAR (Form 4 Insider Trading Atom RSS)
 4. Alpaca (Options Flow & Snapshot Sweeps)
 5. Finnhub (Earnings Calendar REST & Watchlist Auto-Injection)

Dynamic: Watchlists are driven entirely by Redis (`sentinel:watched:equities`)
Telemetry: Structured 60s heartbeats across all sessions matching Sentinel telemetry standards.
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
import feedparser
import time
import websockets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.utils.candles import candle_cache_key
from shared.utils.quote_cache import QUOTE_CACHE_TTL_SEC, quote_key
from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.models.events import entity_cache_key
from shared.utils.equities import is_valid_primary_equity, parse_occ_option_symbol
from shared.utils.logging import setup_sentinel_logging
from shared.utils.heartbeat import start_heartbeat_task
from shared.utils.collector_metrics import CollectorMetrics
from shared.utils.tasks import safe_create_task

# What counts as a block, per instrument rather than per market.
#
# `notional >= 50_000` was applied to every ticker regardless of liquidity.
# Measured over three hours of live market data: 8,174 block trades, median
# notional $71,764, and 7,371 of them (90%) between $50K and $200K -- below the
# conventional $200K threshold everywhere, and in a mega-cap far below anything
# a desk would call a block. A $0.10M AMZN print is 0.002% of that name's daily
# volume and was emitted as "SELL BLOCK" at logger.warning.
#
# A block is a trade that is large *for this instrument*, so the floor scales
# with what the instrument usually trades. The rolling notional the collector
# already maintains is the measurement; the absolute floor below it is the
# cold-start answer for a ticker with no history yet.
BLOCK_FLOOR_USD = float(os.getenv("BLOCK_FLOOR_USD", "200000"))
BLOCK_MULTIPLE_OF_TYPICAL = float(os.getenv("BLOCK_MULTIPLE_OF_TYPICAL", "8.0"))
_TYPICAL_NOTIONAL: dict = {}
_TYPICAL_ALPHA = 0.02  # slow EWMA: a block must not raise the bar it is judged against


def _is_block_trade(ticker: str, notional: float) -> bool:
    """Is this trade large for this instrument?

    The running average is updated *after* the comparison, so a large print is
    measured against the market before it rather than against a baseline it has
    already moved -- the mistake that made a first-ever earnings surprise look
    unremarkable, recorded elsewhere in this audit.
    """
    typical = _TYPICAL_NOTIONAL.get(ticker)

    if typical is None or typical <= 0:
        is_block = notional >= BLOCK_FLOOR_USD
    else:
        is_block = notional >= max(BLOCK_FLOOR_USD, typical * BLOCK_MULTIPLE_OF_TYPICAL)

    prev = typical if typical is not None else notional
    _TYPICAL_NOTIONAL[ticker] = prev + _TYPICAL_ALPHA * (notional - prev)
    return is_block



logger = setup_sentinel_logging("collector.tradfi", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
REDIS_EQUITIES_KEY = "sentinel:watched:equities"

# Finnhub allows 50 concurrent symbol subscriptions, and that budget was spent
# entirely on recency: zrevrange takes the 50 most recently *added* watchlist
# entries, and the watchlist is scored by time.time() as the radar and quant
# agents encounter anomalies. The result on a live pre-market session was a
# subscription set of AAL, ACGL, ACM, BIIB, BRKR, CAG, CLH, DTM, EIX, EME ...
# -- mid-caps that print minutes apart before the open -- with no NVDA, AAPL,
# MSFT, TSLA, AMZN or META anywhere in it. The trade counter sat unchanged at
# 7,787 across 217 consecutive heartbeats while the collector reported
# "Streaming 50/50 symbols", because that figure counts subscriptions, not data.
#
# A desk always watches the same handful of anchors and rotates the rest onto
# whatever is moving today. These slots are reserved for the names that carry
# the market; the remainder still go to the newest discoveries.
CORE_EQUITY_SYMBOLS = [
    t.strip().upper()
    for t in os.getenv(
        "CORE_EQUITY_SYMBOLS",
        "NVDA,AAPL,MSFT,AMZN,META,GOOGL,TSLA,AVGO,JPM,XOM,SPY,QQQ",
    ).split(",")
    if t.strip()
]

# The cross-asset anchors, reserved the same way the equity core is.
#
# Without these the discovery engine cannot find an inverse relationship, and
# the reason is arithmetic rather than statistical. It computes signed Pearson
# and bidirectional Granger correctly -- and over the streamed universe it had
# produced five surviving edges, coefficients 0.657 to 0.991, every one
# positive. That universe was ten crypto pairs plus a rotating set of mid-caps:
# BTCUSDT, EXK, GAP, WU, CI, OKE, F, RGEN, ETSY, ELF. Everything in it is beta
# to the same factor, so "yields up, stocks down" is not a relationship the data
# was capable of expressing.
#
# MACRO_SYMBOLS has carried sixty of these since it was written, and
# macro_intelligence_engine names TNX, CL=F, GC=F and TLT explicitly. None were
# ever streamed: the fifty subscription slots went to the equity core plus
# whatever the radar touched most recently, and the radar touches crypto because
# crypto is 51.7% of the event stream. The attention loop fed itself.
#
# A relationship needs both legs present at the same time. These are the legs.
CORE_MACRO_SYMBOLS = [
    t.strip().upper()
    for t in os.getenv(
        "CORE_MACRO_SYMBOLS",
        "TLT,TNX,CL=F,GC=F,VIX,DXY,ZB=F,HYG",
    ).split(",")
    if t.strip()
]

# Never let the core crowd out discovery entirely: at most this share of the
# budget is reserved, so the radar always keeps room to surface something new.
# The share now covers both cores together -- adding macro must cost the
# discoveries nothing it did not already cost them.
MAX_CORE_SHARE = float(os.getenv("CORE_EQUITY_MAX_SHARE", "0.5"))

# How the reserved half is split between the two cores. Equities carry more of
# the event volume; macro carries the relationships that volume cannot express.
MACRO_CORE_SHARE = float(os.getenv("CORE_MACRO_SHARE", "0.4"))

FINNHUB_SUBSCRIPTION_LIMIT = int(os.getenv("FINNHUB_SUBSCRIPTION_LIMIT", "50"))

# How long a single read may block before the loop checks for staleness. Short,
# because its only job is to return control regularly.
WS_READ_TIMEOUT_SEC = float(os.getenv("FINNHUB_WS_READ_TIMEOUT_SEC", "30"))

# How long the feed may deliver nothing during an open session before the
# connection is treated as dead. Generous enough for a quiet pre-market minute
# on illiquid names, far short of an eight-hour outage.
WS_MAX_STALL_SEC = float(os.getenv("FINNHUB_WS_MAX_STALL_SEC", "300"))

# A send that cannot drain is a wedged socket, not a slow one.
WS_SEND_TIMEOUT_SEC = float(os.getenv("FINNHUB_WS_SEND_TIMEOUT_SEC", "15"))


def select_subscription_symbols(discovered, limit: int = None) -> list:
    """The symbols worth spending a scarce subscription slot on.

    Core anchors first, in declared order, then the most recent discoveries
    until the budget is full. Deduplicated, and capped so the core can never
    consume more than MAX_CORE_SHARE of the slots.

    `discovered` arrives newest-first (zrevrange), and that order is preserved
    for the non-core remainder -- recency is a reasonable tiebreak among
    discoveries, it was simply the wrong rule for the whole budget.
    """
    # `is None`, not `or`: an explicit limit of 0 means subscribe to nothing,
    # and `0 or 50` would quietly turn that into the full budget.
    limit = FINNHUB_SUBSCRIPTION_LIMIT if limit is None else int(limit)
    if limit <= 0:
        return []

    reserved = max(0, int(limit * MAX_CORE_SHARE))
    macro_budget = max(0, min(len(CORE_MACRO_SYMBOLS), int(reserved * MACRO_CORE_SHARE)))
    equity_budget = max(0, min(len(CORE_EQUITY_SYMBOLS), reserved - macro_budget))

    selected, seen = [], set()

    # Macro first. If the budget ever shrinks, the anchors that make a
    # cross-asset relationship expressible are the ones worth keeping: a
    # correlation engine with only one side of a pair discovers nothing.
    for symbol in CORE_MACRO_SYMBOLS[:macro_budget]:
        if symbol and symbol not in seen:
            seen.add(symbol)
            selected.append(symbol)

    for symbol in CORE_EQUITY_SYMBOLS[:equity_budget]:
        if symbol and symbol not in seen:
            seen.add(symbol)
            selected.append(symbol)

    for symbol in discovered:
        if len(selected) >= limit:
            break
        symbol = str(symbol).strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            selected.append(symbol)

    return selected[:limit]


# ── MARKET SESSION HELPER ─────────────────────────────────────────────────────

def get_market_session() -> tuple:
    """
    Determines current US Eastern market session.
    Sessions:
      - PRE_MARKET:  04:00 - 09:30 ET (Mon-Fri)
      - REGULAR:     09:30 - 16:00 ET (Mon-Fri)
      - AFTER_HOURS: 16:00 - 20:00 ET (Mon-Fri)
      - CLOSED:      All other times & weekends
    """
    try:
        from zoneinfo import ZoneInfo
        et_tz = ZoneInfo("America/New_York")
    except Exception:
        et_tz = timezone(timedelta(hours=-5))

    now_et = datetime.now(et_tz)
    weekday = now_et.weekday()
    time_min = now_et.hour * 60 + now_et.minute

    if weekday >= 5:
        return "CLOSED", f"{now_et.strftime('%a %H:%M')} ET (Weekend)"

    pre_market_start = 4 * 60       # 04:00 ET
    regular_start = 9 * 60 + 30     # 09:30 ET
    regular_end = 16 * 60           # 16:00 ET
    after_hours_end = 20 * 60       # 20:00 ET

    if pre_market_start <= time_min < regular_start:
        return "PRE_MARKET", f"{now_et.strftime('%H:%M')} ET (Pre-Market 04:00-09:30)"
    elif regular_start <= time_min < regular_end:
        return "REGULAR", f"{now_et.strftime('%H:%M')} ET (Regular Hours 09:30-16:00)"
    elif regular_end <= time_min < after_hours_end:
        return "AFTER_HOURS", f"{now_et.strftime('%H:%M')} ET (After-Hours 16:00-20:00)"
    else:
        return "CLOSED", f"{now_et.strftime('%H:%M')} ET (Overnight Closed)"


# ── SEC FORM 4 (REST POLLING) ─────────────────────────────────────────────────

async def poll_form4(session: aiohttp.ClientSession, producer: SentinelProducer, redis_client):
    """
    Polls the SEC's EDGAR database for Form 4 filings (Insider Trading).
    Tracks when C-suite executives buy or sell their own company's stock.
    """
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=include&count=40&output=atom"
    loop = asyncio.get_event_loop()
    try:
        # A User-Agent SEC accepts, and a log when it does not.
        #
        # This sent "SENTINEL/1.0", which carries no contact address, and SEC
        # fair-access returns 403 for it -- verified directly: "SENTINEL/1.0"
        # gives HTTP 403 while a UA with a contact address gives HTTP 200 and
        # 22,643 bytes of Form 4 atom feed.
        #
        # The rejection then returned silently, so the poller has been blocked
        # for the life of the deployment with nothing logged. That is why there
        # are zero insider_trade events in the database and why the insider
        # clustering, the insider correlation rule and the Form 4 enricher have
        # all sat idle: not a routing defect, a 403 nobody could see.
        async with session.get(url, timeout=15, headers={"User-Agent": SEC_USER_AGENT}) as resp:
            if resp.status != 200:
                logger.warning(
                    "SEC Form 4 feed returned HTTP %s. Insider events will not "
                    "be produced this cycle; 403 usually means the User-Agent "
                    "lacks a contact address (set SEC_USER_AGENT in .env).",
                    resp.status,
                )
                return
            content = await resp.read()

        feed = await loop.run_in_executor(None, feedparser.parse, content)
        for entry in feed.entries:
            link = entry.get("link", "")
            redis_key = f"sentinel:seen:form4:{link}"

            if await redis_client.raw.exists(redis_key):
                continue

            await redis_client.raw.set(redis_key, "1", ex=604800)

            event = RawEvent(
                source="sec_form4",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "link": link,
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", "")
                }
            )
            await producer.send(Topics.RAW_TRADFI, event.model_dump(), key="form4")
    except Exception as e:
        logger.error("SEC Form 4 error: %s", e, exc_info=True)


# ── OHLCV MINUTE BAR AGGREGATOR ───────────────────────────────────────────────

class OHLCVAggregator:
    """Builds true Open, High, Low, Close, Volume candles and stores them in Redis and Kafka."""

    LUA_AGGREGATE_SCRIPT = """
    local key = KEYS[1]
    local ts = ARGV[1]
    local open = tonumber(ARGV[2])
    local high = tonumber(ARGV[3])
    local low = tonumber(ARGV[4])
    local close = tonumber(ARGV[5])
    local volume = tonumber(ARGV[6])
    local max_len = tonumber(ARGV[7])

    local latest = redis.call("LRANGE", key, 0, 0)
    if #latest == 0 then
        -- No previous candle, push new
        local new_candle = '{"ts":"' .. ts .. '","o":' .. open .. ',"h":' .. high .. ',"l":' .. low .. ',"c":' .. close .. ',"v":' .. volume .. '}'
        redis.call("LPUSH", key, new_candle)
    else
        local c = cjson.decode(latest[1])
        if c.ts == ts then
            -- Same interval, update
            c.h = math.max(c.h, high)
            c.l = math.min(c.l, low)
            c.c = close
            c.v = c.v + volume
            redis.call("LSET", key, 0, cjson.encode(c))
        else
            -- New interval, push
            local new_candle = '{"ts":"' .. ts .. '","o":' .. open .. ',"h":' .. high .. ',"l":' .. low .. ',"c":' .. close .. ',"v":' .. volume .. '}'
            redis.call("LPUSH", key, new_candle)
            redis.call("LTRIM", key, 0, max_len - 1)
        end
        -- Expiry, so the list can be evicted.
        --
        -- These were written without one. Under allkeys-lru that was invisible;
        -- under volatile-lru Redis may only evict keys that carry a TTL, so
        -- 3,436 permanent candle lists at up to 1,440 entries each filled the
        -- instance and every write began returning "command not allowed when
        -- used memory > maxmemory" -- including aircraft:last_seen, which left
        -- the dark-flight detector with nothing to scan.
        redis.call("EXPIRE", key, 604800)
    end
    return 1
    """

    TIMEFRAMES = {
        "5m": {"minutes": 5, "retention": 1000},
        "10m": {"minutes": 10, "retention": 1000},
        "15m": {"minutes": 15, "retention": 1000},
        "1h": {"minutes": 60, "retention": 1000},
        "4h": {"minutes": 240, "retention": 1000},
        "1d": {"minutes": 1440, "retention": 500},
        "1w": {"minutes": 10080, "retention": 260},
    }

    def __init__(self, producer: SentinelProducer, redis_client):
        self.producer = producer
        self.redis_client = redis_client
        self.buffer = {}

    def add_trade(self, ticker: str, price: float, volume: float):
        if ticker not in self.buffer:
            self.buffer[ticker] = {
                "O": price, "H": price, "L": price, "C": price, "V": volume,
                "pv_sum": price * volume,
            }
        else:
            d = self.buffer[ticker]
            d["H"] = max(d["H"], price)
            d["L"] = min(d["L"], price)
            d["C"] = price
            d["V"] = d["V"] + volume
            d["pv_sum"] = d["pv_sum"] + price * volume

    async def flush(self):
        now = datetime.now(timezone.utc)
        session_name, _ = get_market_session()
        count = 0

        async with self.redis_client.raw.pipeline() as pipe:
            for ticker, data in self.buffer.items():
                if data["V"] > 0:
                    vwap = round(data["pv_sum"] / data["V"], 4) if data["V"] > 0 else data["C"]
                    vwap_dev = round((data["C"] - vwap) / vwap, 6) if vwap > 0 else 0.0
                    candle = {
                        "ticker": ticker,
                        "trade_type": "OHLCV_MINUTE_BAR",
                        "open": data["O"],
                        "high": data["H"],
                        "low": data["L"],
                        "close": data["C"],
                        "volume": data["V"],
                        "vwap": vwap,
                        "vwap_deviation": vwap_dev,
                        "notional_usd": round(data["pv_sum"], 2),
                        "session": session_name,
                    }
                    event = RawEvent(
                        source="finnhub_equities" if session_name == "REGULAR" else "alpaca_extended_hours",
                        occurred_at=now,
                        raw_payload=candle
                    )
                    await self.producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                    count += 1
                    try:
                        # Store 1m base candle
                        redis_list_key = candle_cache_key(ticker, "1m")
                        candle_json = json.dumps({"ts": now.isoformat(), **candle})
                        pipe.lpush(redis_list_key, candle_json)
                        pipe.ltrim(redis_list_key, 0, 1439)
                        # See the EXPIRE in the Lua aggregator above: a list
                        # with no TTL cannot be evicted under volatile-lru.
                        pipe.expire(redis_list_key, 604800)
                        
                        # Aggregate higher timeframes atomically via Lua script
                        for tf, cfg in self.TIMEFRAMES.items():
                            minutes = cfg["minutes"]
                            # Calculate floored interval timestamp
                            if tf == "1w":
                                # Align to Monday start of week
                                days_since_monday = now.weekday()
                                interval_dt = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
                            elif tf == "1d":
                                interval_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
                            else:
                                total_minutes = now.hour * 60 + now.minute
                                floored_minutes = (total_minutes // minutes) * minutes
                                interval_dt = now.replace(hour=floored_minutes // 60, minute=floored_minutes % 60, second=0, microsecond=0)
                            
                            interval_ts = interval_dt.isoformat()
                            # Canonical helper, not a hand-built key. Building
                            # it inline here is what let this producer's
                            # vocabulary drift from the crypto producer's.
                            tf_key = candle_cache_key(ticker, tf)
                            
                            pipe.eval(
                                self.LUA_AGGREGATE_SCRIPT, 
                                1, tf_key, 
                                interval_ts, data["O"], data["H"], data["L"], data["C"], data["V"], cfg["retention"]
                            )

                    except Exception as e:
                        logger.debug(f"Redis cache pipeline warning for {ticker}: {e}")

            try:
                await pipe.execute()
            except Exception as e:
                logger.error(f"Failed to execute Redis pipeline for candles: {e}")

        self.buffer.clear()
        if count > 0:
            logger.info(f"📊 Flushed {count} minute bars [{session_name}] to Kafka and Redis.")
        else:
            logger.info(f"⏱️ Aggregator Flush [{session_name}]: 0 minute bars accumulated.")


# ── FINNHUB EQUITIES (WEBSOCKET - REGULAR MARKET HOURS) ───────────────────────

async def stream_equities(producer: SentinelProducer, redis_client, aggregator: OHLCVAggregator, watchlist_sync_event: asyncio.Event = None):
    if not FINNHUB_API_KEY:
        logger.error("FINNHUB_API_KEY missing. Cannot stream Finnhub equities.")
        return

    url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    msg_counter = {"trades": 0}

    async def sync_subscriptions(ws):
        """Watches Redis and dynamically subscribes/unsubscribes using Finnhub JSON formats.
        
        §0.3 — Instant repointing: if watchlist_sync_event is set, we wake immediately
        instead of sleeping the full 60s poll interval.
        """
        current_subs = set()

        while True:
            try:
                # Read more than the budget: the core anchors are prepended, so
                # the discovery list needs headroom to still fill the remainder.
                raw_tickers = await redis_client.raw.zrevrange(
                    REDIS_EQUITIES_KEY, 0, FINNHUB_SUBSCRIPTION_LIMIT * 2
                )
                decoded_tickers = [t.decode('utf-8') if isinstance(t, bytes) else t for t in raw_tickers]
                discovered = [t.upper() for t in decoded_tickers if is_valid_primary_equity(t)]

                # Core anchors first, discoveries after -- and the result is
                # already clamped to the Finnhub limit by construction, so the
                # separate truncation that used to follow is unnecessary.
                desired_subs = set(select_subscription_symbols(discovered))

                to_add = desired_subs - current_subs
                to_remove = current_subs - desired_subs

                # Bounded sends. On a half-open socket `ws.send` awaits a drain
                # that never happens, and this task is the one that also logs the
                # heartbeat -- so a wedged send silences the only signal that
                # would have shown the feed was dead.
                for ticker in to_add:
                    await asyncio.wait_for(
                        ws.send(json.dumps({"type": "subscribe", "symbol": ticker})),
                        timeout=WS_SEND_TIMEOUT_SEC,
                    )
                for ticker in to_remove:
                    await asyncio.wait_for(
                        ws.send(json.dumps({"type": "unsubscribe", "symbol": ticker})),
                        timeout=WS_SEND_TIMEOUT_SEC,
                    )

                if to_add or to_remove:
                    current_subs = desired_subs
                    logger.info(f"📈 Finnhub: Synced subscriptions ({len(current_subs)}/50 limit): {', '.join(sorted(current_subs))}")

                session_name, session_detail = get_market_session()
                logger.info(
                    f"⏱️ Finnhub WS Heartbeat [{session_name}] | Streaming {len(current_subs)}/50 symbols | "
                    f"Session: {session_detail} | Live Trades: {msg_counter['trades']}"
                )
            except Exception as e:
                logger.error("Sync Task Error: %s", e, exc_info=True)

            # §0.3 — Wait up to 60s, but wake instantly if PubSub triggers the event
            if watchlist_sync_event:
                try:
                    await asyncio.wait_for(watchlist_sync_event.wait(), timeout=60)
                    watchlist_sync_event.clear()
                    logger.info("⚡ Instant watchlist sync triggered via PubSub")
                except asyncio.TimeoutError:
                    pass  # Normal 60s poll interval
            else:
                await asyncio.sleep(60)

    async def flush_aggregator():
        """Timer task to flush the minute-bar buffer."""
        while True:
            await asyncio.sleep(60)
            try:
                await aggregator.flush()
            except Exception as e:
                logger.error(f"FATAL: TradFi Aggregator flush crashed: {e}", exc_info=True)

    last_prices = {}

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Finnhub WebSocket")
                sync_task = safe_create_task(sync_subscriptions(ws))
                flush_task = safe_create_task(flush_aggregator())
                last_message_at = time.monotonic()

                try:
                    while True:
                        # Bounded read, with a staleness watchdog.
                        #
                        # A bare `await ws.recv()` blocks forever on a socket
                        # that is healthy at the protocol level but delivering
                        # nothing. That is not hypothetical: this feed went
                        # silent from 12:13 to 20:11 UTC -- the whole regular
                        # session -- while pings were still being answered, so
                        # the keepalive never fired. Heartbeats per hour ran
                        # 4, 0, 0, 0, 1, 0, 0, 0, 49.
                        #
                        # Pings prove the peer is alive. They cannot prove the
                        # subscription is still delivering, and only the second
                        # question matters here.
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=WS_READ_TIMEOUT_SEC)
                            last_message_at = time.monotonic()
                        except asyncio.TimeoutError:
                            stalled_for = time.monotonic() - last_message_at
                            session_name, _ = get_market_session()
                            # Silence is expected when the market is shut. It is
                            # a fault only when trading is open.
                            if session_name != "CLOSED" and stalled_for > WS_MAX_STALL_SEC:
                                raise ConnectionError(
                                    f"Finnhub delivered nothing for {stalled_for:.0f}s "
                                    f"during {session_name}; reconnecting."
                                )
                            continue

                        data = json.loads(message)

                        if data.get("type") == "trade":
                            for item in data.get("data", []):
                                ticker = item.get("s")
                                # Anything deliberately subscribed to is ingested.
                                #
                                # is_valid_primary_equity() gates what may enter
                                # the *equity watchlist*, and correctly rejects
                                # SPY and QQQ -- they are funds, not companies.
                                # Applying it to the *feed* meant spending two
                                # of fifty scarce Finnhub slots on symbols whose
                                # trades were then thrown away on arrival.
                                if not ticker:
                                    continue
                                if not (is_valid_primary_equity(ticker)
                                        or ticker.upper() in CORE_EQUITY_SYMBOLS):
                                    continue
                                price = float(item.get("p", 0))
                                volume = float(item.get("v", 0))
                                notional = price * volume
                                msg_counter["trades"] += 1

                                prev_price = last_prices.get(ticker)
                                if prev_price is None or price == prev_price:
                                    tick_direction = "ZeroTick"
                                elif price > prev_price:
                                    tick_direction = "UpTick"
                                else:
                                    tick_direction = "DownTick"
                                last_prices[ticker] = price

                                aggregator.add_trade(ticker, price, volume)

                                if _is_block_trade(ticker, notional):
                                    if tick_direction == "DownTick":
                                        logger.warning("🔴 SELL BLOCK: %s $%.2fM at $%.2f", ticker, notional / 1e6, price)
                                    elif tick_direction == "UpTick":
                                        logger.warning("🟢 BUY BLOCK: %s $%.2fM at $%.2f", ticker, notional / 1e6, price)
                                    else:
                                        logger.warning("⚪ NEUTRAL BLOCK: %s $%.2fM at $%.2f", ticker, notional / 1e6, price)

                                    event = RawEvent(
                                        source="finnhub_equities",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker, "trade_type": "RAW_TRADE",
                                            "price": price, "size_shares": volume, "notional_usd": notional,
                                            "tick_direction": tick_direction
                                        }
                                    )
                                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                finally:
                    sync_task.cancel()
                    flush_task.cancel()
        except websockets.exceptions.ConnectionClosed as e:
            logger.info("Finnhub disconnected (%s). Reconnecting in 5s...", e)
            await asyncio.sleep(5)
        except Exception as e:
            logger.error("Finnhub error: %s. Reconnecting...", e, exc_info=True)
            await asyncio.sleep(5)


# ── ALPACA EXTENDED-HOURS EQUITIES (PRE-MARKET & AFTER-HOURS REST POLLER) ───

async def poll_extended_hours_equities(producer: SentinelProducer, redis_client, aggregator: OHLCVAggregator):
    """
    Polls Alpaca's Stock Snapshot API during Pre-Market (04:00-09:30 ET) and After-Hours (16:00-20:00 ET).
    Captures live extended-hours price movements, feeds the minute-bar aggregator, and emits block trades.
    """
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        logger.warning("Alpaca API credentials missing. Extended-hours equities polling disabled.")
        return

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Accept": "application/json"
    }
    feed = os.getenv("ALPACA_DATA_FEED", "iex")
    seen_bar_timestamps = {}
    last_prices = {}

    session_timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=session_timeout, headers=headers) as session:
        while True:
            try:
                session_name, session_detail = get_market_session()
                # Active during PRE_MARKET, AFTER_HOURS, or CLOSED (extended hours where Finnhub WS emits zero trades)
                is_extended = session_name in ("PRE_MARKET", "AFTER_HOURS", "CLOSED")

                raw_symbols = await redis_client.raw.zrange(REDIS_EQUITIES_KEY, 0, 49)
                raw_symbols = [s.decode() if isinstance(s, bytes) else s for s in raw_symbols] if raw_symbols else []
                # The same universe the websocket subscribes to, chosen by the
                # same function.
                #
                # This filtered on is_valid_primary_equity alone, which now
                # rejects broad-market funds -- so SPY and QQQ, two of the
                # CORE_EQUITY_SYMBOLS anchors the websocket ingest path admits
                # explicitly, were dropped here. The effect was regular-session
                # bars and no extended-hours bars at all for the two most
                # heavily traded names on the watchlist. Routing both paths
                # through select_subscription_symbols stops them drifting apart
                # again.
                discovered = [s.upper().strip() for s in raw_symbols if is_valid_primary_equity(s)]
                symbols = select_subscription_symbols(discovered, limit=50)
                if not symbols:
                    logger.debug(f"Extended-Hours Poller [{session_name}]: No symbols found in {REDIS_EQUITIES_KEY}. Waiting for dynamic watchlist.")
                    await asyncio.sleep(60)
                    continue

                if is_extended and symbols:
                    url = f"https://data.alpaca.markets/v2/stocks/snapshots?symbols={','.join(symbols[:50])}&feed={feed}"
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            ingested_count = 0

                            for ticker, snapshot in data.items():
                                if not snapshot:
                                    continue

                                bar = snapshot.get("minuteBar") or snapshot.get("latestTrade") or {}
                                if not bar:
                                    continue

                                bar_ts = bar.get("t")
                                if seen_bar_timestamps.get(ticker) == bar_ts:
                                    continue
                                seen_bar_timestamps[ticker] = bar_ts

                                price = float(bar.get("c") or bar.get("p") or 0.0)
                                volume = float(bar.get("v") or bar.get("s") or 0.0)
                                if price <= 0:
                                    continue

                                notional = price * volume

                                prev_price = last_prices.get(ticker)
                                if prev_price is None or price == prev_price:
                                    tick_dir = "ZeroTick"
                                elif price > prev_price:
                                    tick_dir = "UpTick"
                                else:
                                    tick_dir = "DownTick"
                                last_prices[ticker] = price

                                aggregator.add_trade(ticker, price, volume)
                                ingested_count += 1

                                if _is_block_trade(ticker, notional):
                                    tag = "🔴 SELL BLOCK" if tick_dir == "DownTick" else ("🟢 BUY BLOCK" if tick_dir == "UpTick" else "⚪ NEUTRAL BLOCK")
                                    logger.warning("%s [%s]: %s $%.2fM at $%.2f", tag, session_name, ticker, notional / 1e6, price)

                                    event = RawEvent(
                                        source="alpaca_extended_hours",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker,
                                            "trade_type": "RAW_TRADE",
                                            "price": price,
                                            "size_shares": volume,
                                            "notional_usd": notional,
                                            "tick_direction": tick_dir,
                                            "session": session_name,
                                        }
                                    )
                                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)

                                await redis_client.raw.set(
                                    quote_key(ticker), str(price), ex=QUOTE_CACHE_TTL_SEC,
                                )

                            if ingested_count > 0:
                                logger.info(f"📈 Extended-Hours Poller [{session_name}]: Ingested {ingested_count} updates across {len(symbols)} watched symbols.")
                            else:
                                logger.info(f"⏱️ Extended-Hours Poller [{session_name}]: Checked {len(symbols)} symbols | 0 new bars (waiting for new trade tick).")
                        else:
                            text = await resp.text()
                            logger.error(f"Alpaca snapshots returned HTTP {resp.status}: {text[:200]}")
                else:
                    logger.info(f"⏱️ Extended-Hours Poller Idle [{session_name}]: Regular Market Hours active.")
            except Exception as e:
                logger.error(f"Extended-hours equities polling error: {e}", exc_info=True)

            await asyncio.sleep(60)


# The last option trade published per contract, so an unchanged snapshot is not
# republished. Bounded below by _prune_option_fingerprints: contracts expire and
# the chain rolls, so this would otherwise grow for the life of the process.
_last_option_trade: dict = {}

# Contracts to remember before pruning. A day's chain across the watchlist is a
# few thousand; this keeps well clear of that without growing without bound.
_MAX_OPTION_FINGERPRINTS = 20000


async def _option_trade_is_new(redis_client, contract: str, fingerprint: str) -> bool:
    """Whether this contract's last trade differs from the one already seen.

    Redis-backed so a restart does not republish the entire chain. Falls back
    to the in-process map when Redis is unavailable, which is worse than Redis
    and much better than republishing everything on every poll.
    """
    key = f"sentinel:options:last_trade:{contract}"
    if redis_client is not None and getattr(redis_client, "raw", None) is not None:
        try:
            # SET NX returns None when the key exists, so an unchanged trade is
            # a single round trip and no write.
            stored = await redis_client.raw.get(key)
            if stored is not None:
                stored = stored.decode("utf-8") if isinstance(stored, bytes) else str(stored)
                if stored == fingerprint:
                    return False
            await redis_client.raw.set(key, fingerprint, ex=_OPTION_FINGERPRINT_TTL_SEC)
            return True
        except Exception:
            pass    # fall through to the process-local map

    if _last_option_trade.get(contract) == fingerprint:
        return False
    _last_option_trade[contract] = fingerprint
    return True


# Contracts expire and the chain rolls, so a fingerprint outlives its usefulness
# quickly. Two days spans a weekend without holding a quarter of stale keys.
_OPTION_FINGERPRINT_TTL_SEC = 2 * 86400


def _prune_option_fingerprints() -> None:
    """Drops the oldest half once the map gets large.

    Insertion order is preserved by dict, so the oldest entries are the ones
    whose contracts stopped trading -- expired, or rolled off the chain.
    """
    if len(_last_option_trade) <= _MAX_OPTION_FINGERPRINTS:
        return
    for key in list(_last_option_trade)[: len(_last_option_trade) // 2]:
        _last_option_trade.pop(key, None)


# SEC fair-access requires a User-Agent naming the requester with a contact
# address they read. The collector-filings service takes the same variable, so
# one setting covers both SEC callers.
SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT", "Sentinel-Intelligence-Platform/1.0 research@sentinel.local"
)


# Floor for the EPS surprise denominator.
#
# A nickel. Consensus below this is statistically indistinguishable from zero
# for percentage purposes, and dividing by it turns a small absolute miss into a
# four-figure percentage.
MIN_EPS_SURPRISE_DENOMINATOR = 0.05


# ── ALPACA OPTIONS FLOW (REST POLLING) ────────────────────────────────────────

async def poll_options(producer: SentinelProducer, redis_client):
    """
    Polls Alpaca's options snapshot API for the watched symbols.
    Filters for large transactions and pipes raw events to Kafka.
    """
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        logger.warning("Alpaca API credentials missing. Options flow collector will be disabled.")
        return

    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "accept": "application/json"
    }

    session_timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=session_timeout, headers=headers) as session:
        while True:
            total_sweeps = 0
            try:
                raw_symbols = await redis_client.raw.zrange("sentinel:watched:equities", 0, -1)
                raw_symbols = [s.decode() if isinstance(s, bytes) else s for s in raw_symbols] if raw_symbols else []
                symbols = [
                    s.upper().strip() for s in raw_symbols
                    if s and not (s.endswith("USDT") or s.endswith("USD") or "-" in s or "_" in s)
                ]

                if not symbols:
                    symbols = ["AAPL", "TSLA", "MSFT", "NVDA", "AMZN"]

                for ticker in symbols[:50]:
                    url = f"https://data.alpaca.markets/v1beta1/options/snapshots/{ticker}"
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            snapshots = data.get("snapshots", {})
                            if not snapshots:
                                continue

                            count = 0
                            for contract, snapshot in snapshots.items():
                                latest_trade = snapshot.get("latestTrade")
                                if not latest_trade:
                                    continue

                                # Alpaca's market-data API abbreviates: a trade is
                                # {"c","p","s","t","x"}, not {"price","size"}. Reading
                                # the long names returned None for every contract, so
                                # `float(None or 0.0)` made every option trade worth
                                # zero dollars and the filter below could never pass.
                                #
                                # Measured against the live endpoint: 99 of 100 NVDA
                                # contracts carry a latestTrade, and the maximum
                                # premium across all of them computed as $0.00. The
                                # poller has reported "Checked 50 equity tickers |
                                # Sweeps published: 0" on every cycle since it was
                                # written, with no error, because a parse that yields
                                # zero is indistinguishable from a quiet market.
                                #
                                # Downstream this is why options_flow has never been
                                # produced, and why rule_options_darkpool_surge and
                                # rule_insider_options_convergence have never fired --
                                # both trigger on event types nothing emits, while
                                # _enrich_options_flow sits fully implemented and has
                                # never received a single event.
                                price = float(
                                    latest_trade.get("p", latest_trade.get("price")) or 0.0
                                )
                                size = float(
                                    latest_trade.get("s", latest_trade.get("size")) or 0.0
                                )
                                premium = price * size * 100.0

                                if premium >= 50000.0 or size >= 100.0:
                                    # One fill, published once.
                                    #
                                    # latestTrade is the contract's last trade,
                                    # so it does not change between polls unless
                                    # the contract trades again -- and this loop
                                    # republished it every cycle regardless. The
                                    # macro collector had the identical defect
                                    # against the identical Alpaca field and was
                                    # fixed with a fingerprint; this path was
                                    # missed.
                                    #
                                    # Measured over 24 hours: 15,970 options
                                    # events carrying 1,392 distinct contracts,
                                    # 91.3% duplicates, one contract published
                                    # 200 times. Every copy was scored,
                                    # correlated and counted, so a single
                                    # $5.8m TEX sweep became 200 anomalies at a
                                    # score of 1.000.
                                    #
                                    # The trade's own timestamp is the identity:
                                    # a genuine new fill carries a new one, and
                                    # the price and size are included so a
                                    # feed that omits the timestamp still
                                    # deduplicates on the trade itself.
                                    trade_fp = "|".join(str(x) for x in (
                                        latest_trade.get("t") or latest_trade.get("timestamp"),
                                        price,
                                        size,
                                    ))
                                    # Kept in Redis rather than process memory.
                                    #
                                    # The in-process map meant every restart
                                    # republished the whole chain once -- 36
                                    # events per deploy. The aviation gap
                                    # detector had the identical shape earlier
                                    # in this audit, where 462 of 464 events
                                    # turned out to be redeploys re-emitting a
                                    # backlog, and the fix there was the same
                                    # move.
                                    #
                                    # A miss falls through and publishes, which
                                    # is the safe direction: a duplicate costs
                                    # a row, a false suppression loses a fill.
                                    if not await _option_trade_is_new(redis_client, contract, trade_fp):
                                        continue

                                    parsed = parse_occ_option_symbol(contract) or {}
                                    opt_type = parsed.get("option_type", "CALL")
                                    strike_val = parsed.get("strike")
                                    expiry_val = parsed.get("expiry")
                                    greeks_val = snapshot.get("greeks") or {}
                                    iv_val = snapshot.get("impliedVolatility") if snapshot.get("impliedVolatility") is not None else snapshot.get("implied_volatility")
                                    oi_val = snapshot.get("openInterest") if snapshot.get("openInterest") is not None else snapshot.get("open_interest")

                                    event = RawEvent(
                                        source="alpaca_options",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "ticker": ticker,
                                            "option_symbol": contract,
                                            "price": price,
                                            "volume": size,
                                            "size": size,
                                            "premium_usd": premium,
                                            "option_type": opt_type,
                                            "strike": strike_val,
                                            "expiry": expiry_val,
                                            "implied_volatility": float(iv_val) if iv_val is not None else None,
                                            "open_interest": int(oi_val) if oi_val is not None else None,
                                            "greeks": greeks_val,
                                        }
                                    )
                                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=ticker)
                                    count += 1
                                    total_sweeps += 1

                            if count > 0:
                                logger.info(f"Published {count} options flow events for {ticker} to Kafka.")
                        else:
                            text = await resp.text()
                            if resp.status in (401, 403):
                                logger.warning(f"Alpaca Options API returned HTTP {resp.status} (Subscription level restricted). Throttling retry 10m.")
                                await asyncio.sleep(600)
                                break
                            else:
                                logger.error(f"Alpaca API options snapshots for {ticker} returned {resp.status}: {text}")

                _prune_option_fingerprints()
                logger.info(
                    "⏱️ Options Poller Heartbeat: Checked %s equity tickers | "
                    "Sweeps published: %s | Contracts tracked: %s",
                    len(symbols[:50]), total_sweeps, len(_last_option_trade),
                )
            except Exception as e:
                logger.error(f"Error in Options flow collector: {e}", exc_info=True)

            await asyncio.sleep(300)


async def run_polling(producer: SentinelProducer, redis_client):
    connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            await asyncio.gather(
                poll_form4(session, producer, redis_client),
            )
            await asyncio.sleep(60)


# ── FINNHUB EARNINGS CALENDAR (REST POLLING) ──────────────────────────────────

async def poll_finnhub_earnings(producer: SentinelProducer, redis_client):
    """
    Polls Finnhub's /calendar/earnings endpoint daily.
    - Deduplicates via Redis (sentinel:seen:earnings:{symbol}:{date}).
    - Auto-injects T-minus-N day upcoming earnings tickers into sentinel:watched:equities.
    - Emits EARNINGS_REPORT (pre-announcement) or EARNINGS_SURPRISE (post-actual).
    All cadence, lookahead, and surprise thresholds are configurable via env vars.
    """
    import time as _time

    poll_interval = int(os.getenv("EARNINGS_POLL_SECONDS", "3600"))
    lookahead_days = int(os.getenv("EARNINGS_LOOKAHEAD_DAYS", "7"))
    watchlist_inject_days = int(os.getenv("EARNINGS_WATCHLIST_INJECT_DAYS", "3"))

    session_timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            try:
                today = datetime.now(timezone.utc).date()
                from_date = today.isoformat()
                to_date = (today + timedelta(days=lookahead_days)).isoformat()

                url = "https://finnhub.io/api/v1/calendar/earnings"
                params = {
                    "from": from_date,
                    "to": to_date,
                    "token": FINNHUB_API_KEY,
                }

                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"Finnhub earnings API returned {resp.status}: {text[:200]}")
                        await asyncio.sleep(poll_interval)
                        continue

                    data = await resp.json()

                earnings_list = data.get("earningsCalendar", [])
                if not earnings_list:
                    logger.debug("Finnhub earnings calendar returned empty results.")
                    await asyncio.sleep(poll_interval)
                    continue

                emit_count = 0
                watchlist_count = 0
                for entry in earnings_list:
                    symbol = (entry.get("symbol") or "").upper().strip()
                    report_date = entry.get("date", "")
                    if not symbol or not report_date:
                        continue

                    dedup_key = f"sentinel:seen:earnings:{symbol}:{report_date}"
                    eps_actual = entry.get("epsActual")
                    eps_estimate = entry.get("epsEstimate")
                    revenue_actual = entry.get("revenueActual")
                    revenue_estimate = entry.get("revenueEstimate")
                    hour = entry.get("hour", "")

                    fingerprint = f"{eps_actual}:{revenue_actual}"
                    existing = await redis_client.raw.get(dedup_key)
                    if existing:
                        existing_str = existing.decode() if isinstance(existing, bytes) else str(existing)
                        if existing_str == fingerprint:
                            continue

                    await redis_client.raw.set(dedup_key, fingerprint, ex=604800)

                    # A percentage surprise divided by a near-zero estimate
                    # measures the estimate, not the miss.
                    #
                    # The only guard was `eps_estimate != 0`, so an estimate of
                    # $0.0051 passed it. Live rows:
                    #
                    #   SECZ  actual -2.37   estimate 0.0051  ->  <span> -46,570.6%
                    #   RRGB  actual  0.12   estimate 0.0034  ->   +3,429.4%
                    #   MLCI  actual -0.37   estimate 0.0102  ->   -3,727.5%
                    #
                    # None of those describe the size of a miss; they describe
                    # how close a consensus sat to zero. 70 of 101 surprises
                    # cleared the radar agent's 10% gate on this arithmetic.
                    #
                    # The denominator is floored at a nickel, which bounds the
                    # artifact without discarding a real miss, and the absolute
                    # difference is published beside it so a consumer can judge
                    # magnitude directly rather than inferring it from a ratio.
                    eps_surprise_pct = None
                    eps_surprise_abs = None
                    if eps_actual is not None and eps_estimate is not None:
                        try:
                            actual_f = float(eps_actual)
                            estimate_f = float(eps_estimate)
                        except (TypeError, ValueError):
                            actual_f = estimate_f = None
                        if actual_f is not None:
                            eps_surprise_abs = round(actual_f - estimate_f, 4)
                            denominator = max(abs(estimate_f), MIN_EPS_SURPRISE_DENOMINATOR)
                            eps_surprise_pct = ((actual_f - estimate_f) / denominator) * 100.0

                    has_actual = eps_actual is not None
                    trade_type = "EARNINGS_SURPRISE" if has_actual else "EARNINGS_REPORT"

                    mcap_floor_b = float(os.getenv("EARNINGS_MCAP_FLOOR_B", "500"))
                    try:
                        report_dt = datetime.strptime(report_date, "%Y-%m-%d").date()
                        days_until = (report_dt - today).days
                        if 0 <= days_until <= watchlist_inject_days:
                            mcap_b = None
                            mcap_cache_key = f"sentinel:mcap:{symbol}"
                            try:
                                cached_mcap = await redis_client.raw.get(mcap_cache_key)
                                if cached_mcap:
                                    mcap_b = float(cached_mcap)
                                else:
                                    profile_url = "https://finnhub.io/api/v1/stock/profile2"
                                    async with session.get(profile_url, params={"symbol": symbol, "token": FINNHUB_API_KEY}) as profile_resp:
                                        if profile_resp.status == 200:
                                            profile_data = await profile_resp.json()
                                            mcap_raw = profile_data.get("marketCapitalization", 0)
                                            mcap_b = float(mcap_raw) / 1000.0 if mcap_raw else 0.0
                                            await redis_client.raw.set(mcap_cache_key, str(mcap_b), ex=86400)
                                        elif profile_resp.status == 429:
                                            logger.debug(f"Finnhub rate limited during mcap lookup for {symbol}")
                                            mcap_b = None
                                        else:
                                            mcap_b = 0.0
                            except Exception as mcap_err:
                                logger.debug(f"Market cap lookup failed for {symbol}: {mcap_err}")

                            if mcap_b is not None and mcap_b >= mcap_floor_b:
                                await redis_client.raw.zadd(
                                    "sentinel:watched:equities",
                                    mapping={symbol: _time.time()},
                                )
                                await redis_client.raw.zremrangebyrank("sentinel:watched:equities", 0, -51)
                                watchlist_count += 1
                                logger.debug(f"Earnings watchlist inject: {symbol} (mcap ${mcap_b:.0f}B >= ${mcap_floor_b:.0f}B floor)")
                            elif mcap_b is not None:
                                logger.debug(f"Earnings watchlist skip: {symbol} (mcap ${mcap_b:.0f}B < ${mcap_floor_b:.0f}B floor)")
                    except (ValueError, TypeError):
                        pass

                    earnings_context = {
                        "report_date": report_date,
                        "session": hour,
                        "eps_estimate": float(eps_estimate) if eps_estimate is not None else None,
                        "eps_actual": float(eps_actual) if eps_actual is not None else None,
                        "eps_surprise_pct": round(eps_surprise_pct, 2) if eps_surprise_pct is not None else None,
                            "eps_surprise_abs": eps_surprise_abs,
                        "revenue_estimate": float(revenue_estimate) if revenue_estimate is not None else None,
                        "revenue_actual": float(revenue_actual) if revenue_actual is not None else None,
                        "trade_type": trade_type,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                    try:
                        await redis_client.raw.set(
                            entity_cache_key("sentinel:earnings", symbol),
                            json.dumps(earnings_context),
                            ex=604800,
                        )
                    except Exception as re:
                        logger.debug(f"Redis earnings cache write for {symbol}: {re}")

                    event = RawEvent(
                        source="finnhub_earnings",
                        occurred_at=datetime.now(timezone.utc),
                        raw_payload={
                            "ticker": symbol,
                            "trade_type": trade_type,
                            "report_date": report_date,
                            "session": hour,
                            "eps_estimate": float(eps_estimate) if eps_estimate is not None else None,
                            "eps_actual": float(eps_actual) if eps_actual is not None else None,
                            "eps_surprise_pct": round(eps_surprise_pct, 2) if eps_surprise_pct is not None else None,
                            "eps_surprise_abs": eps_surprise_abs,
                            "revenue_estimate": float(revenue_estimate) if revenue_estimate is not None else None,
                            "revenue_actual": float(revenue_actual) if revenue_actual is not None else None,
                        },
                    )
                    await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=symbol)
                    emit_count += 1

                if emit_count > 0 or watchlist_count > 0:
                    logger.info(
                        f"📅 Earnings Calendar: Emitted {emit_count} events, "
                        f"injected {watchlist_count} tickers into watchlist "
                        f"(window: {from_date} → {to_date})"
                    )

            except Exception as e:
                logger.error(f"Finnhub earnings calendar error: {e}", exc_info=True)

            await asyncio.sleep(poll_interval)


# ── INSTITUTIONAL FIX 4.4 CLIENT ──────────

class InstitutionalFIXClient:
    """
    State Street / Tier-1 Institutional FIX 4.4 & FIXT 1.1 Market Data Client.
    Sends Logon (MsgType=A) & MarketDataRequest (MsgType=V) and processes snapshots (MsgType=W) and
    incremental updates (MsgType=X), streaming order book & trade ticks to Kafka.
    """
    def __init__(self, host: str, port: int, sender_comp_id: str = "SENTINEL_PROD", target_comp_id: str = "STATE_STREET"):
        self.host = host
        self.port = port
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.is_connected = False

    def build_logon(self) -> bytes:
        import simplefix
        msg = simplefix.FixMessage()
        msg.append_pair(8, "FIX.4.4")
        msg.append_pair(35, "A")  # Logon
        msg.append_pair(49, self.sender_comp_id)
        msg.append_pair(56, self.target_comp_id)
        msg.append_pair(34, "1")  # MsgSeqNum
        msg.append_pair(52, datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        msg.append_pair(98, "0")  # EncryptMethod: None
        msg.append_pair(108, "30") # HeartBtInt: 30s
        return msg.encode()

    def build_market_data_request(self, symbols: list) -> bytes:
        import simplefix
        msg = simplefix.FixMessage()
        msg.append_pair(8, "FIX.4.4")
        msg.append_pair(35, "V")  # MarketDataRequest
        msg.append_pair(49, self.sender_comp_id)
        msg.append_pair(56, self.target_comp_id)
        msg.append_pair(34, "2")
        msg.append_pair(52, datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3])
        msg.append_pair(262, f"MDR_{int(time.time())}")
        msg.append_pair(263, "1")  # Snapshot + Updates
        msg.append_pair(264, "0")  # Full Book
        msg.append_pair(146, str(len(symbols)))
        for s in symbols:
            msg.append_pair(55, s)
        return msg.encode()

async def run_institutional_fix(producer: SentinelProducer, redis_client):
    """
    Institutional FIX engine loop for watched equities.
    Connects to real institutional FIX venue via TCP socket. If no venue is configured,
    idles in standby mode (publishing zero synthetic quotes to maintain true provenance).
    """
    logger.info("⚡ Institutional FIX 4.4 Engine initialized for watched equities.")
    fix_host = os.getenv("FIX_ENGINE_HOST")
    fix_port_str = os.getenv("FIX_ENGINE_PORT", "9800")
    enable_fix = os.getenv("ENABLE_FIX_CLIENT", "false").lower() in ("true", "1", "yes")

    if not fix_host or not enable_fix:
        logger.info("⚡ FIX 4.4 Engine: No active venue configured (ENABLE_FIX_CLIENT=false). Standby mode active (zero synthetic provenance).")
        while True:
            await asyncio.sleep(60)

    try:
        fix_port = int(fix_port_str)
    except ValueError:
        fix_port = 9800

    while True:
        try:
            raw_watchlist = await redis_client.raw.zrange(REDIS_EQUITIES_KEY, 0, -1)
            symbols = [s.decode() if isinstance(s, bytes) else s for s in raw_watchlist] if raw_watchlist else []
            if not symbols:
                await asyncio.sleep(5)
                continue

            client = InstitutionalFIXClient(fix_host, fix_port)
            logger.info(f"⚡ FIX 4.4 Client connecting to {fix_host}:{fix_port} for {len(symbols)} symbols...")
            reader, writer = await asyncio.open_connection(fix_host, fix_port)
            client.is_connected = True

            # Send FIX Logon & MarketDataRequest
            writer.write(client.build_logon())
            await writer.drain()
            await asyncio.sleep(0.5)

            writer.write(client.build_market_data_request(symbols))
            await writer.drain()

            while client.is_connected:
                raw_data = await reader.read(4096)
                if not raw_data:
                    break
                import simplefix
                parser = simplefix.FixParser()
                parser.append_buffer(raw_data)
                while True:
                    fix_msg = parser.get_message()
                    if fix_msg is None:
                        break
                    msg_type = fix_msg.get(35)
                    sym_tag = fix_msg.get(55)
                    price_tag = fix_msg.get(270) or fix_msg.get(44)
                    if sym_tag and price_tag:
                        try:
                            sym = sym_tag.decode() if isinstance(sym_tag, bytes) else str(sym_tag)
                            p_val = float(price_tag)
                            event = RawEvent(
                                source="institutional_fix",
                                occurred_at=datetime.now(timezone.utc),
                                raw_payload={
                                    "ticker": sym,
                                    "price": p_val,
                                    "msg_type": msg_type.decode() if isinstance(msg_type, bytes) else str(msg_type),
                                    "source_protocol": "FIX.4.4",
                                }
                            )
                            await producer.send(Topics.RAW_TRADFI, event.model_dump(), key=sym)
                        except Exception as fe:
                            logger.warning(f"Error parsing FIX message: {fe}")
        except Exception as e:
            logger.debug(f"FIX 4.4 engine connection loop: {e}")

        await asyncio.sleep(10)


# ── PUBSUB WATCHLIST SYNC LISTENER (§0.3) ─────────────────────────────────────

async def _watchlist_pubsub_listener(redis_client, sync_event: asyncio.Event):
    """Subscribes to sentinel:collector:watchlist_sync PubSub channel.
    
    When the API gateway mutates the watchlist, it publishes to this channel.
    We set the sync_event so stream_equities wakes immediately and repoints the
    Finnhub WebSocket subscriptions without waiting for the 60s poll cycle.
    """
    pubsub = redis_client.raw.pubsub()
    await pubsub.subscribe("sentinel:collector:watchlist_sync")
    logger.info("🔔 Watchlist PubSub listener active on sentinel:collector:watchlist_sync")

    try:
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg and msg.get("type") == "message":
                logger.info(f"📡 Watchlist sync signal received: {msg.get('data', b'').decode() if isinstance(msg.get('data'), bytes) else msg.get('data')}")
                sync_event.set()
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe("sentinel:collector:watchlist_sync")
        await pubsub.close()


# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL TradFi Service (Enterprise Multi-Session Edition)")
    logger.info("=" * 60)
    producer = SentinelProducer(service_name="collector-tradfi")
    await producer.start()
    redis_client = await get_redis()
    logger.info("Starting TradFi Collector (Finnhub WS, SEC EDGAR, Alpaca Extended Hours & Options, Finnhub Earnings, FIX 4.4 Engine)")

    aggregator = OHLCVAggregator(producer, redis_client)

    # §0.3 — Shared event for instant watchlist repointing
    watchlist_sync_event = asyncio.Event()

    # §1.1 — Universal heartbeat
    # Throughput counters. The heartbeat proves this process is alive;
    # these prove it is still producing.
    metrics = CollectorMetrics("collector-tradfi")
    await metrics.start(redis_client)
    hb_task = safe_create_task(start_heartbeat_task(redis_client, "collector-tradfi"))

    # §0.3 — PubSub listener for instant watchlist sync
    pubsub_task = safe_create_task(_watchlist_pubsub_listener(redis_client, watchlist_sync_event))

    try:
        await asyncio.gather(
            run_polling(producer, redis_client),
            stream_equities(producer, redis_client, aggregator, watchlist_sync_event),
            poll_extended_hours_equities(producer, redis_client, aggregator),
            poll_options(producer, redis_client),
            poll_finnhub_earnings(producer, redis_client),
            run_institutional_fix(producer, redis_client),
        )
    finally:
        hb_task.cancel()
        pubsub_task.cancel()
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())