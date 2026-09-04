"""
services/collector-radar/main.py

QUANTITATIVE RADAR COLLECTOR (ALPACA EDITION)
=============================================
Scans the entire US Equities universe dynamically using Alpaca's Snapshot API.
Maintains rolling volume/volatility baselines via Exponential Moving Average.
Emits mathematical anomalies (Z-Score > 3.0) to the Agentic tier for LLM arbitration.
"""

import socket
import asyncio
import aiohttp
import logging
import os
import sys
import math
import time
from datetime import datetime, timezone
from typing import List, Tuple, Dict
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.utils.heartbeat import start_heartbeat_task
from shared.utils.equities import is_valid_primary_equity

from regime import MarketRegime
from shared.utils.collector_metrics import CollectorMetrics
from shared.utils.tasks import safe_create_task

# ─── CONFIGURATION & STANDARDS ────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("collector.radar")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_DATA_URL = "https://data.alpaca.markets/v2/stocks/snapshots"

# How long a per-ticker volume baseline is kept.
#
# Long enough that a ticker trading weekly keeps its history, short enough that
# the long tail of 11,631 evaluated symbols does not accumulate forever.
BASELINE_TTL_SEC = 14 * 86400

# A z-score needs a distribution behind it.
#
# The baseline carried no observation count, so the second bar a ticker ever
# traded was measured against a variance built from one -- effectively zero --
# and came back thousands of sigma from a mean of almost nothing. Measured over
# 24 hours: 2,918 volume-spike events, median z 3.42, and 91 of them above
# 1,000, topping out at 78,542.32. A z of 78,542 is not a large measurement; it
# is a division by a variance that collapsed.
#
# Below this count the ticker is still tracked and its baseline still updated,
# but no spike is emitted: the honest answer for an instrument the platform has
# barely seen is that it does not yet know what normal looks like. This is the
# same rule the aviation gap detector applies with its sixty-sample floor.
MIN_BASELINE_OBSERVATIONS = 20

# The variance floor was `max(1.0, var)` -- one share, absolute.
#
# For a ticker whose mean bar is 800 shares that is no floor at all, and for one
# whose mean bar is 2,000,000 it is meaninglessly small. Volume is heteroscedastic
# across instruments, so the floor has to scale with the instrument: a standard
# deviation is never taken as less than this share of the mean. 0.10 is
# deliberately loose -- it bounds the pathology without flattening genuine
# low-variance names.
MIN_CV_FLOOR = 0.10

# Beyond this, the number describes the denominator rather than the trade. Kept
# generous -- a genuine 30-sigma volume event is real and should still read as
# extraordinary -- but bounded, because everything downstream treats this as a
# continuous magnitude: radar multiplies it, correlation derives confidence from
# it, and the reasoning prompt prints it to a model.
Z_SCORE_REPORTING_CAP = 50.0


class QuantRadar:
    def __init__(self, redis_client):
        self.redis = redis_client
    
    async def _get_baseline(self, ticker: str) -> Tuple[float, float, int]:
        mean_key = f"sentinel:radar:1m_mean:{ticker}"
        var_key = f"sentinel:radar:1m_var:{ticker}"
        n_key = f"sentinel:radar:1m_n:{ticker}"

        mean = float(await self.redis.raw.get(mean_key) or 0.0)
        var = float(await self.redis.raw.get(var_key) or 0.0)
        # How many bars this baseline is built from. Without it there is no way
        # to tell a settled distribution from one observation.
        try:
            n = int(await self.redis.raw.get(n_key) or 0)
        except (TypeError, ValueError):
            n = 0

        return mean, var, n
    
    async def _update_baseline(self, ticker: str, current_vol: float, mean: float, var: float, alpha: float):
        new_mean = (alpha * current_vol) + ((1 - alpha) * mean)
        new_var = (alpha * (current_vol - mean)**2) + ((1 - alpha) * var)

        # An expiry, so the baseline can be evicted.
        #
        # The radar evaluates 11,631 symbols a scan and wrote two permanent keys
        # per ticker: 3,548 of them with no TTL, the largest non-evictable family
        # in the instance. Under volatile-lru Redis may only reclaim keys that
        # carry a TTL, so these accumulated until every write began returning
        # "command not allowed when used memory > 'maxmemory'" -- which took the
        # supervisor's dispatch down with it.
        #
        # A volume baseline for a ticker nobody has traded in a month is not
        # worth holding, and an active ticker rewrites its own key on every
        # scan, so the expiry never bites on anything in use.
        pipe = self.redis.raw.pipeline()
        pipe.set(f"sentinel:radar:1m_mean:{ticker}", new_mean, ex=BASELINE_TTL_SEC)
        pipe.set(f"sentinel:radar:1m_var:{ticker}", new_var, ex=BASELINE_TTL_SEC)
        # Counted with the same expiry as the moments it describes, so a
        # baseline and its observation count can never disagree about how much
        # history there is.
        pipe.incr(f"sentinel:radar:1m_n:{ticker}")
        pipe.expire(f"sentinel:radar:1m_n:{ticker}", BASELINE_TTL_SEC)
        await pipe.execute()

    async def evaluate_volume(self, ticker:str, current_vol: float, current_price: float, alpha: float, z_threshold: float) -> Tuple[bool, float]:
        # 1. PRIMARY EQUITY & DERIVATIVE FILTER: Exclude derivative ETFs (NVDY), crypto, and options
        if not is_valid_primary_equity(ticker):
            return False, 0.0

        # 2. HEIGHTENED NOTIONAL GATEKEEPER: Ignore retail noise.
        # Evaluate if >= $150,000 is moving in a single bar.
        notional_flow = current_vol * current_price
        if notional_flow < 150_000:
            return False, 0.0
        # 3. INTRADAY VWAP CURVE NORMALIZATION
        # Standardize volume spikes against intraday U-shaped volume curve profile
        vwap_mult = MarketRegime.get_intraday_volume_multiplier()
        normalized_vol = current_vol / vwap_mult

        mean, var, n = await self._get_baseline(ticker)

        # The baseline is updated whether or not a spike is emitted, so a
        # warming-up ticker still accumulates history. It is updated *after* the
        # comparison, so a spike is measured against the market before it rather
        # than against a baseline it has already moved -- the mistake that made
        # a first-ever earnings surprise look unremarkable.
        await self._update_baseline(ticker, current_vol, mean, var, alpha)

        if n < MIN_BASELINE_OBSERVATIONS or mean <= 0:
            # Not enough history to say what unusual means here.
            return False, 0.0

        # The floor scales with the instrument. `max(1.0, var)` was one share,
        # which for a thinly-traded name is no floor at all -- and it is exactly
        # the thin names that produced the four- and five-figure z-scores, where
        # the flows were *smaller* than the ones scoring in the twenties.
        std_dev = max(math.sqrt(max(0.0, var)), mean * MIN_CV_FLOOR)
        if std_dev <= 0:
            return False, 0.0

        z_score = (normalized_vol - mean) / std_dev

        # A z above this is not a measurement of the market, it is a
        # measurement of the baseline. Reported at its cap and logged, rather
        # than published as though 78,542 sigma were a finding.
        if z_score > Z_SCORE_REPORTING_CAP:
            logger.warning(
                "%s z-score %.1f exceeds the reporting cap (%d bars of history, "
                "mean %.0f, sd %.1f). Capped -- a z this size measures the "
                "baseline, not the flow.",
                ticker, z_score, n, mean, std_dev,
            )
            z_score = Z_SCORE_REPORTING_CAP

        return z_score > z_threshold, z_score

async def fetch_tradable_universe(session: aiohttp.ClientSession) -> List[str]:
    api_key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}
    
    urls = [
        "https://paper-api.alpaca.markets/v2/assets?status=active&asset_class=us_equity",
        "https://api.alpaca.markets/v2/assets?status=active&asset_class=us_equity"
    ]
    
    for url in urls:
        try:
            async with session.get(url, headers=headers, timeout=15) as resp:
                if resp.status == 200:
                    assets = await resp.json()
                    tickers = [
                        a["symbol"] for a in assets 
                        if a.get("tradable") and a.get("exchange") != "OTC" and is_valid_primary_equity(a["symbol"])
                    ]
                    if len(tickers) > 100:
                        logger.info(f"🌐 Dynamic Tradable Universe Acquired: Tracking {len(tickers)} US equities via {url}.")
                        try:
                            redis_client = await get_redis()
                            await redis_client.raw.delete("sentinel:equities:valid_set")
                            await redis_client.raw.sadd("sentinel:equities:valid_set", *tickers)
                        except Exception as rx:
                            logger.warning(f"Failed to cache valid equities set to Redis: {rx}")
                        return tickers
                else:
                    body = await resp.text()
                    logger.warning(f"Universe fetch {url} returned HTTP {resp.status}: {body[:100]}")
        except Exception as e:
            logger.warning(f"Universe fetch error for {url}: {e}")
            
    logger.error("Could not acquire dynamic Alpaca assets directory.")
    return []

def chunk_list(data: List[str], chunk_size: int):
    for i in range(0, len(data), chunk_size): yield data [i:i + chunk_size]

async def heartbeat_loop(state: dict):
    start_time = asyncio.get_event_loop().time()
    while True:
        await asyncio.sleep(60)
        elapsed = asyncio.get_event_loop().time() - start_time
        logger.info(
            f"⏱ RADAR HEARTBEAT | uptime={int(elapsed)}s "
            f"| total_evaluated={state.get('total_evaluated', 0)} "
            f"| total_anomalies={state.get('total_anomalies', 0)} "
            f"| polls={state.get('polls', 0)}"
        )

async def poll_alpaca_snapshots(session: aiohttp.ClientSession, producer: SentinelProducer, radar: QuantRadar, universe: List[str], alpha: float, z_threshold: float, state: dict):
    state["polls"] += 1
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY, "Accept": "application/json"}
    feed = os.getenv("ALPACA_DATA_FEED", "iex")
    chunks = list(chunk_list(universe, 1000))
    
    semaphore = asyncio.Semaphore(10)

    async def fetch_chunk(chunk_tickers: List[str]) -> dict:
        async with semaphore:
            chunk_url = f"{ALPACA_DATA_URL}?symbols={','.join(chunk_tickers)}&feed={feed}"
            for attempt in range(3):
                try:
                    async with session.get(chunk_url, headers=headers, timeout=10) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        elif resp.status == 429:
                            await asyncio.sleep(1.0 * (attempt + 1))
                except Exception as ex:
                    if attempt == 2:
                        logger.warning(f"Snapshot fetch timeout for chunk after 3 attempts: {ex}")
                    await asyncio.sleep(0.5)
            return {}

    snapshot_results = await asyncio.gather(*[fetch_chunk(c) for c in chunks], return_exceptions=True)

    total_evaluated = 0
    anomalies_detected = 0
    top_ticker = None
    max_z = 0.0
    top_vol = 0.0
    top_price = 0.0

    for snapshots in snapshot_results:
        if not isinstance(snapshots, dict):
            continue

        for ticker, snap in snapshots.items():
            if not snap or not isinstance(snap, dict):
                continue
            minute_bar = snap.get("minuteBar") or {}
            daily_bar = snap.get("dailyBar") or {}
            prev_daily_bar = snap.get("prevDailyBar") or {}

            # Use minuteBar for interval volume to evaluate 1-minute spikes
            volume = minute_bar.get("v", 0)
            close_price = minute_bar.get("c", 0) or daily_bar.get("c", 0) or prev_daily_bar.get("c", 0)

            if volume == 0 or close_price == 0:
                continue

            total_evaluated += 1
            is_anomaly, z_score = await radar.evaluate_volume(ticker, volume, close_price, alpha, z_threshold)

            if z_score > max_z:
                max_z = z_score
                top_ticker = ticker
                top_vol = volume
                top_price = close_price

            if is_anomaly:
                anomalies_detected += 1
                logger.warning(f"🚨 RADAR ANOMALY DETECTED: {ticker} | Volume: {volume:,.0f} | Z-Score: {z_score:.2f} | Price: ${close_price:.2f}")

                event = RawEvent(
                    source="alpaca_quant_radar",
                    type="volume_anomaly",
                    financial_data={
                        "ticker": ticker,
                        "volume": volume,
                        "close_price": close_price,
                        "z_score": z_score,
                        "notional_usd": volume * close_price
                    },
                    raw_payload={
                        "ticker": ticker,
                        "volume": volume,
                        "close_price": close_price,
                        "z_score": z_score,
                        # evaluate_volume() already computed this and refused to
                        # emit below $150k, then dropped the number. RadarAgent
                        # re-checks notional against its own $50k floor and read
                        # a missing key as 0.0, so every anomaly this collector
                        # raised was discarded one hop later.
                        "notional_usd": float(volume) * float(close_price),
                    },
                    occurred_at=datetime.now(timezone.utc)
                )

                event_dict = event.model_dump(mode="json") if hasattr(event, "model_dump") else event.dict()
                await producer.send(Topics.RAW_RADAR, event_dict, key=ticker)

    state["total_evaluated"] += total_evaluated
    state["total_anomalies"] += anomalies_detected

    if total_evaluated > 0:
        top_str = f" | Top Dynamic Mover: {top_ticker} (Z={max_z:.2f}, ${top_price*top_vol/1e6:.2f}M)" if top_ticker else ""
        logger.info(
            f"📡 RADAR SCAN COMPLETE: Evaluated {total_evaluated} symbols{top_str} "
            f"| Anomalies: {anomalies_detected} | Regime (α={alpha:.3f}, Z_th={z_threshold:.1f})"
        )

async def main():
    if not ALPACA_API_KEY:
        logger.error("ALPACA_API_KEY is not set in environment. Radar collector exiting.")
        sys.exit(1)
    producer = SentinelProducer(service_name="collector-radar")
    await producer.start()
    redis_client = await get_redis()

    radar = QuantRadar(redis_client)
    regime = MarketRegime(redis_client)

    state = {"total_evaluated": 0, "total_anomalies": 0, "polls": 0}
    heartbeat_task = safe_create_task(heartbeat_loop(state))

    # §1.1 Universal heartbeat
    # Throughput counters. The heartbeat proves this process is alive;
    # these prove it is still producing.
    metrics = CollectorMetrics("collector-radar")
    await metrics.start(redis_client)
    hb_shared_task = safe_create_task(start_heartbeat_task(redis_client, "collector-radar"))

    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, family=socket.AF_INET)
    async with aiohttp.ClientSession(connector=connector) as session:
        universe = await fetch_tradable_universe(session)
        # A collector that connects and then evaluates nothing is the hardest
        # failure to see: no exception, no restart, a healthy heartbeat, an empty
        # panel indistinguishable from a quiet market.
        safe_create_task(metrics.watch_for_starvation(source="alpaca"))
        refresh_every = 60          # cycles; one cycle is ~60s
        try:
            while True:
                t0 = asyncio.get_event_loop().time()
                # The universe was fetched once at startup. A transient 503 or a
                # missing ALPACA_SECRET_KEY therefore stranded this process
                # permanently: it polled an empty list forever while reporting
                # total_evaluated=0 as though the market were simply quiet.
                # Re-acquire until it succeeds, then refresh hourly so listings
                # and delistings are picked up.
                if not universe or state["polls"] % refresh_every == 0:
                    refreshed = await fetch_tradable_universe(session)
                    if refreshed:
                        universe = refreshed
                    elif not universe:
                        logger.error(
                            "Radar universe is empty -- Alpaca rejected the credentials "
                            "(check ALPACA_API_KEY and ALPACA_SECRET_KEY). Retrying next cycle; "
                            "no symbols are being evaluated until this resolves."
                        )
                alpha, z_threshold = await regime.get_dynamic_thresholds()
                logger.info(f"Starting radar evaluation cycle | dynamic thresholds: alpha={alpha:.4f}, z_threshold={z_threshold:.2f}")
                await poll_alpaca_snapshots(session, producer, radar, universe, alpha, z_threshold, state)
                elapsed = asyncio.get_event_loop().time() - t0
                if elapsed > 60.0:
                    logger.warning(f"⚠️ Radar poll cycle exceeded 60.0 seconds! Elapsed: {elapsed:.2f}s")
                await asyncio.sleep(max(0, 60.0 - elapsed))
        finally:
            heartbeat_task.cancel()
            hb_shared_task.cancel()
            await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())