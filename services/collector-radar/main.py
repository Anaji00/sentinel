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
from shared.utils.equities import is_valid_primary_equity

from regime import MarketRegime

# ─── CONFIGURATION & STANDARDS ────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("collector.radar")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_DATA_URL = "https://data.alpaca.markets/v2/stocks/snapshots"

class QuantRadar:
    def __init__(self, redis_client):
        self.redis = redis_client
    
    async def _get_baseline(self, ticker: str) -> Tuple[float, float]:
        mean_key = f"sentinel:radar:mean:{ticker}"
        var_key = f"sentinel:radar:var:{ticker}"

        mean = float(await self.redis.raw.get(mean_key) or 0.0)
        var = float(await self.redis.raw.get(var_key) or 0.0)

        return mean, var
    
    async def _update_baseline(self, ticker: str, current_vol: float, mean: float, var: float, alpha: float):
        new_mean = (alpha * current_vol) + ((1 - alpha) * mean)
        new_var = (alpha * (current_vol - mean)**2) + ((1 - alpha) * var)

        pipe = self.redis.raw.pipeline()
        pipe.set(f"sentinel:radar:mean:{ticker}", new_mean)
        pipe.set(f"sentinel:radar:var:{ticker}", new_var)
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

        mean, var = await self._get_baseline(ticker)
        std_dev = math.sqrt(max(1.0, var))
        z_score = (normalized_vol - mean) / std_dev if mean > 0 else 0.0
        await self._update_baseline(ticker, current_vol, mean, var, alpha)
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
            daily_bar = snap.get("dailyBar") or {}
            prev_daily_bar = snap.get("prevDailyBar") or {}

            volume = daily_bar.get("v", 0)
            close_price = daily_bar.get("c", 0) or prev_daily_bar.get("c", 0)

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
                        "z_score": z_score
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
    producer = SentinelProducer()
    await producer.start()
    redis_client = await get_redis()

    radar = QuantRadar(redis_client)
    regime = MarketRegime(redis_client)

    state = {"total_evaluated": 0, "total_anomalies": 0, "polls": 0}
    heartbeat_task = asyncio.create_task(heartbeat_loop(state))

    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, family=socket.AF_INET)
    async with aiohttp.ClientSession(connector=connector) as session:
        universe = await fetch_tradable_universe(session)
        try:
            while True:
                t0 = asyncio.get_event_loop().time()
                alpha, z_threshold = await regime.get_dynamic_thresholds()
                logger.info(f"Starting radar evaluation cycle | dynamic thresholds: alpha={alpha:.4f}, z_threshold={z_threshold:.2f}")
                await poll_alpaca_snapshots(session, producer, radar, universe, alpha, z_threshold, state)
                elapsed = asyncio.get_event_loop().time() - t0
                if elapsed > 60.0:
                    logger.warning(f"⚠️ Radar poll cycle exceeded 60.0 seconds! Elapsed: {elapsed:.2f}s")
                await asyncio.sleep(max(0, 60.0 - elapsed))
        finally:
            heartbeat_task.cancel()
            await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())