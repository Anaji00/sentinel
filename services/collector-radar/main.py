"""
services/collector-radar/main.py

QUANTITATIVE RADAR COLLECTOR (ALPACA EDITION)
=============================================
Scans the entire US Equities universe dynamically using Alpaca's Snapshot API.
Maintains rolling volume/volatility baselines via Exponential Moving Average.
Emits mathematical anomalies (Z-Score > 3.0) to the Agentic tier for LLM arbitration.
"""

import asyncio
import aiohttp
import logging
import os
import sys
import math
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

from regime import MarketRegime

# ─── CONFIGURATION & STANDARDS ────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("collector.radar")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_API_SECRET")
ALPACA_DATA_URL = "https://data.alpaca.markets/v2/stocks/snapshots"
ALPACA_ASSETS_URL = "https://api.alpaca.markets/v2/assets"


MAG_7 = ["MSFT", "AVGO", "GOOG", "AMZN", "TSLA", "AAPL", "NVDA", "MU"]

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
        # 1. NOTIONAL GATEKEEPER: Ignore retail noise entirely.
        # Only evaluate if > $50,000 is moving in a single 1-minute bar.
        notional_flow = current_vol * current_price
        if notional_flow < 50_000:
            return False, 0.0
        mean, var = await self._get_baseline(ticker)
        std_dev = math.sqrt(var) + 1e-8  # Avoid division by zero
        if mean == 0.0:
            await self._update_baseline(ticker, current_vol, current_vol, 1.0, alpha)
            return False, 0.0
        
        z_score = (current_vol - mean) / std_dev
        await self._update_baseline(ticker, current_vol, mean, var, alpha)
        return z_score > z_threshold, z_score

async def fetch_tradable_universe(session: aiohttp.ClientSession) -> List[str]:
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY}
    try:
        async with session.get(f"{ALPACA_ASSETS_URL}?status=active&asset_class=us_equity", headers=headers) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error(f"Alpaca API error fetching universe (HTTP {resp.status}): {body[:200]}. Falling back to MAG_7.")
                return MAG_7
            assets = await resp.json()
            tickers = [a["symbol"] for a in assets if a["tradable"] and a["exchange"] != "OTC"]
            logger.info(f"🌐 Dynamic Universe Acquired: Tracking {len(tickers)} equities.")
            return tickers
    except Exception as e:
        logger.error(f"Universe fetch failed: {e}")
        return MAG_7
    
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
    chunks = list(chunk_list(universe, 1000))
    tasks = [session.get(f"{ALPACA_DATA_URL}?symbols={','.join(c)}", headers=headers) for c in chunks]
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    total_evaluated = 0
    anomalies_detected = 0

    for i, resp in enumerate(responses):
        if isinstance(resp, Exception):
            logger.error(f"Failed to fetch snapshots for chunk {i}: {resp}")
            continue
        if resp.status != 200:
            body = await resp.text()
            logger.error(f"Alpaca snapshot API error for chunk {i} (HTTP {resp.status}): {body[:200]}")
            continue
            
        data = await resp.json()

        for ticker, snapshot in data.items():
            min_bar = snapshot.get("minuteBar")
            if not min_bar: continue

            volume = float(min_bar.get("v", 0.0))
            price = float(min_bar.get("c", 0.0))
            if volume == 0.0: continue

            is_anomaly, z_score = await radar.evaluate_volume(ticker, volume, price, alpha, z_threshold)
            total_evaluated += 1
            if is_anomaly:
                anomalies_detected += 1
                notional = volume * price
                
                latest_trade = snapshot.get("latestTrade")
                trade_details = ""
                trade_price = price
                trade_size = volume
                if latest_trade:
                    trade_price = float(latest_trade.get("p", price))
                    trade_size = float(latest_trade.get("s", volume))
                    trade_details = f" | Latest Trade: {trade_size} shares @ ${trade_price:,.2f}"

                logger.warning(
                    f"🚨 RADAR ANOMALY: {ticker} | Z-Score: {z_score:.2f} (threshold: {z_threshold:.1f}) "
                    f"| 1m Vol: {volume} (${notional/1e6:.2f}M){trade_details}"
                )
                event = {
                    "source": "alpaca_quant_radar",
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                    "raw_payload": {
                        "ticker": ticker,
                        "z_score": round(z_score, 3),
                        "volume": volume,
                        "price": price,
                        "notional_usd": notional,
                        "trigger": "structural_volume_spike",
                        "latest_trade_price": trade_price,
                        "latest_trade_size": trade_size
                    }
                }
                await producer.send(Topics.RAW_RADAR, event, key=ticker)

    state["total_evaluated"] += total_evaluated
    state["total_anomalies"] += anomalies_detected

    if total_evaluated > 0:
        logger.info(f"Radar evaluation complete: evaluated {total_evaluated} symbols | detected {anomalies_detected} anomalies | regime alpha={alpha:.3f}, z_threshold={z_threshold:.1f}")

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

    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        universe = await fetch_tradable_universe(session)
        try:
            while True:
                t0 = asyncio.get_event_loop().time()
                alpha, z_threshold = await regime.get_dynamic_thresholds()
                await poll_alpaca_snapshots(session, producer, radar, universe, alpha, z_threshold, state)
                elapsed = asyncio.get_event_loop().time() - t0
                await asyncio.sleep(max(0, 60.0 - elapsed))
        finally:
            heartbeat_task.cancel()
            await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())