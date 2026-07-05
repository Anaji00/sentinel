"""
services/agents/macro_cointegration_engine.py

MACRO-ASSET COINTEGRATION ENGINE
================================
Dynamically queries Neo4j for commodity-to-equity exposure networks.
Executes online Engle-Granger cointegration tracking to detect 
structural macroeconomic regime drifts and stealth decoupling.
"""

import asyncio
import json
import logging
import os
import sys
import numpy as np
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from dotenv import load_dotenv

# Ensure system paths resolve shared microservice assets cleanly
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.models.events import NormalizedEvent, EventType, Entity, EntityType
from shared.db import get_redis, get_neo4j, get_timescale

# Standardized logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("engine.macro_cointegration")

# Immutable configuration injected via environment
WINDOW_SIZE = int(os.getenv("MACRO_COINTEG_WINDOW", "300"))      # 300-tick rolling window
Z_THRESH = float(os.getenv("MACRO_COINTEG_Z_THRESH", "3.0"))     # 3 Standard Deviations
GRAPH_CACHE_TTL = int(os.getenv("GRAPH_CACHE_TTL_SEC", "3600"))  # 1 Hour TTL for Neo4j lookups

class MacroAssetCointegrationEngine:
    def __init__(self, redis_client, neo4j_client, db_client, producer):
        self.redis = redis_client
        self.neo4j = neo4j_client
        self.db = db_client
        self.producer = producer

# ─── 1. GRAPH RESOLUTION (CACHED) ───────────────
    async def _get_exposed_instrument(self, macro_asset: str) -> List[str]:
        """
        Queries Neo4j for instruments with 1 or 2 degree exposure to the macro asset.
        Caches the result in Redis to prevent graph-thrashing on every tick.
        """
        cache_key = f"sentinel:cache:exposure:{macro_asset}"
        loop = asyncio.get_running_loop()

        cached_raw = await loop.run_in_executor(None, self.redis.raw.get, cache_key)
        if cached_raw:
            return json.loads(cached_raw)
        
        # O(log N) Graph Traversal (Cache Miss)
        logger.info(f"Graph Cache Miss: Resolving supply chain exposures for {macro_asset} via Neo4j...")
        query = """
        MATCH (c:Entity {id: $macro_asset})-[:COMMODITY_EXPOSURE|SUPPLIES*1..2]-(e:Entity {type: 'instrument'})
        RETURN DISTINCT e.id AS exposed_ticker
        """
        
        try:
            rows = await loop.run_in_executor(None, lambda: self.neo4j.query(query, {"macro_asset": macro_asset}))
            exposed_tickers = [row["exposed_ticker"] for row in rows if row.get("exposed_ticker")]

            await loop.run_in_executor(None, lambda: self.redis.raw.set(cache_key, json.dumps(exposed_tickers), ex=GRAPH_CACHE_TTL))
            return exposed_tickers
        except Exception as e:
            logger.error(f"Neo4j query failed for {macro_asset}: {e}")
            return []
        
    # ─── 2. MATHEMATICAL KERNEL ───────────────

    def _compute_ols_residual(self, x: List[float], y: List[float]) -> Tuple[float, float, float]:
        """
        Executes highly optimized Matrix Math for Ordinary Least Squares (OLS) regression.
        Isolated for execution inside a ThreadPool to prevent event loop blocking.
        """
        X = np.array(x, dtype=np.float64)
        Y = np.array(y, dtype=np.float64)
        
        # A = [X, 1] to solve for both beta (slope) and alpha (intercept)
        A = np.vstack([X, np.ones(len(X))]).T
        beta, alpha = np.linalg.lstsq(A, Y, rcond=None)[0]
        
        # Calculate the tension on the "bungee cord" for the most recent tick
        current_residual = float(Y[-1] - (beta * X[-1] + alpha))
        return beta, alpha, current_residual

    # ─── 3. STATE SYNC & STATISTICAL GATING ───────────────

    async def _process_pair(self, macro_asset: str, micro_ticker: str, current_macro_price: float, current_micro_price: float) -> Optional[NormalizedEvent]:
        """
        Synchronizes parallel state arrays in Redis, calculates regressions, 
        and detects Z-score statistical anomalies.
        """
        loop = asyncio.get_running_loop()
        series_key = f"sentinel:cointeg:{macro_asset}:{micro_ticker}"

        def _redis_transaction():
            pipe = self.redis.raw.pipeline()
            # 1. Store new prices
            pipe.rpush(f"{series_key}:x", current_macro_price)
            pipe.rpush(f"{series_key}:y", current_micro_price)
            # 2. Trim to bounded window size (Memory Safety)
            pipe.ltrim(f"{series_key}:x", -WINDOW_SIZE, -1)
            pipe.ltrim(f"{series_key}:y", -WINDOW_SIZE, -1)
            # 3. Fetch full arrays for computation
            pipe.lrange(f"{series_key}:x", 0, -1)
            pipe.lrange(f"{series_key}:y", 0, -1)
            return pipe.execute()
        
        try:
            results = await loop.run_in_executor(None, _redis_transaction)
            raw_x, raw_y = results[4], results[5]

            if len(raw_x) < WINDOW_SIZE:
                return None
            
            x_vec = [float(v) for v in raw_x]
            y_vec = [float(v) for v in raw_y]
            beta, alpha, current_residual = await loop.run_in_executor(None, self._compute_ols_residual, x_vec, y_vec)
            res_key = f"{series_key}:residuals"

            def _residual_transaction():
                pipe = self.redis.raw.pipeline()
                pipe.rpush(res_key, current_residual)
                pipe.ltrim(res_key, -WINDOW_SIZE, -1)
                pipe.lrange(res_key, 0, -1)
                return pipe.execute()
            
            res_results = await loop.run_in_executor(None, _residual_transaction)
            res_vec = [float(r) for r in res_results[2]]

            res_mean = np.mean(res_vec)
            res_std = np.std(res_vec)
            if res_std == 0.0: res_std = 1.0

            z_score = (current_residual - res_mean) / res_std
            # ── TRIGGER LOGIC ──
            if abs(z_score) > Z_THRESH:
                logger.warning(f"🚨 MACRO DECOUPLING DETECTED: {macro_asset} vs {micro_ticker} | Z-Score: {z_score:.2f}")

                return NormalizedEvent(
                    type=EventType.MARKET_ANOMALY,
                    occurred_at=datetime.now(timezone.utc).isoformat(),
                    source= "macro_cointegration_engine",
                    primary_entity=Entity(id=micro_ticker, type=EntityType.INSTRUMENT, name=micro_ticker),
                    headline=f"Macroeconomic Decoupling: {micro_ticker} structural correlation to {macro_asset} has broken (Z-Score: {z_score:.2f})",
                    tags=["macro_divergence", "cointegration_break", macro_asset.lower(), micro_ticker.lower()],
                    anomaly_score=round(min(1.0, abs(z_score) / 6.0), 4) # Normalize z-score to 0.0 - 1.0 boundary
                )
            
            return None
        except Exception as e:
            logger.error(f"Failed to process pair {macro_asset}-{micro_ticker}: {e}", exc_info=True)
            return None
        
        # ─── 4. EMISSION ───────────────

    async def _emit_anomaly(self, event: NormalizedEvent):
        loop = asyncio.get_running_loop()

        try:
            self.producer.send(Topics.ENRICHED_EVENTS, event.model_dump(), key=event.primary_entity.id)
        except Exception as e:
            logger.error(f"Failed to emit anomaly: {e}")
    

        def _db_write():
            self.db.execute("""
                INSERT INTO events (event_id, type, occurred_at, collected_at, source, primary_entity_id, headline, tags, anomaly_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id, occurred_at) DO NOTHING;
            """, (event.event_id, event.type.value, event.occurred_at, event.collected_at, event.source, event.primary_entity.id, event.headline, event.tags, event.anomaly_score))
            
        await loop.run_in_executor(None, _db_write)

async def main_stream():
    # Instantiate long-lived connections
    redis_client = await get_redis()
    neo4j_client = get_neo4j()
    db_client = get_timescale()
    producer = SentinelProducer()

    engine = MacroAssetCointegrationEngine(redis_client, neo4j_client, db_client, producer)
    
    # We subscribe to the raw stream where macro indices/commodities are published
    consumer = SentinelConsumer(
        topics=[Topics.RAW_TRADFI, "raw.macro"], 
        group_id="macro-cointegration-group",
        auto_offset_reset="latest"
    )
    logger.info("⚡ MacroAssetCointegrationEngine Online. Monitoring global equilibrium...")
    loop = asyncio.get_running_loop()

    try:
        while True:
            messages = await loop.run_in_executor(None, consumer.raw.poll, 1.0)
            if not messages:
                continue

            for _, msg_list in messages.items():
                for msg in msg_list:
                    payload = json.loads(msg.value.decode('utf-8')) if isinstance(msg.value, bytes) else msg.value
                    macro_asset = payload.get("ticker")
                    macro_price = payload.get("price")

                    if not macro_asset or not macro_price: continue

                    exposed_equities = await engine._get_exposed_instrument(macro_asset)
                    if not exposed_equities: continue

                    tasks = []
                    for micro_ticker in exposed_equities:
                        raw_micro = redis_client.raw.get(f"sentinel:quotes:latest:{micro_ticker}")
                        if not raw_micro: continue
                        micro_price = float(raw_micro)

                        tasks.append(engine._process_pair(macro_asset, micro_ticker, macro_price, micro_price))

                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, NormalizedEvent):
                            await engine._emit_anomaly(result)

    except asyncio.CancelledError:
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Fatal crash inside Macro Cointegration loop: {e}", exc_info=True)
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(main_stream())
                    


                    



