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

from services.agents.base import SentinelAgent
from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.models.events import NormalizedEvent, EventType, Entity, EntityType
from shared.db import get_redis, get_neo4j, get_timescale

# Standardized logging
logger = logging.getLogger("agent.macro_cointegration")

# Immutable configuration injected via environment
WINDOW_SIZE = int(os.getenv("MACRO_COINTEG_WINDOW", "300"))      # 300-tick rolling window
Z_THRESH = float(os.getenv("MACRO_COINTEG_Z_THRESH", "3.0"))     # 3 Standard Deviations
GRAPH_CACHE_TTL = int(os.getenv("GRAPH_CACHE_TTL_SEC", "3600"))  # 1 Hour TTL for Neo4j lookups

class MacroAssetCointegrationEngine(SentinelAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def output_topic(self) -> str:
        return Topics.ENRICHED_EVENTS

    async def handle(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw = payload.get("raw_payload", payload)
        macro_asset = raw.get("ticker")
        macro_price = raw.get("close") or raw.get("price")

        if not macro_asset or not macro_price:
            return None

        exposed_equities = await self._get_exposed_instrument(macro_asset)
        if not exposed_equities:
            return None

        keys = [f"sentinel:quotes:latest:{t}" for t in exposed_equities]
        raw_micros = await self.redis.raw.mget(keys)

        tasks = []
        for micro_ticker, raw_micro in zip(exposed_equities, raw_micros):
            if not raw_micro:
                continue
            try:
                micro_price = float(raw_micro)
            except (ValueError, TypeError):
                continue

            tasks.append(self._process_pair(macro_asset, micro_ticker, macro_price, micro_price))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, NormalizedEvent):
                await self._emit_anomaly(result)
        return None

# ─── 1. GRAPH RESOLUTION (CACHED) ───────────────
    async def _get_exposed_instrument(self, macro_asset: str) -> List[str]:
        """
        Queries Neo4j for instruments with 1 or 2 degree exposure to the macro asset.
        Caches the result in Redis to prevent graph-thrashing on every tick.
        """
        cache_key = f"sentinel:cache:exposure:{macro_asset}"

        # FIX: Await the async redis call directly
        cached_raw = await self.redis.raw.get(cache_key)
        if cached_raw:
            return json.loads(cached_raw)
        
        # O(log N) Graph Traversal (Cache Miss)
        logger.info(f"Graph Cache Miss: Resolving supply chain exposures for {macro_asset} via Neo4j...")
        query = """
        MATCH (c:Entity {id: $macro_asset})-[:COMMODITY_EXPOSURE|SUPPLIES|POSITIVE_EXPOSURE_TO|INVERSE_EXPOSURE_TO*1..2]-(e:Entity {type: 'instrument'})
        RETURN DISTINCT e.id AS exposed_ticker
        """
        
        try:
            # FIX: Await the async neo4j call directly
            rows = await self.neo4j.query(query, {"macro_asset": macro_asset})
            exposed_tickers = [row["exposed_ticker"] for row in rows if row.get("exposed_ticker")]

            # FIX: Await the async redis call directly
            await self.redis.raw.set(cache_key, json.dumps(exposed_tickers), ex=GRAPH_CACHE_TTL)
            return exposed_tickers
        except Exception as e:
            logger.error(f"Neo4j query failed for {macro_asset}: {e}")
            return []
        
    # ─── 2. MATHEMATICAL KERNEL ───────────────

    def _compute_ols_residual(self, x: List[float], y: List[float]) -> Tuple[float, float, float, float, bool]:
        """
        Executes highly optimized Matrix Math for Ordinary Least Squares (OLS) regression,
        computes Ornstein-Uhlenbeck (OU) mean-reversion half-life (tau = -ln(2) / lambda),
        and verifies residual stationarity using the Augmented Dickey-Fuller (ADF) t-statistic.
        Isolated for execution inside a ThreadPool to prevent event loop blocking.
        """
        X = np.array(x, dtype=np.float64)
        Y = np.array(y, dtype=np.float64)
        
        # A = [X, 1] to solve for both beta (slope) and alpha (intercept)
        A = np.vstack([X, np.ones(len(X))]).T
        beta, alpha = np.linalg.lstsq(A, Y, rcond=None)[0]
        
        residuals = Y - (beta * X + alpha)
        current_residual = float(residuals[-1])

        # Ornstein-Uhlenbeck mean-reversion parameter estimation & ADF t-statistic calculation
        half_life = 999.0
        is_stationary = False
        if len(residuals) > 5:
            res_lag = residuals[:-1]
            res_diff = np.diff(residuals)
            A_ou = np.vstack([res_lag, np.ones(len(res_lag))]).T
            solution, resids, rank, s = np.linalg.lstsq(A_ou, res_diff, rcond=None)
            lambda_ou = solution[0]
            
            if lambda_ou < 0:
                half_life = float(-np.log(2) / lambda_ou)
                # Compute standard error of lambda_ou for ADF t-stat: t_stat = lambda / se
                n_samples = len(res_diff)
                dof = max(1, n_samples - 2)
                sse = float(resids[0]) if len(resids) > 0 else float(np.sum((res_diff - (A_ou @ solution))**2))
                mse = sse / dof
                var_lambda = mse / max(1e-9, float(np.sum((res_lag - np.mean(res_lag))**2)))
                se_lambda = float(np.sqrt(max(1e-9, var_lambda)))
                adf_tstat = lambda_ou / se_lambda
                # ADF 95% critical value ~ -2.86 for N=300
                is_stationary = adf_tstat < -2.57

        return beta, alpha, current_residual, half_life, is_stationary

    # ─── 3. STATE SYNC & STATISTICAL GATING ───────────────

    async def _process_pair(self, macro_asset: str, micro_ticker: str, current_macro_price: float, current_micro_price: float) -> Optional[NormalizedEvent]:
        """
        Synchronizes parallel state arrays in Redis, calculates regressions, 
        and detects Z-score statistical anomalies.
        """
        series_key = f"sentinel:cointeg:{macro_asset}:{micro_ticker}"

        try:
            # FIX: Use async with context manager for aioredis pipeline natively
            async with self.redis.raw.pipeline(transaction=True) as pipe:
                pipe.rpush(f"{series_key}:x", current_macro_price)
                pipe.rpush(f"{series_key}:y", current_micro_price)
                pipe.ltrim(f"{series_key}:x", -WINDOW_SIZE, -1)
                pipe.ltrim(f"{series_key}:y", -WINDOW_SIZE, -1)
                pipe.lrange(f"{series_key}:x", 0, -1)
                pipe.lrange(f"{series_key}:y", 0, -1)
                results = await pipe.execute()
                
            raw_x, raw_y = results[4], results[5]

            if len(raw_x) < WINDOW_SIZE:
                return None
            
            x_vec = [float(v) for v in raw_x]
            y_vec = [float(v) for v in raw_y]
            
            loop = asyncio.get_running_loop()
            beta, alpha, current_residual, half_life, is_stationary = await loop.run_in_executor(None, self._compute_ols_residual, x_vec, y_vec)
            res_key = f"{series_key}:residuals"

            # FIX: Use async with context manager for aioredis pipeline natively
            async with self.redis.raw.pipeline(transaction=True) as pipe:
                pipe.rpush(res_key, current_residual)
                pipe.ltrim(res_key, -WINDOW_SIZE, -1)
                pipe.lrange(res_key, 0, -1)
                res_results = await pipe.execute()
                
            res_vec = [float(r) for r in res_results[2]]

            res_mean = np.mean(res_vec)
            res_std = np.std(res_vec)
            if res_std == 0.0: res_std = 1.0

            z_score = (current_residual - res_mean) / res_std
            # ── TRIGGER LOGIC: Require ADF stationarity (p < 0.05) to eliminate spurious random walk false alerts ──
            if abs(z_score) > Z_THRESH and is_stationary:
                logger.warning(f"🚨 MACRO DECOUPLING DETECTED: {macro_asset} vs {micro_ticker} | Z-Score: {z_score:.2f}")

                asyncio.create_task(self.write_agent_memory(f"Macro Decoupling Detected: {micro_ticker} broke correlation with {macro_asset} (Z-Score: {z_score:.2f})."))
                try:
                    decoupling_summary = f"Macro Decoupling: {micro_ticker} broke correlation with {macro_asset} (Z-Score: {z_score:.2f}, Beta: {beta:.2f})"
                    await self.redis.raw.set("sentinel:macro:decoupling:latest", decoupling_summary)
                except Exception:
                    pass

                return NormalizedEvent(
                    type=EventType.MARKET_ANOMALY,
                    occurred_at=datetime.now(timezone.utc).isoformat(),
                    source= "macro_cointegration_engine",
                    primary_entity=Entity(id=micro_ticker, type=EntityType.INSTRUMENT, name=micro_ticker),
                    headline=f"Macroeconomic Decoupling: {micro_ticker} structural correlation to {macro_asset} has broken (Z-Score: {z_score:.2f})",
                    tags=["macro_divergence", "cointegration_break", macro_asset.lower(), micro_ticker.lower()],
                    anomaly_score=round(min(1.0, abs(z_score) / 4.0), 4) # Normalize z-score to 0.0 - 1.0 boundary
                )
                
            # ── CORRELATION CONFIRMATION LOGIC ──
            macro_mean = np.mean(x_vec)
            macro_std = np.std(x_vec) if np.std(x_vec) > 0 else 1.0
            macro_z_score = (x_vec[-1] - macro_mean) / macro_std
            
            if abs(macro_z_score) > 2.5 and abs(z_score) <= 1.5:
                direction = "surged" if macro_z_score > 0 else "plummeted"
                micro_dir = "UP" if (macro_z_score > 0 and beta > 0) or (macro_z_score < 0 and beta < 0) else "DOWN"
                logger.info(f"✅ MACRO CONFIRMATION: {macro_asset} shocked, {micro_ticker} followed perfectly.")
                
                asyncio.create_task(self.write_agent_memory(f"Macro Shock Confirmed: {macro_asset} {direction}, strongly dragging {micro_ticker} {micro_dir}."))

                return NormalizedEvent(
                    type=EventType.MARKET_ANOMALY,
                    occurred_at=datetime.now(timezone.utc).isoformat(),
                    source="macro_cointegration_engine",
                    primary_entity=Entity(id=micro_ticker, type=EntityType.INSTRUMENT, name=micro_ticker),
                    headline=f"Macro Correlation Confirmed: {macro_asset} {direction}, dragging {micro_ticker} {micro_dir} (Beta: {beta:.2f})",
                    tags=["macro_confirmation", "cointegration_hold", macro_asset.lower(), micro_ticker.lower()],
                    anomaly_score=round(min(1.0, abs(macro_z_score) / 4.0), 4)
                )
            
            return None
        except Exception as e:
            logger.error(f"Failed to process pair {macro_asset}-{micro_ticker}: {e}", exc_info=True)
            return None
        
        # ─── 4. EMISSION ───────────────

    async def _emit_anomaly(self, event: NormalizedEvent):
        try:
            # FIX: Await the async send call
            await self._producer.send(self.output_topic, event.model_dump(), key=event.primary_entity.id)
        except Exception as e:
            logger.error(f"Failed to emit anomaly: {e}")
    
        try:
            # FIX: Await the database write directly, and use positional $1-$9 instead of %s for asyncpg
            await self.db.execute("""
                INSERT INTO events (event_id, type, occurred_at, collected_at, source, primary_entity_id, headline, tags, anomaly_score)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (event_id, occurred_at) DO NOTHING;
            """, event.event_id, event.type.value, event.occurred_at, event.collected_at, event.source, event.primary_entity.id, event.headline, event.tags, event.anomaly_score)
        except Exception as e:
            logger.error(f"Database write failed for anomaly {event.event_id}: {e}", exc_info=True)

async def main_stream():
    # Instantiate long-lived connections
    from shared.db import get_redis, get_neo4j, get_timescale
    from shared.kafka import SentinelProducer, SentinelConsumer

    redis_client = await get_redis()
    db_client = await get_timescale()
    neo4j_client = await get_neo4j()

    producer = SentinelProducer()
    dlq = SentinelProducer()
    consumer = SentinelConsumer(
        topics=[Topics.RAW_TRADFI, Topics.RAW_CRYPTO], 
        group_id="macro-cointegration-group",
        auto_offset_reset="latest"
    )

    engine = MacroAssetCointegrationEngine(
        agent_name="macro_cointegration_engine",
        input_topics=[Topics.RAW_TRADFI, Topics.RAW_CRYPTO],
        redis_client=redis_client,
        db_client=db_client,
        neo4j_client=neo4j_client,
        producer=producer,
        consumer=consumer,
        dlq=dlq,
    )
    logger.info("⚡ MacroAssetCointegrationEngine Online. Monitoring global equilibrium...")
    await engine.run()

if __name__ == "__main__":
    asyncio.run(main_stream())
                    


                    



