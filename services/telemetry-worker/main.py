"""
services/telemetry-worker/main.py

Subscribes to agents.telemetry and writes agent lifecycle and performance metrics to TimescaleDB.
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.utils.logging import setup_sentinel_logging, ThrottledLogger

logger = setup_sentinel_logging("telemetry-worker", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
throttled_logger = ThrottledLogger(logger, default_interval_sec=10.0)

from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.db import get_timescale, get_redis
from shared.utils.metrics import MetricsCollector

try:
    from drift_scheduler import ModelDriftScheduler
except ImportError:
    try:
        from services.telemetry_worker.drift_scheduler import ModelDriftScheduler
    except ImportError:
        import importlib.util
        spec = importlib.util.spec_from_file_location("drift_scheduler", Path(__file__).parent / "drift_scheduler.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        ModelDriftScheduler = mod.ModelDriftScheduler

async def init_db(db):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS agent_telemetry (
            id SERIAL PRIMARY KEY,
            agent_name VARCHAR(255) NOT NULL,
            task_id VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            system_prompt_length INT,
            user_prompt_length INT,
            latency_ms FLOAT,
            output_payload JSONB,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS agent_predictions (
            id SERIAL PRIMARY KEY,
            prediction_id VARCHAR(255),
            correlation_id VARCHAR(255),
            predicted_target VARCHAR(255),
            confidence FLOAT,
            simulated_vector JSONB,
            recommendations JSONB,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

async def process_telemetry(consumer, db):
    while True:
        try:
            batches = await consumer.get_batch(timeout_ms=1000)
            if not batches:
                continue
                
            for tp, messages in batches.items():
                for message in messages:
                    try:
                        data = json.loads(message.value.decode('utf-8'))
                        if message.topic == Topics.AGENTS_PREDICTIONS:
                            await db.execute("""
                                INSERT INTO agent_predictions (
                                    prediction_id, correlation_id, predicted_target, 
                                    confidence, simulated_vector, recommendations, occurred_at
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                            """,
                                str(data.get("prediction_id") or data.get("trace_id") or "wargame_sim"),
                                str(data.get("correlation_id") or ""),
                                str(data.get("predicted_next_target_entity_id") or data.get("predicted_target") or "unknown"),
                                float(data.get("simulation_confidence") or data.get("confidence") or 0.0),
                                json.dumps(data.get("simulated_trajectory_vector", [])),
                                json.dumps(data.get("preemptive_recommendations", [])),
                                datetime.now(timezone.utc)
                            )
                            MetricsCollector.increment("agent_predictions_consumed_total")
                            logger.info("🔮 Persisted agent prediction for target: %s", data.get("predicted_next_target_entity_id"))
                        else:
                            await db.execute("""
                                INSERT INTO agent_telemetry (
                                    agent_name, task_id, status, 
                                    system_prompt_length, user_prompt_length, 
                                    latency_ms, output_payload, occurred_at
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            """,
                                data.get("agent", "unknown"),
                                data.get("task_id", "unknown"),
                                data.get("status", "unknown"),
                                data.get("system_prompt_length"),
                                data.get("user_prompt_length"),
                                data.get("latency_ms"),
                                json.dumps(data.get("output_payload")) if "output_payload" in data else None,
                                datetime.now(timezone.utc)
                            )
                    except Exception as parse_e:
                        throttled_logger.error("parse_error", f"Failed parsing telemetry/prediction message: {parse_e}")
            await consumer.commit()
        except Exception as batch_error:
            logger.error(f"Batch execution failed. Backing off 5s. Error: {batch_error}")
            await asyncio.sleep(5)

async def main():
    logger.info("Starting Telemetry Worker")
    db = await get_timescale()
    await init_db(db)
    
    redis_client = None
    try:
        redis_client = await get_redis()
    except Exception as re:
        logger.warning(f"Redis unavailable for drift scheduler: {re}")

    producer = SentinelProducer()
    await producer.start()

    # ── WIRE MODEL DRIFT SCHEDULER (§6.1, §6.2) ──────────────────────────────
    drift_scheduler = ModelDriftScheduler(
        redis_client=redis_client,
        producer=producer,
        check_interval_sec=3600,
    )
    drift_task = asyncio.create_task(drift_scheduler.start())

    # Regression Guard: Confirm background task is running (§6.2)
    await asyncio.sleep(0.05)
    if drift_task.done() and drift_task.exception():
        logger.error(f"❌ ModelDriftScheduler failed to launch: {drift_task.exception()}")
        raise RuntimeError(f"ModelDriftScheduler startup failure: {drift_task.exception()}")
    
    logger.info("🛡️ ModelDriftScheduler startup regression guard passed: background task is active.")
    MetricsCollector.set_gauge("drift_scheduler_active", 1.0)
    
    consumer = SentinelConsumer(
        topics=[Topics.TELEMETRY, Topics.AGENTS_PREDICTIONS],
        group_id="telemetry-worker-group",
        auto_offset_reset="latest",
    )
    await consumer.start()
    
    try:
        await process_telemetry(consumer, db)
    except asyncio.CancelledError:
        pass
    finally:
        drift_scheduler.stop()
        drift_task.cancel()
        await consumer.close()
        await producer.flush()
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
