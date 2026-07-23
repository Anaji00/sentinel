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

from shared.kafka import SentinelConsumer, Topics
from shared.db import get_timescale

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
                        throttled_logger.error("parse_error", f"Failed parsing telemetry message: {parse_e}")
            await consumer.commit()
        except Exception as batch_error:
            logger.error(f"Batch execution failed. Backing off 5s. Error: {batch_error}")
            await asyncio.sleep(5)

async def main():
    logger.info("Starting Telemetry Worker")
    db = await get_timescale()
    await init_db(db)
    
    consumer = SentinelConsumer(
        topics=[Topics.TELEMETRY],
        group_id="telemetry-worker-group",
        auto_offset_reset="latest",
    )
    await consumer.start()
    
    try:
        await process_telemetry(consumer, db)
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
