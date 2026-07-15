"""
services/reasoning/main.py  

ENTERPRISE REASONING ORCHESTRATOR (GEMINI EDITION)
==================================================
Consumes Tier 2+ correlated clusters from Kafka.
Feeds raw data + Graph DB context + ML Scores into Ollama.
Synthesizes tactical scenarios, stores them, broadcasts to Kafka, 
and CLOSES THE LOOP by autonomously updating Redis watchlists.
"""

import asyncio
import json
import logging
import os
import sys
import re
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("reasoning.orchestrator")

from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.models import CorrelationCluster, AlertTier
from shared.db import get_timescale, get_redis

from services.reasoning.context_builder    import ContextBuilder
from services.reasoning.scenario_generator import ScenarioGenerator
from services.reasoning.scenario_tracker   import ScenarioTracker
from services.reasoning.pattern_library    import PatternLibrary

async def _save_scenario(db, scenario):
    """Persists the AI-generated scenario to PostgreSQL for frontend retrieval."""
    try:
        # 2. CRITICAL FIX: Await the execution natively
        await db.execute("""
            INSERT INTO scenarios (
                scenario_id, correlation_id, status,
                headline, significance, hypotheses,
                recommended_monitoring, confidence_overall,
                confidence_rationale
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) 
            -- NOTE: Remember to change %s to positional $1 for asyncpg!
        """, 
            scenario.scenario_id,
            scenario.correlation_id,
            scenario.status.value,
            scenario.headline,
            scenario.significance,
            json.dumps(scenario.hypotheses),
            scenario.recommended_monitoring,
            scenario.confidence_overall,
            scenario.confidence_rationale
        )
        logger.info("✅ Intelligence Synthesis Saved: %s", scenario.headline[:80])
    except Exception as e:
        logger.error("Error saving scenario %s to DB: %s", scenario.scenario_id, e, exc_info=True)

async def apply_autonomous_feedback(scenario, redis_client):
    """
    Parses Gemini output for crypto wallets. 
    (Equity tickers are now handled deterministically by the QuantResearcherAgent).
    """
    monitoring_text = str(scenario.recommended_monitoring)
    wallets = set(re.findall(r'(0x[a-fA-F0-9]{40})', monitoring_text))
    for wallet in wallets:
        is_new = await redis_client.raw.sadd("sentinel:watched:wallets", wallet)
        if is_new:
            # Expire the watch command after 30 days automatically
            await redis_client.raw.expire("sentinel:watched:wallets", 2592000)
            logger.warning("🤖 AUTONOMOUS PIVOT: Instructing Crypto collector to track wallet %s", wallet)
async def process_cluster(cluster: CorrelationCluster, db, redis_client, producer, context_builder, generator, library):
    """The core synthesis pipeline."""
    try:
        if cluster.alert_tier == AlertTier.WATCH:
            return
            
        logger.info("🧠 Synthesizing [%s] %s via Gemini...", cluster.alert_tier.name, cluster.rule_name)

        context = await context_builder.build(cluster)
        patterns = await library.find_similar(cluster.tags, cluster.rule_id)
        scenario = await generator.generate(cluster, context, patterns)
        
        if scenario:
            # 1. Save to DB (For frontend viewing)
            await asyncio.gather(
                _save_scenario(db, scenario),
                producer.send("scenarios.generated", scenario.model_dump(), key=scenario.scenario_id)
            )
            logger.info("📡 Broadcasted Scenario %s to Kafka", scenario.scenario_id)

            # 3. Close the loop (Machine Learning pivot)
            await apply_autonomous_feedback(scenario, redis_client)

    except Exception as e:
        logger.error("Failed to process cluster %s: %s", cluster.correlation_id, e, exc_info=True)
        raise e  # Let the caller handle DLQ and logging


async def run_reasoning_loop(context_builder, generator, library, db, redis_client):
    """Main asynchronous Kafka consumption loop."""
    consumer = SentinelConsumer(
        topics=[Topics.CORRELATIONS, "agents.intel.briefs"],
        group_id="reasoning-service-group",
        auto_offset_reset="latest",
    )
    producer = SentinelProducer()
    await consumer.start()
    await producer.start()

    import time as _time
    _start_time = _time.monotonic()
    _processed = 0
    _scenarios = 0
    _errors = 0

    async def _heartbeat():
        nonlocal _processed, _scenarios, _errors
        while True:
            await asyncio.sleep(60)
            elapsed = _time.monotonic() - _start_time
            rate = _processed / elapsed if elapsed > 0 else 0
            logger.info(
                f"⏱ HEARTBEAT | clusters_processed={_processed} "
                f"scenarios_generated={_scenarios} errors={_errors} "
                f"rate={rate:.1f}/s uptime={int(elapsed)}s"
            )

    heartbeat_task = asyncio.create_task(_heartbeat())
    
    logger.info("Sentinel Reasoning Engine Online. Listening for anomalies...")
    
    try:
        while True:
            try:
                batches = await consumer.get_batch(timeout_ms=1000)
                if not batches:
                    continue
                batch_tasks = []
                dlq_payloads = []
                for tp, msgs in batches.items():
                    logger.info(f"Received batch of {len(msgs)} messages to reason on partition {tp.topic}:{tp.partition}")
                    for message in msgs:
                        try:
                            raw_data = json.loads(message.value.decode('utf-8'))
                            
                            if tp.topic == "agents.intel.briefs":
                                brief = raw_data.get("brief", {})
                                headline = brief.get("headline", "No headline")
                                logger.info(f"Received intel brief on agents.intel.briefs: {headline} (severity: {brief.get('severity')})")
                                if brief.get("severity", 0) >= 4:
                                    await redis_client.raw.set(
                                        "sentinel:intel:briefs:latest",
                                        json.dumps(brief),
                                        ex=3600,  # Use native expire assignment
                                    )
                                continue
                                
                            cluster = CorrelationCluster(**raw_data)
                            logger.info(f"Received correlation cluster {cluster.correlation_id} (rule: {cluster.rule_name}, tier: {cluster.alert_tier.name}) for reasoning analysis")
                            
                            # Dispatch cluster processing to background task without blocking the consume loop
                            task = asyncio.create_task(
                                process_cluster(cluster, db, redis_client, producer, context_builder, generator, library)
                            )
                            batch_tasks.append(task)
                            dlq_payloads.append(raw_data)

                        except Exception as parse_e:
                            logger.error(f"Failed parsing reasoning message: {parse_e}", exc_info=True)
                            await producer.send(Topics.DLQ, {"error": str(parse_e), "raw": str(message.value)})
                if batch_tasks:
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    for task_result, original_payload in zip(results, dlq_payloads):
                            if isinstance(task_result, Exception):
                                _errors += 1
                                logger.error(f"Synthesis task failed silently: {task_result}", exc_info=True)
                                await producer.send(Topics.DLQ, {"error": str(task_result), "payload": original_payload})
                            else:
                                _processed += 1
                                if task_result is not None:
                                    _scenarios += 1
                    # Commit offsets securely
                await consumer.commit()
        
            except Exception as batch_error:
                    # Keep the service alive during transient network partitions
                    logger.error(f"Batch execution failed. Backing off 5s. Error: {batch_error}", exc_info=True)
                    await asyncio.sleep(5)
                    
    except asyncio.CancelledError:
        pass
    finally:
        heartbeat_task.cancel()
        await consumer.close()
        await producer.close()
        logger.info(f"Final — clusters: {_processed}  scenarios: {_scenarios}  errors: {_errors}")

async def _tracker_loop(tracker: ScenarioTracker):
    while True:
        await asyncio.sleep(1800)
        try:
            await tracker.check_all()
        except Exception as e:
            logger.error(f"Scenario Tracker error: {e}")
 
async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AI REASONING SERVICE")
    logger.info("=" * 60)
 
    db              = await get_timescale()
    redis_client    = await get_redis()
    context_builder = ContextBuilder(db)
    generator       = ScenarioGenerator(db) 
    tracker_producer = SentinelProducer()
    await tracker_producer.start()
    tracker         = ScenarioTracker(db, tracker_producer)
    library         = PatternLibrary(db)
 
    tracker_task = asyncio.create_task(_tracker_loop(tracker))
    reasoning_task = asyncio.create_task(run_reasoning_loop(context_builder, generator, library, db, redis_client))
    
    try:
        await asyncio.gather(tracker_task, reasoning_task)
    except KeyboardInterrupt:
        logger.info("Shutting down Reasoning Service...")
 
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())