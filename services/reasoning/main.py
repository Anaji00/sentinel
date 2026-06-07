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

def _save_scenario(db, scenario):
    """Persists the AI-generated scenario to PostgreSQL for frontend retrieval."""
    try:
        db.execute("""
            INSERT INTO scenarios (
                scenario_id, correlation_id, status,
                headline, significance, hypotheses,
                recommended_monitoring, confidence_overall,
                confidence_rationale
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            scenario.scenario_id,
            scenario.correlation_id,
            scenario.status.value,
            scenario.headline,
            scenario.significance,
            json.dumps(scenario.hypotheses),
            scenario.recommended_monitoring,
            scenario.confidence_overall,
            scenario.confidence_rationale,
        ))
        logger.info("✅ Intelligence Synthesis Saved: %s", scenario.headline[:80])
    except Exception as e:
        logger.error("Error saving scenario %s to DB: %s", scenario.scenario_id, e, exc_info=True)

async def apply_autonomous_feedback(scenario, redis_client):
    """
    Parses Gemini output for crypto wallets. 
    (Equity tickers are now handled deterministically by the QuantResearcherAgent).
    """
    wallets = set(re.findall(r'(0x[a-fA-F0-9]{40})', scenario.recommended_monitoring))
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

        context = context_builder.build(cluster)
        patterns = library.find_similar(cluster.tags, cluster.rule_id)
        scenario = await generator.generate(cluster, context, patterns)
        
        if scenario:
            # 1. Save to DB (For frontend viewing)
            await asyncio.gather(
                asyncio.to_thread(_save_scenario, db, scenario),
                asyncio.to_thread(producer.send, "scenarios.generated", scenario.model_dump(), key=scenario.scenario_id)
            )
            logger.info("📡 Broadcasted Scenario %s to Kafka", scenario.scenario_id)

            # 3. Close the loop (Machine Learning pivot)
            await apply_autonomous_feedback(scenario, redis_client)

    except Exception as e:
        logger.error("Failed to process cluster %s: %s", cluster.correlation_id, e, exc_info=True)

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

    
    logger.info("Sentinel Reasoning Engine Online. Listening for anomalies...")
    
    try:
        while True:
            batches = await consumer.get_batch(timeout_ms=1000)
            if not batches:
                continue
            for tp, msgs in batches.items():
                for message in msgs:
                    try:
                        raw_data = json.loads(message.value.decode('utf-8'))
                        
                        if tp.topic == "agents.intel.briefs":
                            brief = raw_data.get("brief", {})
                            if brief.get("severity", 0) >= 4:
                                await redis_client.raw.set(
                                    "sentinel:intel:briefs:latest",
                                    json.dumps(brief),
                                    ex=3600,  # [CRITICAL FIX]: Use native expire assignment
                                )
                            continue
                            
                        cluster = CorrelationCluster(**raw_data)
                        
                        # Dispatch cluster processing to background task without blocking the consume loop
                        asyncio.create_task(
                            process_cluster(cluster, db, redis_client, producer, context_builder, generator, library)
                        )
                    except Exception as e:
                        logger.error(f"Failed parsing reasoning message: {e}", exc_info=True)
                
                # [CRITICAL FIX]: Commit offsets securely
                await consumer.commit()
                    
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Reasoning consumer crashed: %s", e, exc_info=True)
    finally:
        await consumer.close()
        await producer.close()

async def _tracker_loop(tracker: ScenarioTracker):
    while True:
        await asyncio.sleep(1800)
        try:
            await asyncio.to_thread(tracker.check_all)
        except Exception as e:
            logger.error(f"Scenario Tracker error: {e}")
 
async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AI REASONING SERVICE")
    logger.info("=" * 60)
 
    db              = get_timescale()
    redis_client    = await get_async_redis()
    context_builder = ContextBuilder()
    generator       = ScenarioGenerator(db) 
    tracker         = ScenarioTracker()
    library         = PatternLibrary()
 
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