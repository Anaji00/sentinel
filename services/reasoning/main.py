"""
services/reasoning/main.py  

ENTERPRISE REASONING ORCHESTRATOR (GEMINI EDITION)
==================================================
Consumes Tier 2+ correlated clusters from Kafka.
Feeds raw data + Graph DB context + ML Scores into Gemini 2.5 Pro.
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
    """Parses the Gemini output for specific tracking recommendations and updates Redis."""
    recommendations = scenario.recommended_monitoring.upper()
    
    tickers = set(re.findall(r'\$([A-Z]{1,5})', recommendations))
    wallets = set(re.findall(r'(0x[a-fA-F0-9]{40})', scenario.recommended_monitoring))
    
    loop = asyncio.get_event_loop()
    
    for ticker in tickers:
        is_new = await loop.run_in_executor(None, redis_client.sadd, "sentinel:watched:equities", ticker)
        if is_new:
            logger.warning("🤖 AUTONOMOUS PIVOT: Instructing TradFi collector to track %s", ticker)

    for wallet in wallets:
        is_new = await loop.run_in_executor(None, redis_client.sadd, "sentinel:watched:wallets", wallet)
        if is_new:
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
            await asyncio.to_thread(_save_scenario, db, scenario)
            
            # 2. Broadcast to Kafka (For Alert Manager / Webhooks / Telegram)
            # Make sure Topics.SCENARIOS_GENERATED exists in your shared/kafka/topics definition
            producer.send("scenarios.generated", scenario.model_dump(), key=scenario.scenario_id)
            logger.info("📡 Broadcasted Scenario %s to Kafka", scenario.scenario_id)

            # 3. Close the loop (Machine Learning pivot)
            await apply_autonomous_feedback(scenario, redis_client)

    except Exception as e:
        logger.error("Failed to process cluster %s: %s", cluster.correlation_id, e, exc_info=True)

async def run_reasoning_loop(context_builder, generator, library, db, redis_client):
    """Main asynchronous Kafka consumption loop."""
    consumer = SentinelConsumer(
        topics=[Topics.CORRELATIONS],
        group_id="reasoning-service-group",
        auto_offset_reset="latest",
    )
    producer = SentinelProducer()
    
    logger.info("Sentinel Reasoning Engine Online. Listening for anomalies...")
    
    try:
        loop = asyncio.get_running_loop()
        while True:
            messages = await loop.run_in_executor(None, consumer.raw.poll, 1.0)
            
            for tp, msgs in messages.items():
                for message in msgs:
                    # Passed directly via kwargs since message.value is already a parsed dictionary
                    cluster = CorrelationCluster(**message.value)
                    
                    asyncio.create_task(process_cluster(cluster, db, redis_client, producer, context_builder, generator, library))
                    
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Reasoning consumer crashed: %s", e, exc_info=True)
    finally:
        consumer.close()
        producer.close()

async def _tracker_loop(tracker: ScenarioTracker):
    while True:
        await asyncio.sleep(1800)
        try:
            await asyncio.to_thread(tracker.check_all)
        except Exception as e:
            logger.error(f"Scenario Tracker error: {e}")
 
async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AI REASONING SERVICE (GEMINI)")
    logger.info("=" * 60)
 
    db              = get_timescale()
    redis_client    = get_redis()
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