"""
services/reasoning/main.py  

ENTERPRISE REASONING ORCHESTRATOR (GEMINI EDITION)
==================================================
Consumes Tier 2+ correlated clusters from Kafka.
Feeds raw data + Graph DB context + ML Scores into Gemini 2.5 Pro.
Synthesizes tactical scenarios, stores them, and CLOSES THE LOOP by 
autonomously updating Redis watchlists based on AI recommendations.
"""

import asyncio
import json
import logging
import os
import sys
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

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

from shared.kafka import SentinelConsumer, Topics
from shared.models import CorrelationCluster, AlertTier
from shared.db import get_timescale, get_redis

from services.reasoning.context_builder    import ContextBuilder
from services.reasoning.scenario_generator import ScenarioGenerator
from services.reasoning.scenario_tracker   import ScenarioTracker
from services.reasoning.pattern_library    import PatternLibrary

# Maintain strong references to background tasks so the Garbage Collector doesn't kill them
background_tasks = set()

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
    Parses the Gemini output for specific tracking recommendations and updates Redis.
    This is what makes the system self-steering.
    """
    recommendations = scenario.recommended_monitoring.upper()
    
    # 1. Look for stock tickers (e.g., "$LMT", "$COIN")
    tickers = set(re.findall(r'\$([A-Z]{1,5})', recommendations))
    
    # 2. Look for wallet addresses (e.g., "0x123...abc")
    wallets = set(re.findall(r'(0x[a-fA-F0-9]{40})', scenario.recommended_monitoring))
    
    loop = asyncio.get_event_loop()
    
    for ticker in tickers:
        is_new = await loop.run_in_executor(None, redis_client.raw.sadd, "sentinel:watched:equities", ticker)
        if is_new:
            logger.warning("🤖 AUTONOMOUS PIVOT: Instructing TradFi collector to track %s", ticker)

    for wallet in wallets:
        is_new = await loop.run_in_executor(None, redis_client.raw.sadd, "sentinel:watched:wallets", wallet)
        if is_new:
            logger.warning("🤖 AUTONOMOUS PIVOT: Instructing Crypto collector to track wallet %s", wallet)

async def process_cluster(cluster: CorrelationCluster, db, redis_client, context_builder, generator, library):
    """
    The core synthesis pipeline. Fetches Neo4j context, cross-references historical
    patterns, streams the payload to Gemini, and updates the system's sensors.
    """
    try:
        if cluster.alert_tier == AlertTier.WATCH:
            logger.debug("Skipping Tier 1 WATCH signal: %s", cluster.rule_name)
            return
            
        logger.info("🧠 Synthesizing [%s] %s via Gemini...", cluster.alert_tier.name, cluster.rule_name)

        context = context_builder.build(cluster)
        patterns = library.find_similar(cluster.tags, cluster.rule_id)
        
        scenario = await generator.generate(cluster, context, patterns)
        
        if scenario:
            await asyncio.to_thread(_save_scenario, db, scenario)
            await apply_autonomous_feedback(scenario, redis_client)

    except Exception as e:
        logger.error("Failed to process cluster %s: %s", cluster.correlation_id, e, exc_info=True)

def _consume_loop(consumer, db, redis_client, context_builder, generator, library, loop):
    """
    Blocking Kafka consume loop. Runs in a ThreadPoolExecutor so the 
    asyncio event loop stays free for the heavy Gemini API calls.
    """
    for message in consumer:
        try:
            # FIX: message.value is already a dict, no need to decode/json.loads
            cluster = CorrelationCluster(**message.value)
            
            # Create the task and assign it to the main async loop
            task = asyncio.run_coroutine_threadsafe(
                process_cluster(cluster, db, redis_client, context_builder, generator, library), 
                loop
            )
            # Prevent garbage collection
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)

        except Exception as e:
            logger.error("Reasoning message processing error: %s", e, exc_info=True)

async def _tracker_loop(tracker: ScenarioTracker):
    """Background task to evaluate if active scenarios have resolved."""
    while True:
        await asyncio.sleep(1800) # 30 minutes
        try:
            await asyncio.to_thread(tracker.check_all)
        except Exception as e:
            logger.error(f"Scenario Tracker error: {e}")

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AI REASONING SERVICE (GEMINI)")
    logger.info("=" * 60)

    if not os.getenv("GEMINI_API_KEY"):
        logger.error("CRITICAL: GEMINI_API_KEY not found in environment!")
        return

    db              = get_timescale()
    redis_client    = get_redis()
    context_builder = ContextBuilder()
    generator       = ScenarioGenerator(db) 
    tracker         = ScenarioTracker()
    library         = PatternLibrary()

    consumer = SentinelConsumer(
        topics=[Topics.CORRELATIONS],
        group_id="reasoning-service-group",
        auto_offset_reset="latest",
    )

    logger.info("Sentinel Reasoning Engine Online. Listening for anomalies...")

    tracker_task = asyncio.create_task(_tracker_loop(tracker))
    loop = asyncio.get_running_loop()

    try:
        # Run the synchronous Kafka consumer in a separate thread
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="reason_consumer") as pool:
            await loop.run_in_executor(
                pool, 
                _consume_loop, 
                consumer, db, redis_client, context_builder, generator, library, loop
            )
    except KeyboardInterrupt:
        logger.info("Shutting down Reasoning Service...")
    finally:
        consumer.close()
        tracker_task.cancel()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())