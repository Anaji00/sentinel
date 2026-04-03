"""
services/reasoning/main.py  —  run loop only.
 
Consumes correlations.detected from Kafka.
For Tier 2+ clusters: builds context, calls Gemini, stores scenario.
Also runs scenario_tracker every 30 min to check for resolution signals.
 
Only fires on Tier 2 (ALERT) and Tier 3 (INTELLIGENCE).
Tier 1 (WATCH) is logged but not sent to Gemini — not worth the API cost.
"""

# ── 1. PYTHON STANDARD LIBRARY IMPORTS ────────────────────────────────────────
# These come pre-installed with Python. We use them for async concurrency,
# JSON parsing, logging, and interacting with the operating system.
import asyncio
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── 2. THIRD-PARTY LIBRARY IMPORTS ────────────────────────────────────────────
# These are external packages installed via pip. 
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# FIXED: Typo "reasoning,main" changed to "reasoning.main"
logger = logging.getLogger("reasoning.main")

# ── 3. INTERNAL CUSTOM IMPORTS (SHARED) ───────────────────────────────────────
# These are our own modules from the `shared` directory.
from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.models import CorrelationCluster, AlertTier
from shared.db import get_timescale

# ── 4. INTERNAL CUSTOM IMPORTS (LOCAL SERVICES) ───────────────────────────────
# These are our own custom classes specifically built for the Reasoning pipeline.
from services.reasoning.context_builder    import ContextBuilder
from services.reasoning.scenario_generator import ScenarioGenerator
from services.reasoning.scenario_tracker   import ScenarioTracker
from services.reasoning.pattern_library    import PatternLibrary

def _save_scenario(db, scenario):
    """
    Persist scenario to TimescaleDB.
    Separating this from the generation logic is a best practice (Separation of Concerns).
    """
    try:
        # STANDARD LIBRARY: `json` is used here to serialize a Python dictionary 
        # into a JSON string so PostgreSQL can store it in a JSONB column.
        import json as _json
        
        # INTERNAL FUNCTION: `db.execute()` is our custom wrapper around the psycopg2 library.
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
            _json.dumps(scenario.hypotheses),
            scenario.recommended_monitoring,
            scenario.confidence_overall,
            scenario.confidence_rationale,
        ))
        logger.info(
            f"Scenario Saved: {scenario.headline[:80]} "
            f"(confidence: {scenario.confidence_overall}%)"
        )
    except Exception as e:
        logger.error(f"Error saving scenario: {e}")

def _reasoning_loop(consumer, context_builder, generator, library, db):
    """Blocking consume loop — runs in thread executor."""
    processed = 0
    generated = 0

    while True:
        try:
            for message in consumer:
                try:
                    cluster = CorrelationCluster(**message.value)

                    if cluster.aler_tier == AlertTier.WATCH:
                        logger.info("WATCH SIGNAL: SKIPPING...")
                        processed += 1
                        continue

                    logger.info(
                        f"Processing [{cluster.alert_tier.name}] {cluster.rule_name}"
                    )

                    context = context_builder.build(cluster)
                    
                    patterns = library.find_similar(cluster.tags, cluster.rule_id)

                    scenario = generator.generate(cluster, context, patterns)
                    if scenario:
                        _save_scenario(db, scenario)
                        generated += 1
                    processed += 1
                    if processed % 100 == 0:
                        logger.info(f"Processed {processed} | Generated {generated} scenarios")
                
                except Exception as e:
                    logger.error(f"Error processing message/Reasoning loop error: {e}", exc_info=True)
        except KeyboardInterrupt:
            break
        except StopIteration:
            continue
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
            break

    return processed, generated

async def _tracker_loop(tracker: ScenarioTracker):
    """Check active scenarios for resolution signals every 30 min."""
    while True:
        await asyncio.sleep(1800)
        try:
            tracker.check_all()
        except Exception as e:
            logger.error(f"Tracker error: {e}", exc_info=True)
 
 
async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Reasoning Service")
    logger.info("=" * 60)
 
    db              = get_timescale()
    context_builder = ContextBuilder()
    generator       = ScenarioGenerator()
    tracker         = ScenarioTracker()
    library         = PatternLibrary()
 
    consumer = SentinelConsumer(
        topics=[Topics.CORRELATIONS],
        group_id="reasoning-service",
        auto_offset_reset="latest",
    )
 
    asyncio.create_task(_tracker_loop(tracker))
 
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="reasoning") as pool:
        processed, generated = await loop.run_in_executor(
            pool,
            _reasoning_loop,
            consumer, context_builder, generator, library, db,
        )
 
    consumer.close()
    logger.info(f"Final — processed: {processed}  scenarios generated: {generated}")
 
 
if __name__ == "__main__":
    asyncio.run(main())