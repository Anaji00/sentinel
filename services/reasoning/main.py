"""
services/reasoning/main.py  

ENTERPRISE REASONING ORCHESTRATOR (OLLAMA EDITION)
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
import time
from datetime import datetime, timezone
import aiohttp
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.utils.logging import setup_sentinel_logging

logger = setup_sentinel_logging("reasoning.orchestrator", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.models import CorrelationCluster, AlertTier
from shared.db import get_timescale, get_redis, get_neo4j

from services.reasoning.context_builder    import ContextBuilder
from services.reasoning.scenario_generator import ScenarioGenerator
from services.reasoning.scenario_tracker   import ScenarioTracker
from services.reasoning.pattern_library    import PatternLibrary
from shared.utils.ollama import OllamaClient
from shared.utils.inference_budget import InferenceBudget
from shared.utils.tasks import safe_create_task
from shared.utils.heartbeat import start_heartbeat_task

async def _save_scenario(db, scenario):
    """Persists the AI-generated scenario to PostgreSQL for frontend retrieval."""
    try:
        await db.execute("""
            INSERT INTO scenarios (
                scenario_id, correlation_id, status,
                headline, significance, hypotheses,
                recommended_monitoring, confidence_overall,
                confidence_rationale
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) 
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
    Parses output for crypto wallets. 
    (Equity tickers are now handled deterministically by the QuantResearcherAgent).
    """
    monitoring_text = str(scenario.recommended_monitoring)
    wallets = set(re.findall(r'(0x[a-fA-F0-9]{40})', monitoring_text))
    for wallet in wallets:
        is_new = await redis_client.raw.sadd("sentinel:watched:wallets", wallet)
        if is_new:
            await redis_client.raw.expire("sentinel:watched:wallets", 2592000)
            logger.warning("🤖 AUTONOMOUS PIVOT: Instructing Crypto collector to track wallet %s", wallet)

async def process_cluster(cluster: CorrelationCluster, db, redis_client, producer, context_builder, generator, library):
    """The core synthesis pipeline."""
    if cluster.alert_tier == AlertTier.WATCH:
        return
        
    logger.info("🧠 Synthesizing [%s] %s via Ollama...", cluster.alert_tier.name, cluster.rule_name)

    context = await context_builder.build(cluster)
    patterns = await library.find_similar(cluster.tags, cluster.rule_id)
    scenario = await generator.generate(cluster, context, patterns)
    
    if scenario:
        await asyncio.gather(
            _save_scenario(db, scenario),
            producer.send(Topics.SCENARIOS_GENERATED, scenario.model_dump(), key=scenario.scenario_id)
        )
        logger.info("📡 Broadcasted Scenario %s to Kafka", scenario.scenario_id)
        # Broadcast synthesized scenario to live WebSocket feed
        try:
            scenario_pub_payload = {
                "event_id": str(scenario.scenario_id),
                "type": "scenario_synthesis",
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "source": "Reasoning Engine",
                "primary_entity_id": str(scenario.correlation_id),
                "primary_entity_name": scenario.headline or "Strategic Intelligence Scenario",
                "entity_name": scenario.headline or "Strategic Intelligence Scenario",
                "headline": f"🧠 STRATEGIC SCENARIO: {scenario.headline}",
                "summary": str(scenario.significance or scenario.confidence_rationale or "Synthesis complete."),
                "anomaly_score": float((scenario.confidence_overall or 80) / 100.0),
                "region": "GLOBAL",
                "tags": ["scenario_synthesis", "llm_generated"],
            }
            await redis_client.raw.publish("sentinel:events:live", json.dumps(scenario_pub_payload))
        except Exception as pub_err:
            logger.debug(f"Scenario live feed pub bypass: {pub_err}")
            
        await apply_autonomous_feedback(scenario, redis_client)
        return scenario
    return None

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

    from shared.utils.ollama import OllamaClient, OLLAMA_TIMEOUT

    connector = aiohttp.TCPConnector(limit=10)
    session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
    ollama_client = OllamaClient(session, redis_client=redis_client)

    _start_time = time.monotonic()
    _processed = 0
    _scenarios = 0
    _errors = 0

    async def _heartbeat():
        nonlocal _processed, _scenarios, _errors
        while True:
            await asyncio.sleep(60)
            elapsed = time.monotonic() - _start_time
            rate = _processed / elapsed if elapsed > 0 else 0
            logger.info(
                f"⏱ HEARTBEAT | clusters_processed={_processed} "
                f"scenarios_generated={_scenarios} errors={_errors} "
                f"rate={rate:.1f}/s uptime={int(elapsed)}s"
            )

    heartbeat_task = safe_create_task(_heartbeat(), name="reasoning-heartbeat")

    # §1.1 Universal heartbeat — shared telemetry for data-health dashboard
    hb_shared_task = asyncio.create_task(start_heartbeat_task(redis_client, "reasoning"))
    
    sem = asyncio.Semaphore(3)

    # Shared with the agent swarm: one Ollama, one budget.
    _budget = InferenceBudget(redis_client, os.getenv("AGENT_MODEL", "qwen2.5:1.5b"))
    _shed = 0

    # Detached syntheses, bounded so a slow model cannot turn backlog into
    # unbounded memory. Rarely approached: the semaphore admits three at a time.
    MAX_INFLIGHT_SYNTHESES = 32
    _inflight: set = set()

    def _account_for_synthesis(task, original_payload):
        """Records a detached synthesis as it finishes.

        Runs as a completion callback so the consume loop never waits on it.
        Retrieving the exception matters: an un-retrieved one is swallowed into
        a warning at garbage-collection time, hiding every real failure.
        """
        nonlocal _processed, _scenarios, _errors
        if task.cancelled():
            return
        err = task.exception()
        if err is not None:
            _errors += 1
            logger.error(f"Synthesis task failed: {err}", exc_info=err)
            safe_create_task(
                producer.send(Topics.DLQ, {"error": str(err), "payload": original_payload})
            )
            return
        _processed += 1
        if task.result() is not None:
            _scenarios += 1


    async def sem_process_cluster(cluster, *args):
        async with sem:
            return await process_cluster(cluster, *args)

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
                    for message in msgs:
                        try:
                            raw_data = json.loads(message.value.decode('utf-8'))
                            
                            if tp.topic == "agents.intel.briefs":
                                brief = raw_data.get("brief", {})
                                headline = brief.get("headline", "No headline")
                                logger.debug(f"Received intel brief: {headline} (severity: {brief.get('severity')})")
                                if brief.get("severity", 0) >= 3:
                                    await redis_client.raw.set(
                                        "sentinel:intel:briefs:latest",
                                        json.dumps(brief),
                                        ex=3600,
                                    )
                                continue
                                
                            cluster = CorrelationCluster(**raw_data)
                            logger.debug(f"Received correlation cluster {cluster.correlation_id} for reasoning analysis")
                            
                            # Shed what cannot possibly be reached. Scenario
                            # synthesis is two model passes at several minutes
                            # each, three at a time -- about 36 clusters an hour
                            # against a backlog of 161,000, which is six months
                            # of work that will never be done. Queuing it all
                            # only guarantees the service reasons about
                            # increasingly stale correlations.
                            #
                            # The budget is shared with the agent swarm because
                            # they all talk to the same single-threaded Ollama.
                            # Peeking does not claim the slot; it just avoids
                            # building work that would sit unread.
                            if not await _budget.is_available():
                                _shed += 1
                                if _shed % 500 == 1:
                                    logger.info(
                                        f"Reasoning shed {_shed} clusters to stay within "
                                        f"inference capacity (consumer stays current)"
                                    )
                                continue

                            task = safe_create_task(
                                sem_process_cluster(cluster, db, redis_client, producer, context_builder, generator, library)
                            )
                            batch_tasks.append(task)
                            dlq_payloads.append(raw_data)

                        except Exception as parse_e:
                            logger.error(f"Failed parsing reasoning message: {parse_e}", exc_info=True)
                            await producer.send(Topics.DLQ, {"error": str(parse_e), "raw": str(message.value)})
                if batch_tasks:
                    # Scenario synthesis is multi-pass: a generation, then a
                    # devil's-advocate critique, each a separate model call of
                    # several minutes. Awaiting the batch here meant the loop
                    # stopped polling and committing until every cluster in it
                    # was fully argued through -- measured stuck at exactly zero
                    # messages an hour with 161,000 of backlog, while the process
                    # sat at 0.5% CPU simply waiting.
                    #
                    # Work is registered and accounted for on completion instead.
                    # Concurrency is still bounded by the semaphore inside
                    # sem_process_cluster, so this does not increase load on the
                    # model; it only stops the consumer waiting on it.
                    for task, original_payload in zip(batch_tasks, dlq_payloads):
                        _inflight.add(task)
                        task.add_done_callback(_inflight.discard)
                        task.add_done_callback(
                            lambda t, p=original_payload: _account_for_synthesis(t, p)
                        )

                # Committed once the work is accepted rather than once it is
                # argued. The correlation cluster is already persisted upstream;
                # what a crash costs is one scenario, which is regenerable.
                await consumer.commit()

                # Throttle *after* committing, never before. A backlog this size
                # delivers batches far larger than the in-flight ceiling, so
                # waiting for capacity first meant blocking before the offset
                # ever moved -- the stall simply relocated. Committing first lets
                # the consumer advance; this only paces the next fetch.
                while len(_inflight) >= MAX_INFLIGHT_SYNTHESES:
                    await asyncio.wait(set(_inflight), return_when=asyncio.FIRST_COMPLETED)
        
            except Exception as batch_error:
                logger.error(f"Batch execution failed. Backing off 5s. Error: {batch_error}", exc_info=True)
                await asyncio.sleep(5)
                    
    except asyncio.CancelledError:
        pass
    finally:
        heartbeat_task.cancel()
        hb_shared_task.cancel()
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
    generator       = ScenarioGenerator(db, redis_client=redis_client) 
    tracker_producer = SentinelProducer()
    await tracker_producer.start()
    tracker         = ScenarioTracker(db, tracker_producer)
    library         = PatternLibrary(db)
 
    tracker_task = safe_create_task(_tracker_loop(tracker), name="scenario-tracker")
    reasoning_task = safe_create_task(run_reasoning_loop(context_builder, generator, library, db, redis_client), name="reasoning-main-loop")
    
    try:
        await asyncio.gather(tracker_task, reasoning_task)
    except KeyboardInterrupt:
        logger.info("Shutting down Reasoning Service...")
    finally:
        tracker_task.cancel()
        reasoning_task.cancel()
        await asyncio.gather(tracker_task, reasoning_task, return_exceptions=True)
        await tracker_producer.close()
        logger.info("Reasoning Service shut down cleanly")
 
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())