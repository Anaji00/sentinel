"""
services/correlation/main.py

Consumes enriched.events.
Runs every rule against every event.
Emits CorrelationCluster to correlations.detected when a rule fires.

Rules live in rules/:
  maritime.py     — MARITIME_001
  financial.py    — FINANCIAL_001
  aviation.py     — AVIATION_001
  news.py         — NEWS_001
  cross_domain.py — CROSS_001
"""
import aiohttp
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
import inspect

# ── 1. ENVIRONMENT & PATH SETUP ───────────────────────────────────────────────
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.utils.ollama import OllamaClient
from services.correlation.soft_correlator import SoftCorrelator
from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import NormalizedEvent, CorrelationCluster, AlertTier
from shared.db import get_redis
from services.correlation.event_store import EventStore
from services.correlation.rules import ALL_RULES


logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("correlation")


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Correlation Engine")
    logger.info(f"Rules loaded: {len(ALL_RULES)}")
    logger.info("=" * 60)

    # ── 2. INITIALIZE INFRASTRUCTURE ──────────────────────────────────────────
    redis_client = await get_redis()
    store    = EventStore(redis_client)
    producer = SentinelProducer()
    consumer = SentinelConsumer(
        topics=[Topics.ENRICHED_EVENTS],
        group_id="correlation-engine",
        auto_offset_reset="latest",
    )

    processed  = 0
    corr_fired = 0
    errors     = 0

    await producer.start()
    await consumer.start()

    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        ollama_client = OllamaClient(session)
        soft_correlator = SoftCorrelator(ollama_client)
        # Load soft correlator in the background to prevent blocking service startup
        asyncio.create_task(soft_correlator._load())

    import time as _time
    _start_time = _time.monotonic()

    async def _heartbeat():
        nonlocal processed, corr_fired, errors
        while True:
            await asyncio.sleep(60)
            elapsed = _time.monotonic() - _start_time
            rate = processed / elapsed if elapsed > 0 else 0
            logger.info(
                f"⏱ HEARTBEAT | processed={processed} "
                f"correlations={corr_fired} errors={errors} "
                f"rate={rate:.1f}/s uptime={int(elapsed)}s"
            )

    heartbeat_task = asyncio.create_task(_heartbeat())

    total_received = 0
    last_logged_received = 0

    try:
        # ── 3. THE EVENT PUMP (MAIN LOOP) ─────────────────────────────────────
        while True:
            try:
            # CORRECTED: Use native async batch fetching to prevent event-loop blocking
                batches = await consumer.get_batch(timeout_ms=1000)
                
                if not batches:
                    continue

                for tp, messages in batches.items():
                    total_received += len(messages)
                    if total_received - last_logged_received >= 250:
                        logger.info(f"Received batch of {len(messages)} events to correlate on partition {tp.topic}:{tp.partition}")
                        last_logged_received = total_received
                    for message in messages:
                        try:
                            # Parse the JSON payload safely before feeding to Pydantic
                            raw_data = json.loads(message.value.decode('utf-8'))
                            event = NormalizedEvent(**raw_data)

                            try:
                                await store.add_event(event)
                                
                                # ── 4. SEMANTIC CORRELATION (SOFT) ───────────────────
                                embedding = await soft_correlator.embed_event(event)
                                if embedding:
                                    await soft_correlator.store(event, embedding)
                                    
                                    # Cross-domain similarity search
                                    similar_events = await soft_correlator.find_similar(
                                        embedding, 
                                        exclude_domain=event.type.value.split("_")[0]
                                    )
                                    
                                    if similar_events:
                                        logger.info(f"🧠 Semantic Match Found for event {event.event_id} -> rule: Cross-Domain Semantic Convergence")
                                        
                                        # Synthesize the CorrelationCluster dynamically
                                        supporting_ids = [e.get("event_id") for e in similar_events[:3] if e.get("event_id")]
                                        
                                        cluster = CorrelationCluster(
                                            rule_id="SEMANTIC_001",
                                            rule_name="Cross-Domain Semantic Convergence",
                                            alert_tier=AlertTier.INTELLIGENCE if len(supporting_ids) >= 2 else AlertTier.ALERT,
                                            trigger_event_id=event.event_id,
                                            supporting_event_ids=supporting_ids,
                                            entity_ids=[event.primary_entity.id] if event.primary_entity else [],
                                            description=f"Neural embedding matched {len(similar_events)} highly similar cross-domain events. Anomalous semantic convergence detected.",
                                            tags=["semantic_match", "cross_domain", "ai_cluster"]
                                        )
                                        
                                        await store.save_correlation(cluster)
                                        await producer.send(
                                            Topics.CORRELATIONS,
                                            cluster.model_dump(),
                                            key=cluster.correlation_id,
                                        )
                                        corr_fired += 1
                                        
                            except Exception as e:
                                logger.error(f"Failed to store or embed event {event.event_id}: {e}", exc_info=True)
                                continue

                            processed += 1
                            if processed % 100 == 0:
                                logger.info(f"Heartbeat | Processed {processed} events | Total correlations: {corr_fired}")

                        except Exception as e:
                            errors += 1
                            logger.error(f"Correlation loop error: {e}", exc_info=True)
                            try:
                                # CORRECTED: Await the DLQ network call
                                await producer.send(
                                    Topics.DLQ,
                                    data={
                                        "topic": Topics.ENRICHED_EVENTS,
                                        "error": str(e),
                                        "raw": str(message.value)
                                    }
                                )
                            except Exception as dlq_e:
                                logger.error(f"FATAL: Failed to route to DLQ: {dlq_e}. Shutting down.")
                
                    
                    # CORRECTED: Commit offsets after batch is fully processed to prevent infinite replay
                    await consumer.commit()

            except Exception as outer_err:
                # Catch broad event loop/network exceptions without crashing the worker
                logger.error(f"Kafka consumer network/batch error. Backing off. {outer_err}")
                await asyncio.sleep(5)
                
    except asyncio.CancelledError:
        logger.info("Shutting down correlation engine...")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        heartbeat_task.cancel()
        # CORRECTED: Await graceful closure of TCP sockets
        await producer.close()
        await consumer.close()
        logger.info(f"Final — processed: {processed}  correlations: {corr_fired}  errors: {errors}")


if __name__ == "__main__":
    # CORRECTED: OS-level event loop policy enforcement and proper coroutine execution
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())