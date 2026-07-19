import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional
import json

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("enrichment")

from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import RawEvent, NormalizedEvent
from shared.db import get_redis, get_timescale, get_neo4j
from shared.db.bootstrap import bootstrap_database

from services.enrichment.anomaly_scorer import DynamicAnomalyScorer
from services.enrichment.db_writer import DBWriter
from services.enrichment.graph_writer import GraphWriter
from services.enrichment.entity_resolver import EntityResolver
from services.enrichment.gap_detector import VesselGapDetector

# --- THE NEW ENRICHERS ---
from services.enrichment.enrichers.maritime import MaritimeEnricher
from services.enrichment.enrichers.aviation import AviationEnricher
from services.enrichment.enrichers.news import NewsEnricher
from services.enrichment.enrichers.cyber import CyberEnricher
from services.enrichment.enrichers.tradfi import TradFiEnricher
from services.enrichment.enrichers.crypto import CryptoEnricher
from services.enrichment.enrichers.prediction import PredictionEnricher



async def _heartbeat_loop(state: dict):
    """Periodic heartbeat for operational visibility."""
    while True:
        await asyncio.sleep(60)
        elapsed = state["elapsed"]()
        rate = state["processed"] / elapsed if elapsed > 0 else 0
        logger.info(
            f"⏱ HEARTBEAT | processed={state['processed']} "
            f"errors={state['errors']} rate={rate:.1f}/s "
            f"uptime={int(elapsed)}s"
        )

async def _ofac_sync_loop():
    """Syncs OFAC sanctions list on startup then every 24 hours.
    
    Uses a simple asyncio loop instead of apscheduler — no external
    dependency needed for a single daily job.
    """
    from shared.utils.sanctions import rebuild_sanctions_from_list

    while True:
        try:
            logger.info("Starting OFAC sanctions sync...")
            # Phase 2: download and parse the actual SDN list from OFAC.
            # For now, we rebuild from a static keyword set.
            updated_keywords = ["irgc", "dprk", "wagner", "pdvsa", "new_sanction_target"]
            rebuild_sanctions_from_list(updated_keywords)
            logger.info("OFAC sanctions sync complete.")
        except Exception as e:
            logger.error(f"OFAC sync failed: {e}")
        await asyncio.sleep(86_400)  # 24 hours

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Enrichment Service (Multi-Domain Edition)")
    logger.info("=" * 60)

    await bootstrap_database()  # Ensure DB schema is ready before processing

    timescale = await get_timescale()
    redis     = await get_redis()
    asyncio.create_task(_ofac_sync_loop())
    
    # Wait for databases to come online
    producer = SentinelProducer()
    dlq = SentinelProducer()
    await producer.start()
    await dlq.start()
    
    scorer = DynamicAnomalyScorer(redis)
    db = DBWriter(timescale)
    graph = GraphWriter(producer)
    neo4j = await get_neo4j()
    resolver = EntityResolver(redis, neo4j)

# STRICT DEPENDENCY INJECTION ALIGNMENT: (scorer, redis, graph, [resolver])
    maritime = MaritimeEnricher(scorer, redis, graph, resolver)
    aviation = AviationEnricher(scorer, redis, graph, resolver)
    news = NewsEnricher(scorer, redis, graph)
    cyber = CyberEnricher(scorer, redis, graph)
    tradfi = TradFiEnricher(scorer, redis, graph)
    crypto = CryptoEnricher(scorer, redis, graph)
    prediction = PredictionEnricher(scorer, redis, graph)
    
    enrichers_tuple = (maritime, aviation, news, cyber, tradfi, crypto, prediction)

    consumer = SentinelConsumer(
        topics=[
            Topics.RAW_MARITIME, Topics.RAW_AVIATION, Topics.RAW_NEWS, 
            Topics.RAW_CYBER, Topics.RAW_TRADFI, Topics.RAW_CRYPTO, Topics.RAW_PREDICTION
        ],
        group_id="enrichment-service",
    )
    await consumer.start()

    gap = VesselGapDetector(producer, scorer, db, redis)
    gap_task = asyncio.create_task(gap.run())

    import time as _time
    _start_time = _time.monotonic()
    processed = 0
    errors = 0
    heartbeat_state = {
        "processed": 0,
        "errors": 0,
        "elapsed": lambda: _time.monotonic() - _start_time,
    }
    heartbeat_task = asyncio.create_task(_heartbeat_loop(heartbeat_state))

    logger.info("Enrichment Pipeline LIVE. Listening for raw telemetry...")
    
    try:
        while True:
            batches = await consumer.get_batch(timeout_ms=1000)
            if not batches:
                continue

            for tp, messages in batches.items():
                batch_to_write = []
                
                # ── 1. ASYNC CONCURRENT ENRICHMENT ───────────────────────────
                topic_to_enricher = {
                    Topics.RAW_MARITIME: enrichers_tuple[0],
                    Topics.RAW_AVIATION: enrichers_tuple[1],
                    Topics.RAW_NEWS: enrichers_tuple[2],
                    Topics.RAW_CYBER: enrichers_tuple[3],
                    Topics.RAW_TRADFI: enrichers_tuple[4],
                    Topics.RAW_CRYPTO: enrichers_tuple[5],
                    Topics.RAW_PREDICTION: enrichers_tuple[6]
                }
                
                # Group raw events by topic
                raw_events_by_topic = {}
                for msg in messages:
                    try:
                        raw_data = json.loads(msg.value.decode('utf-8'))
                        raw_event = RawEvent(**raw_data)
                        raw_events_by_topic.setdefault(msg.topic, []).append(raw_event)
                    except json.JSONDecodeError as e:
                        logger.error(f"POISON PILL JSON dropped: {e}", exc_info=True)
                        asyncio.create_task(dlq.send(Topics.DLQ, {"error": "Invalid JSON bytes", "topic": msg.topic, "raw": str(msg.value)}))
                
                enrich_tasks = []
                for topic, raw_events in raw_events_by_topic.items():
                    enricher = topic_to_enricher.get(topic)
                    if enricher:
                        if hasattr(enricher, "enrich_batch"):
                            enrich_tasks.append(enricher.enrich_batch(raw_events))
                        else:
                            # Fallback if enrich_batch is not implemented
                            async def _fallback_batch(e_batch, e_inst=enricher):
                                return await asyncio.gather(*[e_inst.enrich(e) for e in e_batch], return_exceptions=True)
                            enrich_tasks.append(_fallback_batch(raw_events))

                # Execute all batches simultaneously 
                results = await asyncio.gather(*enrich_tasks, return_exceptions=True)
                
                # ── 2. ASYNC CONCURRENT PRODUCER DISPATCH & ERROR ROUTING ──
                produce_tasks = []
                for batch_result, (topic, raw_events) in zip(results, list(raw_events_by_topic.items())):
                    if isinstance(batch_result, Exception):
                        logger.error(f"Batch enrichment failed for topic {topic}: {batch_result}", exc_info=batch_result)
                        for re in raw_events:
                            asyncio.create_task(
                                dlq.send(
                                    Topics.DLQ,
                                    {
                                        "error": f"Batch enrichment error: {batch_result}",
                                        "topic": topic,
                                        "raw": re.model_dump()
                                    }
                                )
                            )
                    elif isinstance(batch_result, list):
                        for enriched, re in zip(batch_result, raw_events):
                            if isinstance(enriched, NormalizedEvent):
                                batch_to_write.append(enriched)
                                produce_tasks.append(
                                    producer.send(
                                        Topics.ENRICHED_EVENTS,
                                        enriched.model_dump(),
                                        key=enriched.primary_entity.id,
                                    )
                                )
                            elif isinstance(enriched, Exception):
                                logger.error(f"Enrichment failed for event from {topic}: {enriched}", exc_info=enriched)
                                asyncio.create_task(
                                    dlq.send(
                                        Topics.DLQ,
                                        {
                                            "error": f"Enrichment event error: {enriched}",
                                            "topic": topic,
                                            "raw": re.model_dump()
                                        }
                                    )
                                )
                
                if produce_tasks:
                    await asyncio.gather(*produce_tasks, return_exceptions=True)
                    processed += len(produce_tasks)
                    heartbeat_state["processed"] = processed
                    if processed % 250 == 0:
                        logger.info(f"Processed {processed} successfully")

                # ── 3. FAULT-TOLERANT DB WRITES ──────────────────────────────
                batch_success = True
                if batch_to_write:
                    for attempt in range(3):
                        try:
                            # FIX: write_events_batch is async — call it directly,
                            # not via run_in_executor (which is for sync functions).
                            await db.write_events_batch(batch_to_write)
                            break # Success
                        except Exception as write_err:
                            if attempt == 2: 
                                errors += len(batch_to_write)
                                heartbeat_state["errors"] = errors
                                logger.error(f"FATAL DB WRITE ERROR: {write_err}. Routing batch to DLQ to prevent data loss.", exc_info=True)
                                # Send the failed DB batch with full payload to DLQ rather than crashing the service
                                dlq_tasks = [
                                    dlq.send(
                                        Topics.DLQ,
                                        {
                                            "error": f"DB_WRITE_FAILED: {write_err}",
                                            "topic": Topics.ENRICHED_EVENTS,
                                            "raw": e.model_dump()
                                        }
                                    )
                                    for e in batch_to_write
                                ]
                                await asyncio.gather(*dlq_tasks, return_exceptions=True)
                                batch_success = False
                            else:
                                await asyncio.sleep(2 ** attempt) 

                # ── 4. COMMIT ────────────────────────────────────────────────
                if batch_success:
                    await consumer.commit()
                
    except asyncio.CancelledError:
        logger.info("Shutdown signal received. Closing consumer...")
    except Exception as e:
        logger.critical(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        heartbeat_task.cancel()
        gap_task.cancel()
        try:
            await asyncio.gather(gap_task, heartbeat_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        logger.info(f"Final — processed: {processed}  errors: {errors}")

        await producer.close()
        await dlq.close()
        await consumer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())