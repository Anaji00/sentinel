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

import warnings
warnings.filterwarnings("ignore", message=".*Failed to initialize NumPy.*")

from shared.utils.logging import setup_sentinel_logging

logger = setup_sentinel_logging("enrichment", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import RawEvent, NormalizedEvent, CrossDomainSignal
from shared.db import get_redis, get_timescale, get_neo4j
from shared.db.bootstrap import bootstrap_database

from services.enrichment.anomaly_scorer import DynamicAnomalyScorer
from services.enrichment.db_writer import DBWriter
from services.enrichment.graph_writer import GraphWriter
from services.enrichment.entity_resolver import EntityResolver
from services.enrichment.gap_detector import VesselGapDetector

from shared.utils.tasks import safe_create_task

# --- THE NEW ENRICHERS ---
from services.enrichment.enrichers.maritime import MaritimeEnricher
from services.enrichment.enrichers.aviation import AviationEnricher
from services.enrichment.enrichers.news import NewsEnricher
from services.enrichment.enrichers.cyber import CyberEnricher
from services.enrichment.enrichers.tradfi import TradFiEnricher
from services.enrichment.enrichers.crypto import CryptoEnricher
from services.enrichment.enrichers.prediction import PredictionEnricher



async def _attach_cross_domain_signals(events: list, redis_client):
    """
    Pre-computed cross-domain signal injection.
    Cache recent high-anomaly events in Redis by entity/region, and attach
    matching signals from OTHER domains to each NormalizedEvent before publishing.
    """
    if not redis_client or not events:
        return

    try:
        pipe = redis_client.raw.pipeline()
        for evt in events:
            if not isinstance(evt, NormalizedEvent):
                continue

            entity_id = evt.primary_entity.id if evt.primary_entity else None
            region = evt.region

            # Cache current high-anomaly event (TTL 4h)
            if evt.anomaly_score >= 0.5:
                sig_data = json.dumps({
                    "event_id": str(evt.event_id),
                    "event_type": str(evt.type.value if hasattr(evt.type, "value") else evt.type),
                    "domain": evt.source.split("_")[0] if "_" in evt.source else evt.source,
                    "entity_id": entity_id or "unknown",
                    "entity_name": evt.primary_entity.name if evt.primary_entity else None,
                    "headline": evt.headline,
                    "anomaly_score": evt.anomaly_score,
                    "occurred_at": evt.occurred_at.isoformat() if evt.occurred_at else None,
                    "region": region,
                })
                if entity_id:
                    pipe.lpush(f"sentinel:recent_signals:entity:{entity_id.upper()}", sig_data)
                    pipe.ltrim(f"sentinel:recent_signals:entity:{entity_id.upper()}", 0, 9)
                    pipe.expire(f"sentinel:recent_signals:entity:{entity_id.upper()}", 14400)
                if region:
                    pipe.lpush(f"sentinel:recent_signals:region:{region.lower()}", sig_data)
                    pipe.ltrim(f"sentinel:recent_signals:region:{region.lower()}", 0, 9)
                    pipe.expire(f"sentinel:recent_signals:region:{region.lower()}", 14400)

            # Query existing cached signals
            if entity_id:
                pipe.lrange(f"sentinel:recent_signals:entity:{entity_id.upper()}", 0, 5)
            elif region:
                pipe.lrange(f"sentinel:recent_signals:region:{region.lower()}", 0, 5)

        results = await pipe.execute()

        # Parse results and populate cross_domain_signals
        res_idx = 0
        for evt in events:
            if not isinstance(evt, NormalizedEvent):
                continue

            entity_id = evt.primary_entity.id if evt.primary_entity else None
            region = evt.region

            # Skip write pipeline commands indices
            if evt.anomaly_score >= 0.5:
                if entity_id:
                    res_idx += 3
                if region:
                    res_idx += 3

            if entity_id or region:
                raw_signals = results[res_idx] if res_idx < len(results) else []
                res_idx += 1
                if raw_signals:
                    current_domain = evt.source.split("_")[0] if "_" in evt.source else evt.source
                    cross_signals = []
                    for item in raw_signals:
                        try:
                            s = json.loads(item if isinstance(item, str) else item.decode("utf-8"))
                            if s.get("domain") != current_domain and s.get("event_id") != str(evt.event_id):
                                cross_signals.append(CrossDomainSignal(**s))
                        except Exception:
                            pass
                    evt.cross_domain_signals = cross_signals[:3]
    except Exception as e:
        logger.debug(f"Cross-domain signal attachment warning: {e}")


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
    safe_create_task(_ofac_sync_loop(), name="ofac-sync")
    
    # Wait for databases to come online
    producer = SentinelProducer()
    dlq = SentinelProducer()
    await producer.start()
    await dlq.start()
    
    scorer = DynamicAnomalyScorer(redis)
    db = DBWriter(timescale)
    graph = GraphWriter(producer)
    neo4j = await get_neo4j()
    resolver = EntityResolver(redis, neo4j, producer=producer)

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
            Topics.RAW_CYBER, Topics.RAW_TRADFI, Topics.RAW_CRYPTO, 
            Topics.RAW_PREDICTION, Topics.RAW_RADAR
        ],
        group_id="enrichment-service",
    )
    await consumer.start()

    gap = VesselGapDetector(producer, scorer, db, redis)
    gap_task = safe_create_task(gap.run(), name="vessel-gap-detector")

    import time as _time
    _start_time = _time.monotonic()
    processed = 0
    errors = 0
    heartbeat_state = {
        "processed": 0,
        "errors": 0,
        "elapsed": lambda: _time.monotonic() - _start_time,
    }
    heartbeat_task = safe_create_task(_heartbeat_loop(heartbeat_state), name="enrichment-heartbeat")

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
                    Topics.RAW_PREDICTION: enrichers_tuple[6],
                    Topics.RAW_RADAR: enrichers_tuple[4]
                }
                
                # Group raw events by topic
                raw_events_by_topic = {}
                pending_dlq_tasks = []
                for msg in messages:
                    try:
                        raw_data = json.loads(msg.value.decode('utf-8'))
                        if isinstance(raw_data, dict) and not raw_data.get("source"):
                            raw_data["source"] = msg.topic.split(".")[-1] if "." in msg.topic else msg.topic
                        raw_event = RawEvent(**raw_data)
                        raw_events_by_topic.setdefault(msg.topic, []).append(raw_event)
                    except Exception as e:
                        logger.error(f"POISON PILL / Invalid RawEvent dropped: {e}", exc_info=True)
                        pending_dlq_tasks.append(dlq.send(Topics.DLQ, {"error": f"Invalid RawEvent: {e}", "topic": msg.topic, "raw": str(msg.value)}))
                
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
                            pending_dlq_tasks.append(
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
                        def _flatten(items):
                            flat = []
                            for item in items:
                                if isinstance(item, list):
                                    flat.extend(_flatten(item))
                                elif item is not None:
                                    flat.append(item)
                            return flat

                        vessel_positions_to_write = []
                        flat_events = _flatten(batch_result)
                        for enriched in flat_events:
                            if isinstance(enriched, NormalizedEvent):
                                batch_to_write.append(enriched)
                                if enriched.vessel_data and getattr(enriched.vessel_data, 'mmsi', None):
                                    vd = enriched.vessel_data
                                    vessel_positions_to_write.append((
                                        vd.mmsi,
                                        enriched.occurred_at,
                                        enriched.latitude or 0.0,
                                        enriched.longitude or 0.0,
                                        getattr(vd, 'speed_knots', 0.0) or 0.0,
                                        getattr(vd, 'heading', 0) or 0,
                                        getattr(vd, 'nav_status', 'underway') or 'underway'
                                    ))
                            elif isinstance(enriched, Exception):
                                logger.error(f"Enrichment item failed for topic {topic}: {enriched}", exc_info=enriched)
                                pending_dlq_tasks.append(
                                    dlq.send(
                                        Topics.DLQ,
                                        {
                                            "error": f"Item enrichment error: {enriched}",
                                            "topic": topic,
                                        }
                                    )
                                )
                        if vessel_positions_to_write:
                            pending_dlq_tasks.append(db.write_vessel_positions_batch(vessel_positions_to_write))

                batch_success = True

                # Await all pending DLQ sends & vessel writes before proceeding
                if pending_dlq_tasks:
                    pending_res = await asyncio.gather(*pending_dlq_tasks, return_exceptions=True)
                    for r in pending_res:
                        if isinstance(r, Exception):
                            logger.error(f"DLQ delivery / aux dispatch failed: {r}", exc_info=r)
                            batch_success = False  # Gate offset commit on DLQ delivery success

                # Pre-computed cross-domain signal injection pass
                if batch_to_write:
                    await _attach_cross_domain_signals(batch_to_write, redis)

                for enriched in batch_to_write:
                    entity_key = enriched.primary_entity.id if (enriched.primary_entity and enriched.primary_entity.id) else "unknown"
                    produce_tasks.append(
                        producer.send(
                            Topics.ENRICHED_EVENTS,
                            enriched.model_dump(),
                            key=entity_key,
                        )
                    )
                    try:
                        live_dict = enriched.model_dump(mode="json")
                        live_dict["event_id"] = str(enriched.event_id)
                        safe_create_task(redis.raw.publish("sentinel:events:live", json.dumps(live_dict)), name="live-feed-pub")
                    except Exception as pub_err:
                        logger.debug(f"Redis live feed publish bypass: {pub_err}")

                if produce_tasks:
                    produce_results = await asyncio.gather(*produce_tasks, return_exceptions=True)
                    failed_produce_dlq_tasks = []
                    successful_produces = 0
                    for enriched, produce_res in zip(batch_to_write, produce_results):
                        if isinstance(produce_res, Exception):
                            logger.error(
                                f"Kafka produce to {Topics.ENRICHED_EVENTS} failed for event {enriched.event_id}: {produce_res}",
                                exc_info=produce_res,
                            )
                            errors += 1
                            batch_success = False
                            failed_produce_dlq_tasks.append(
                                dlq.send(
                                    Topics.DLQ,
                                    {
                                        "error": f"KAFKA_PRODUCE_FAILED: {produce_res}",
                                        "topic": Topics.ENRICHED_EVENTS,
                                        "raw": enriched.model_dump(),
                                    },
                                )
                            )
                        else:
                            successful_produces += 1

                    if failed_produce_dlq_tasks:
                        dlq_res = await asyncio.gather(*failed_produce_dlq_tasks, return_exceptions=True)
                        for r in dlq_res:
                            if isinstance(r, Exception):
                                logger.error(f"DLQ delivery for failed Kafka produce failed: {r}", exc_info=r)

                    processed += successful_produces
                    heartbeat_state["processed"] = processed
                    heartbeat_state["errors"] = errors
                    if processed % 250 == 0:
                        logger.info(f"Processed {processed} successfully")

                # ── 3. FAULT-TOLERANT DB WRITES ──────────────────────────────
                if batch_to_write:
                    for attempt in range(3):
                        try:
                            # FIX: write_events_batch is async — call it directly,
                            # not via run_in_executor (which is for sync functions).
                            await db.write_events_batch(batch_to_write)
                            # Broadcast enriched events to Redis PubSub for real-time WebSocket live feed
                            try:
                                pub_pipe = redis.raw.pipeline()
                                for evt in batch_to_write:
                                    payload = json.dumps(evt.model_dump(), default=str)
                                    pub_pipe.publish("sentinel:events:live", payload)
                                await pub_pipe.execute()
                            except Exception as pub_err:
                                logger.debug(f"Redis pubsub publish warning: {pub_err}")
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