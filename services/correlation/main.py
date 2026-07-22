"""
services/correlation/main.py

Consumes enriched.events.
Executes Dynamic JSON Correlation Rules from Redis.
Emits CorrelationCluster to correlations.detected when a rule fires.
"""

import aiohttp
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
import inspect
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.utils.logging import setup_sentinel_logging

logger = setup_sentinel_logging("correlation", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logging.getLogger("httpx").setLevel(logging.WARNING)

from shared.utils.ollama import OllamaClient
from services.correlation.soft_correlator import SoftCorrelator
from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import NormalizedEvent, CorrelationCluster, AlertTier
from shared.db import get_redis, get_timescale
from services.correlation.event_store import EventStore
from services.correlation.cascade import GeopoliticalCascadeEngine

_dynamic_rules_cache = {}

async def _listen_for_rule_updates(redis_client):
    """Subscribes to Redis Pub/Sub to instantly hot-reload dynamic rules into memory."""
    global _dynamic_rules_cache
    
    try:
        rules_raw = await redis_client.raw.hvals("sentinel:correlation:dynamic_rules")
        if rules_raw:
            for raw in rules_raw:
                rule = json.loads(raw)
                if "rule_id" in rule:
                    _dynamic_rules_cache[rule["rule_id"]] = rule
        else:
            logger.info("⚡ No dynamic rules in Redis. Seeding default baseline correlation rules...")
            default_rules = [
                {
                    "rule_id": "rule_cyber_aviation_chokepoint",
                    "trigger_event_type": "cyber_threat",
                    "conditions": {"min_anomaly": 0.1},
                    "correlations": [{"event_types": ["flight_position", "vessel_position"], "hours": 48, "min_anomaly": 0.0}],
                    "alert_tier": "CRITICAL",
                    "expires_at": int(time.time()) + 315360000
                },
                {
                    "rule_id": "rule_macro_market_volatility",
                    "trigger_event_type": "macro_indicator",
                    "conditions": {"min_anomaly": 0.0},
                    "correlations": [{"event_types": ["market_anomaly", "crypto_trade", "price_anomaly"], "hours": 24, "min_anomaly": 0.0}],
                    "alert_tier": "ELEVATED",
                    "expires_at": int(time.time()) + 315360000
                },
                {
                    "rule_id": "rule_news_market_anomaly",
                    "trigger_event_type": "news_article",
                    "conditions": {"min_anomaly": 0.0},
                    "correlations": [{"event_types": ["market_anomaly", "crypto_trade", "price_anomaly"], "hours": 12, "min_anomaly": 0.0}],
                    "alert_tier": "WATCH",
                    "expires_at": int(time.time()) + 315360000
                }
            ]
            for d_rule in default_rules:
                await redis_client.raw.hset("sentinel:correlation:dynamic_rules", d_rule["rule_id"], json.dumps(d_rule))
                _dynamic_rules_cache[d_rule["rule_id"]] = d_rule
        logger.info(f"Loaded {len(_dynamic_rules_cache)} dynamic rules into memory.")
    except Exception as e:
        logger.error(f"Failed to load dynamic rules on startup: {e}")

    pubsub = redis_client.raw.pubsub()
    await pubsub.subscribe("sentinel:correlation:rule_updates")
    logger.info("Listening for dynamic rule hot-reloads...")
    
    async for message in pubsub.listen():
        if message["type"] == "message":
            try:
                rule_data = json.loads(message["data"])
                rule_id = rule_data.get("rule_id")
                
                if rule_data.get("deprecated"):
                    if rule_id in _dynamic_rules_cache:
                        del _dynamic_rules_cache[rule_id]
                        logger.info(f"Hot-reloaded: Deprecated rule {rule_id}")
                else:
                    _dynamic_rules_cache[rule_id] = rule_data
                    logger.info(f"Hot-reloaded: Updated rule {rule_id}")
            except Exception as e:
                logger.error(f"PubSub message parse error: {e}")

async def evaluate_dynamic_rules(event: NormalizedEvent, store: EventStore) -> list[CorrelationCluster]:
    clusters = []
    try:
        now = int(time.time())
        for rule in list(_dynamic_rules_cache.values()):
            try:
                if rule.get("expires_at", 0) < now:
                    continue
                if event.type.value != rule.get("trigger_event_type"):
                    continue
                    
                cond = rule.get("conditions", {})
                if cond.get("min_anomaly", 0) > event.anomaly_score:
                    continue
                if cond.get("region") and event.region not in cond["region"]:
                    continue
                    
                supporting_events = []
                domains_triggered = set()
                for corr in rule.get("correlations", []):
                    hits = await store.get_recent(
                        corr.get("event_types"),
                        hours=corr.get("hours", 48),
                        min_anomaly=corr.get("min_anomaly", 0.0),
                        tags=corr.get("tags"),
                        region=corr.get("region")
                    )
                    if hits:
                        supporting_events.extend(hits)
                        domains_triggered.update([h.get("type", "").split("_")[0] for h in hits])
                        
                if len(supporting_events) > 0:
                    cluster = CorrelationCluster(
                        trace_id=event.trace_id,
                        rule_id=rule.get("rule_id", "DYN_UNKNOWN"),
                        rule_name=rule.get("rule_name", "Dynamic AI Rule"),
                        alert_tier=AlertTier(rule.get("alert_tier", "alert").upper()),
                        trigger_event_id=event.event_id,
                        supporting_event_ids=[e["event_id"] for e in supporting_events[:10]],
                        entity_ids=[event.primary_entity.id] if event.primary_entity else [],
                        description=f"Dynamic rule '{rule.get('rule_name')}' triggered. Correlated with {len(supporting_events)} events across {len(domains_triggered)} domains.",
                        tags=["dynamic_rule", "ai_generated", f"trigger_anomaly_{event.anomaly_score:.2f}"] + rule.get("tags", [])
                    )
                    clusters.append(cluster)
            except Exception as e:
                logger.error(f"Failed to parse or eval dynamic rule: {e}")
    except Exception as e:
        logger.error(f"Failed to evaluate dynamic rules: {e}")
    return clusters

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Correlation Engine (Dynamic AI Rules)")
    logger.info("=" * 60)

    redis_client = await get_redis()
    db_client = await get_timescale()
    
    asyncio.create_task(_listen_for_rule_updates(redis_client))
    
    store    = EventStore(redis_client, db_client)
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
    session = aiohttp.ClientSession(connector=connector)
    ollama_client = OllamaClient(session)
    soft_correlator = SoftCorrelator(ollama_client)
    asyncio.create_task(soft_correlator._load())

    _start_time = time.monotonic()

    async def _heartbeat():
        nonlocal processed, corr_fired, errors
        while True:
            await asyncio.sleep(60)
            elapsed = time.monotonic() - _start_time
            rate = processed / elapsed if elapsed > 0 else 0
            logger.info(
                f"⏱ HEARTBEAT | processed={processed} "
                f"correlations={corr_fired} errors={errors} "
                f"rate={rate:.1f}/s uptime={int(elapsed)}s"
            )

    heartbeat_task = asyncio.create_task(_heartbeat())

    total_received = 0
    last_logged_received = 0

    cascade_engine = GeopoliticalCascadeEngine(window_seconds=3600)

    async def _process_correlation_event(event: NormalizedEvent):
        nonlocal corr_fired, processed
        try:
            # 1. Evaluate Geopolitical Cascade Engine
            cascade_cluster = cascade_engine.ingest_event(event)
            if cascade_cluster:
                await store.save_correlation(cascade_cluster)
                await producer.send(
                    Topics.CORRELATIONS,
                    cascade_cluster.model_dump(),
                    key=cascade_cluster.correlation_id,
                )
                corr_fired += 1
                logger.info(f"🚨 Geopolitical Cascade Alert Fired: {cascade_cluster.correlation_id}")

            dynamic_clusters = await evaluate_dynamic_rules(event, store)
            for c in dynamic_clusters:
                await store.save_correlation(c)
                await producer.send(
                    Topics.CORRELATIONS,
                    c.model_dump(),
                    key=c.correlation_id,
                )
                corr_fired += 1
                logger.info(f"⚡ Dynamic Rule {c.rule_id} Fired for event {event.event_id}")
            
            embedding = await soft_correlator.embed_event(event)
            if embedding:
                await soft_correlator.store(event, embedding)
                
                similar_events = await soft_correlator.find_similar(
                    embedding, 
                    exclude_domain=event.type.value.split("_")[0]
                )
                
                if similar_events:
                    logger.info(f"🧠 Semantic Match Found for event {event.event_id} -> rule: Cross-Domain Semantic Convergence")
                    
                    supporting_ids = [e.get("event_id") for e in similar_events[:3] if e.get("event_id")]
                    
                    cluster = CorrelationCluster(
                        trace_id=event.trace_id,
                        rule_id="SEMANTIC_001",
                        rule_name="Cross-Domain Semantic Convergence",
                        alert_tier=AlertTier.INTELLIGENCE if len(supporting_ids) >= 2 else AlertTier.ALERT,
                        trigger_event_id=event.event_id,
                        supporting_event_ids=supporting_ids,
                        entity_ids=[event.primary_entity.id] if event.primary_entity else [],
                        description=f"Neural embedding matched {len(similar_events)} highly similar cross-domain events. Anomalous semantic convergence detected.",
                        tags=["semantic_match", "cross_domain", "ai_cluster", f"trigger_anomaly_{event.anomaly_score:.2f}"]
                    )
                    
                    await store.save_correlation(cluster)
                    await producer.send(
                        Topics.CORRELATIONS,
                        cluster.model_dump(),
                        key=cluster.correlation_id,
                    )
                    corr_fired += 1
                    
        except Exception as e:
            logger.error(f"Failed to process correlation for event {event.event_id}: {e}", exc_info=True)
            return

    try:
        while True:
            try:
                batches = await consumer.get_batch(timeout_ms=1000)
                
                if not batches:
                    continue

                for tp, messages in batches.items():
                    total_received += len(messages)
                    if total_received - last_logged_received >= 250:
                        logger.info(f"Received batch of {len(messages)} events to correlate on partition {tp.topic}:{tp.partition}")
                        last_logged_received = total_received
                    
                    all_events = []
                    representative_events = {}
                    
                    for message in messages:
                        try:
                            raw_data = json.loads(message.value.decode('utf-8'))
                            event = NormalizedEvent(**raw_data)
                            all_events.append(event)
                            
                            dedup_key = f"{event.primary_entity.id if event.primary_entity else 'unknown'}_{event.type.value}"
                            if dedup_key not in representative_events:
                                representative_events[dedup_key] = event
                            else:
                                if event.anomaly_score > representative_events[dedup_key].anomaly_score:
                                    representative_events[dedup_key] = event
                                    
                        except Exception as e:
                            errors += 1
                            logger.error(f"Failed to parse event: {e}")
                            try:
                                await producer.send(Topics.DLQ, data={"topic": Topics.ENRICHED_EVENTS, "error": str(e), "raw": str(message.value)})
                            except Exception:
                                pass
                    
                    if all_events:
                        await asyncio.gather(*[store.add_event(e) for e in all_events], return_exceptions=True)
                        
                        tasks = [_process_correlation_event(e) for e in representative_events.values()]
                        
                        # 1. Pause assigned partitions to prevent new messages and allow heartbeats
                        assigned = consumer._c.assignment()
                        if assigned:
                            consumer._c.pause(*assigned)
                            
                        # 2. Run processing in a background task
                        async def _run_tasks():
                            await asyncio.gather(*tasks, return_exceptions=True)
                        processing_task = asyncio.create_task(_run_tasks())
                        
                        # 3. Yield heartbeats by polling consumer in a loop until done
                        while not processing_task.done():
                            try:
                                # Poll empty (partitions are paused) to send heartbeats
                                await consumer.get_batch(timeout_ms=1000)
                            except Exception as pe:
                                logger.warning(f"Consumer heartbeat poll warning: {pe}")
                                
                        # 4. Resume assigned partitions
                        if assigned:
                            consumer._c.resume(*assigned)
                            
                        # Await the processing task to bubble any cancellation or clean errors
                        await processing_task
                        
                        processed += len(all_events)
                        if processed % 100 < len(all_events):
                            logger.info(f"Heartbeat | Processed {processed} events | Total correlations: {corr_fired}")
                
                    await consumer.commit()

            except Exception as outer_err:
                logger.error(f"Kafka consumer network/batch error. Backing off. {outer_err}")
                await asyncio.sleep(5)
                
    except asyncio.CancelledError:
        logger.info("Shutting down correlation engine...")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        heartbeat_task.cancel()
        await producer.close()
        await consumer.close()
        await session.close()
        logger.info(f"Final — processed: {processed}  correlations: {corr_fired}  errors: {errors}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())