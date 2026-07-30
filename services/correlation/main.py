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
            default_rules = [
                {
                    "rule_id": "rule_cyber_aviation_chokepoint",
                    "rule_name": "Cyber Aviation Chokepoint Disruption",
                    "trigger_event_type": ["breach_detected", "infra_exposed", "ransomware", "bgp_anomaly"],
                    "conditions": {"min_anomaly": 0.25},
                    "correlations": [{"event_types": ["flight_position", "flight_dark", "flight_anomaly", "vessel_position"], "hours": 48, "min_anomaly": 0.25}],
                    "alert_tier": "CRITICAL",
                    "expires_at": int(time.time()) + 315360000
                },
                {
                    "rule_id": "rule_financial_block_volume_spike",
                    "rule_name": "Equity Block & Options Convergence",
                    "trigger_event_type": ["equity_block", "price_anomaly"],
                    "conditions": {"min_anomaly": 0.25},
                    "correlations": [{"event_types": ["options_flow", "dark_pool", "insider_trade", "market_anomaly", "price_anomaly"], "hours": 48, "min_anomaly": 0.25}],
                    "alert_tier": "ELEVATED",
                    "expires_at": int(time.time()) + 315360000
                },
                {
                    "rule_id": "rule_insider_options_convergence",
                    "rule_name": "Insider Form 4 & Microstructure Convergence",
                    "trigger_event_type": "insider_trade",
                    "conditions": {"min_anomaly": 0.20},
                    "correlations": [{"event_types": ["options_flow", "equity_block", "price_anomaly", "dark_pool"], "hours": 72, "min_anomaly": 0.20}],
                    "alert_tier": "INTELLIGENCE",
                    "expires_at": int(time.time()) + 315360000
                },
                {
                    "rule_id": "rule_options_darkpool_surge",
                    "rule_name": "Dark Pool & Options Flow Accumulation",
                    "trigger_event_type": "options_flow",
                    "conditions": {"min_anomaly": 0.25},
                    "correlations": [{"event_types": ["dark_pool", "equity_block", "price_anomaly", "insider_trade"], "hours": 48, "min_anomaly": 0.25}],
                    "alert_tier": "ELEVATED",
                    "expires_at": int(time.time()) + 315360000
                },
                {
                    "rule_id": "rule_news_financial_impact",
                    "rule_name": "Headline Market Impact Convergence",
                    "trigger_event_type": ["headline", "narrative_cluster"],
                    "conditions": {"min_anomaly": 0.20},
                    "correlations": [{"event_types": ["equity_block", "price_anomaly", "options_flow", "crypto_trade", "crypto_liquidation", "dark_pool"], "hours": 24, "min_anomaly": 0.20}],
                    "alert_tier": "ALERT",
                    "expires_at": int(time.time()) + 315360000
                },
                {
                    "rule_id": "rule_crypto_equity_contagion",
                    "rule_name": "Crypto Liquidation & Equity Spillover",
                    "trigger_event_type": ["crypto_liquidation", "crypto_perp_funding"],
                    "conditions": {"min_anomaly": 0.25},
                    "correlations": [{"event_types": ["crypto_trade", "crypto_transfer", "equity_block", "price_anomaly"], "hours": 24, "min_anomaly": 0.25}],
                    "alert_tier": "ELEVATED",
                    "expires_at": int(time.time()) + 315360000
                },
                {
                    "rule_id": "rule_prediction_market_divergence",
                    "rule_name": "Prediction Market & Asset Shift",
                    "trigger_event_type": ["prediction_market_trade", "prediction_market"],
                    "conditions": {"min_anomaly": 0.20},
                    "correlations": [{"event_types": ["equity_block", "options_flow", "headline", "crypto_trade"], "hours": 48, "min_anomaly": 0.20}],
                    "alert_tier": "ALERT",
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
        event_type_str = event.type.value if hasattr(event.type, "value") else str(event.type)
        for rule in list(_dynamic_rules_cache.values()):
            try:
                if rule.get("expires_at", 0) < now:
                    continue
                    
                trig_spec = rule.get("trigger_event_type")
                if isinstance(trig_spec, list):
                    if event_type_str not in trig_spec:
                        continue
                elif isinstance(trig_spec, str):
                    if event_type_str != trig_spec:
                        continue
                else:
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
                    rule_tier_str = str(rule.get("alert_tier", "ALERT")).strip().upper()
                    alert_tier = AlertTier[rule_tier_str] if rule_tier_str in AlertTier.__members__ else AlertTier.ALERT
                    entity_id = event.primary_entity.id if event.primary_entity else "UNKNOWN"
                    entity_name = (event.primary_entity.name or entity_id) if event.primary_entity else "UNKNOWN"

                    # Extract rich context from supporting events
                    supporting_entity_names = list(dict.fromkeys(
                        e.get("entity_name") or e.get("entity_id", "Unknown")
                        for e in supporting_events[:10]
                        if e.get("entity_name") or e.get("entity_id")
                    ))[:5]
                    supporting_headlines = [
                        e.get("headline") or e.get("summary") or f"{e.get('type', 'event')}: {e.get('entity_name', 'Unknown')}"
                        for e in supporting_events[:3]
                    ]

                    cluster = CorrelationCluster(
                        trace_id=event.trace_id,
                        rule_id=rule.get("rule_id", "DYN_UNKNOWN"),
                        rule_name=rule.get("rule_name", "Dynamic AI Rule"),
                        alert_tier=alert_tier,
                        primary_domain=event.type.value.split("_")[0] if event.type and event.type.value else "general",
                        confidence_score=min(1.0, event.anomaly_score + 0.1),
                        summary_headline=f"🚨 {rule.get('rule_name', rule.get('rule_id'))}: {entity_name}",
                        supporting_headlines=supporting_headlines,
                        metrics_summary={
                            "supporting_event_count": len(supporting_events),
                            "domain_count": len(domains_triggered),
                            "domains": sorted(list(domains_triggered)),
                            "trigger_anomaly_score": event.anomaly_score,
                        },
                        trigger_event_id=event.event_id,
                        supporting_event_ids=[e["event_id"] for e in supporting_events[:10]],
                        primary_entity_id=entity_id,
                        primary_entity_name=entity_name,
                        entity_ids=[entity_id] + [e.get("entity_id", "") for e in supporting_events[:5] if e.get("entity_id")],
                        entity_names=list(dict.fromkeys([entity_name] + supporting_entity_names)),
                        description=(
                            f"Rule '{rule.get('rule_name', rule.get('rule_id'))}' triggered by '{entity_name}'. "
                            f"Correlated with {len(supporting_events)} events across {len(domains_triggered)} domains "
                            f"({', '.join(sorted(domains_triggered))}). "
                            f"Related: {'; '.join(supporting_headlines)}"
                        ),
                        tags=["dynamic_rule", "ai_generated", f"entity:{entity_name}", f"trigger_anomaly_{event.anomaly_score:.2f}"]
                            + [f"domain:{d}" for d in sorted(domains_triggered)]
                            + rule.get("tags", [])
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

    from shared.utils.ollama import OLLAMA_TIMEOUT
    connector = aiohttp.TCPConnector(limit=20)
    session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
    ollama_client = OllamaClient(session, redis_client=redis_client)
    soft_correlator = SoftCorrelator(ollama_client)
    asyncio.create_task(soft_correlator._load())

    cascade_engine = GeopoliticalCascadeEngine(window_seconds=3600)

    async def _stream_live_correlation(c: CorrelationCluster):
        try:
            headline = c.summary_headline or f"🚨 CORRELATION ALERT: {c.rule_name} (Tier: {c.alert_tier.value if hasattr(c.alert_tier, 'value') else c.alert_tier})"
            pe_id = c.primary_entity_id or (c.entity_ids[0] if c.entity_ids else "CORRELATION")
            pe_name = c.primary_entity_name or (c.entity_names[0] if c.entity_names else (c.rule_name or c.rule_id))
            primary_entities_list = []
            if c.entity_ids or c.entity_names:
                max_len = max(len(c.entity_ids), len(c.entity_names))
                for i in range(max_len):
                    eid = c.entity_ids[i] if i < len(c.entity_ids) else (c.entity_names[i] if i < len(c.entity_names) else pe_id)
                    ename = c.entity_names[i] if i < len(c.entity_names) else eid
                    primary_entities_list.append({"id": str(eid), "name": str(ename)})

            payload = {
                "event_id": str(c.correlation_id),
                "type": "correlation_alert",
                "occurred_at": c.detected_at.isoformat() if hasattr(c.detected_at, 'isoformat') else str(c.detected_at),
                "source": "Correlation Engine",
                "primary_entity_id": pe_id,
                "primary_entity_name": pe_name,
                "primary_entity": {"id": pe_id, "name": pe_name},
                "primary_entities": primary_entities_list or [{"id": pe_id, "name": pe_name}],
                "entity_id": pe_id,
                "entity_name": pe_name,
                "entity_ids": c.entity_ids or [pe_id],
                "entity_names": c.entity_names or [pe_name],
                "headline": headline,
                "summary": c.description,
                "supporting_headlines": c.supporting_headlines,
                "primary_domain": c.primary_domain or "cross_domain",
                "confidence_score": c.confidence_score,
                "anomaly_score": 0.95,
                "region": "GLOBAL",
                "tags": c.tags or [],
                "metrics": c.metrics_summary,
            }
            await redis_client.raw.publish("sentinel:events:live", json.dumps(payload))
        except Exception as pub_err:
            logger.debug(f"Failed to stream correlation live: {pub_err}")

    async def _process_correlation_event(event: NormalizedEvent):
        nonlocal corr_fired, processed
        try:
            # 1. Evaluate Geopolitical Cascade Engine
            cascade_cluster = cascade_engine.ingest_event(event)
            if cascade_cluster:
                if not cascade_cluster.primary_domain:
                    cascade_cluster.primary_domain = "geopolitical"
                if not cascade_cluster.summary_headline:
                    cascade_cluster.summary_headline = f"🌐 Cascade Alert: {cascade_cluster.rule_name}"
                await store.save_correlation(cascade_cluster)
                await producer.send(
                    Topics.CORRELATIONS,
                    cascade_cluster.model_dump(),
                    key=cascade_cluster.correlation_id,
                )
                await _stream_live_correlation(cascade_cluster)
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
                await _stream_live_correlation(c)
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
                    supp_headlines = [e.get("headline") or e.get("summary") or f"{e.get('type')}: {e.get('entity_name', 'Unknown')}" for e in similar_events[:3]]
                    
                    e_id = event.primary_entity.id if event.primary_entity else "UNKNOWN"
                    e_name = (event.primary_entity.name or e_id) if event.primary_entity else "UNKNOWN"

                    centrality_mult = 1.0
                    if e_id and e_id != "UNKNOWN":
                        try:
                            from shared.db import get_neo4j
                            import math
                            neo4j_client = await get_neo4j()
                            res = await neo4j_client.query("MATCH (e:Entity {id: $id})-[r]-(n) RETURN count(r) as degree", {"id": e_id.upper()})
                            if res and res[0].get("degree"):
                                degree = float(res[0]["degree"])
                                centrality_mult = 1.0 + math.log(1.0 + degree)
                        except Exception as cx:
                            logger.debug(f"Centrality lookup fallback for {e_id}: {cx}")

                    effective_score = len(supporting_ids) * centrality_mult
                    tier = AlertTier.CRITICAL if effective_score >= 4.0 else (AlertTier.INTELLIGENCE if effective_score >= 2.0 else AlertTier.ALERT)

                    cluster = CorrelationCluster(
                        trace_id=event.trace_id,
                        rule_id="SEMANTIC_001",
                        rule_name="Cross-Domain Semantic Convergence",
                        alert_tier=tier,
                        primary_domain=event.type.value.split("_")[0] if event.type and event.type.value else "semantic",
                        confidence_score=min(1.0, 0.70 + (0.1 * len(supporting_ids))),
                        summary_headline=f"🧠 Semantic Convergence: {e_name} across {len(similar_events)} cross-domain events",
                        supporting_headlines=supp_headlines,
                        metrics_summary={
                            "similar_event_count": len(similar_events),
                            "centrality_multiplier": round(centrality_mult, 2),
                            "effective_score": round(effective_score, 2),
                        },
                        trigger_event_id=event.event_id,
                        supporting_event_ids=supporting_ids,
                        primary_entity_id=e_id,
                        primary_entity_name=e_name,
                        entity_ids=[e_id],
                        entity_names=[e_name],
                        description=f"Neural embedding matched {len(similar_events)} highly similar cross-domain events for entity '{e_name}' (Centrality Multiplier: {centrality_mult:.2f}x). Anomalous semantic convergence detected.",
                        tags=["semantic_match", "cross_domain", "ai_cluster", f"entity:{e_name}", f"trigger_anomaly_{event.anomaly_score:.2f}", f"centrality_{centrality_mult:.2f}"]
                    )
                    
                    await store.save_correlation(cluster)
                    await producer.send(
                        Topics.CORRELATIONS,
                        cluster.model_dump(),
                        key=cluster.correlation_id,
                    )
                    await _stream_live_correlation(cluster)
                    corr_fired += 1

        except Exception as e:
            import traceback
            logger.error(f"Failed to process correlation for event {event.event_id}: {e}\n{traceback.format_exc()}")
            return

    try:
        while True:
            try:
                batches = await consumer.get_batch(timeout_ms=1000)
                
                if not batches:
                    continue

                for tp, messages in batches.items():
                    total_received += len(messages)
                    if total_received - last_logged_received >= 500:
                        logger.debug(f"Received batch of {len(messages)} events to correlate on partition {tp.topic}:{tp.partition}")
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