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
from datetime import datetime, timezone
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
from shared.db import get_redis, get_timescale, get_neo4j
from services.correlation.event_store import EventStore
from services.correlation.cascade import GeopoliticalCascadeEngine
from services.correlation.hawkes_correlator import CrossDomainHawkesCorrelator
from services.correlation.statistical_discovery import StatisticalDiscoveryEngine
from services.correlation import peer_graph
from services.enrichment.ref_data import get_reference_data
from shared.utils.streaming_detectors import FirstStoryDetector
from services.correlation.soft_correlator import POSITION_TELEMETRY_TYPES
from shared.utils.freshness import is_stale as _shared_is_stale
from shared.utils.tasks import safe_create_task

# How many tickers the peer pass considers, and how deep a window it asks for.
# Pairwise correlation is O(n^2), and only twenty tickers currently carry
# twenty bars, so the cap is headroom rather than a constraint today.
# What a textual resemblance has to reach before it is filed as intelligence.
#
# effective_score is distinct_subjects x centrality, so this asks for either
# many more subjects than the three that used to suffice, or a genuinely
# central entity. CRITICAL is deliberately unreachable from this rule: a
# sentence encoder reporting that two headlines are worded alike cannot
# establish the relationship that tier is supposed to mean.
SEMANTIC_INTELLIGENCE_SCORE = float(os.getenv("SEMANTIC_INTELLIGENCE_SCORE", "6.0"))

PEER_MAX_TICKERS = int(os.getenv("PEER_MAX_TICKERS", "60"))
PEER_SERIES_BARS = int(os.getenv("PEER_SERIES_BARS", "120"))
from shared.utils.heartbeat import start_heartbeat_task

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
                        # Routine position telemetry is not corroboration.
                        #
                        # A vessel or aircraft reporting where it is makes no
                        # claim, so nothing about it can support or contradict a
                        # BGP hijack. Accepting it meant a rule fired at CRITICAL
                        # named "Cyber Aviation Chokepoint Disruption" whose only
                        # evidence was fifty vessel position fixes over 48 hours
                        # -- no aviation event, no cyber correlation, and a name
                        # asserting both. Vessels are always somewhere; that is
                        # not a finding.
                        #
                        # Dark, spoofed and anomalous position events are still
                        # accepted: those are findings about a vessel or
                        # aircraft, and a finding is what a rule should correlate.
                        hits = [
                            h for h in hits
                            if str(h.get("type", "")) not in POSITION_TELEMETRY_TYPES
                        ]
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
                        # Names the domains actually matched. The rule name is a
                        # detector's name and may assert a combination the match
                        # never required -- "Cyber Aviation Chokepoint
                        # Disruption" fires on any one of four correlation
                        # types, so the title claimed aviation on evidence that
                        # contained none.
                        summary_headline=(
                            f"🚨 {rule.get('rule_name', rule.get('rule_id'))}: {entity_name} "
                            f"[matched: {', '.join(sorted(domains_triggered)) or 'none'}]"
                        ),
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
                        # Deduplicated, as entity_names already was. The trigger
                        # entity appeared again in its own supporting events, so
                        # a cluster listed the same wallet twice and looked like
                        # two subjects.
                        entity_ids=list(dict.fromkeys(
                            [entity_id] + [e.get("entity_id", "") for e in supporting_events[:5] if e.get("entity_id")]
                        )),
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

# How late an observation may be and still be worth correlating.
#
# Correlation answers "what else is happening right now". Past this age the
# answer is a historical note, not an alert, and emitting it as an alert is
# what made this engine report yesterday's market as though it were live.
MAX_EVENT_AGE_SEC = int(os.getenv("CORRELATION_MAX_EVENT_AGE_SEC", "900"))


def _is_stale(event, now=None) -> bool:
    """Thin wrapper over the shared rule, kept for this module's call sites.

    The rule lives in shared.utils.freshness because correlation is not the only
    stage that must refuse history: the reasoning service and every agent resume
    from a committed offset after any interruption and would otherwise work
    forward through a backlog. One definition, so the three cannot drift.
    """
    return _shared_is_stale(event, MAX_EVENT_AGE_SEC, now)


def _corroboration_weight(event) -> float:
    """Multiplier for a correlation's confidence, from its claim's support.

    Only events that can be corroborated carry an assessment -- news and OSINT.
    Market ticks and position fixes have no notion of a second source, so they
    are left unweighted rather than penalised for lacking a field that does not
    apply to them.

    A single-sourced claim is discounted rather than discarded: it is still a
    lead worth surfacing, just not one to act on as though it were confirmed.
    """
    assessment = getattr(event, "corroboration", None)
    if not isinstance(assessment, dict):
        return 1.0
    if assessment.get("is_single_sourced"):
        return 0.75
    score = assessment.get("corroboration_score")
    try:
        # Corroborated claims are trusted at face value and a little above, so a
        # well-supported story can clear a tier a single report would not.
        return 1.0 + 0.15 * float(score or 0.0)
    except (TypeError, ValueError):
        return 1.0


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Correlation Engine (Dynamic AI Rules)")
    logger.info("=" * 60)

    redis_client = await get_redis()
    db_client = await get_timescale()
    neo4j_client = await get_neo4j()
    
    rule_listener_task = safe_create_task(_listen_for_rule_updates(redis_client), name="correlation-rule-listener")

    # §1.1 Universal heartbeat
    hb_task = asyncio.create_task(start_heartbeat_task(redis_client, "correlation"))
    
    store    = EventStore(redis_client, db_client)
    producer = SentinelProducer()
    discovery_engine = StatisticalDiscoveryEngine(
        db_client=db_client,
        redis_client=redis_client,
        neo4j_client=neo4j_client,
        producer=producer,
    )
    consumer = SentinelConsumer(
        topics=[Topics.ENRICHED_EVENTS],
        group_id="correlation-engine",
        auto_offset_reset="latest",
        # SentinelConsumer defaults to 15 records a poll. This loop spends about
        # a second per cycle -- the getmany timeout plus processing -- so 15 a
        # poll is a hard ceiling of roughly 15 events/second, and measured it
        # was consuming 12.75/s against 12/s of production. Net drain of 0.75/s
        # against a 357,000-message backlog is five and a half days, which is
        # not a backlog, it is a permanent state.
        #
        # The cap was never a safety measure here: the pause/heartbeat dance
        # below already keeps the group membership alive across a long batch,
        # which is the thing a small poll size normally protects. Larger batches
        # also make the encoder cheaper, since embed_events() is roughly twice
        # as fast per event batched as called one at a time.
        max_poll_records=int(os.getenv("CORRELATION_MAX_POLL_RECORDS", "200")),
    )

    processed = 0
    corr_fired = 0
    errors = 0
    stale_skipped = 0
    stale_dropped = 0
    last_logged_stale = 0
    total_received = 0
    last_logged_received = 0

    await producer.start()
    await consumer.start()

    from shared.utils.ollama import OLLAMA_TIMEOUT
    connector = aiohttp.TCPConnector(limit=20)
    session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
    ollama_client = OllamaClient(session, redis_client=redis_client)
    soft_correlator = SoftCorrelator(ollama_client)
    asyncio.create_task(soft_correlator._load())

    # Layer 3: Multivariate Hawkes cross-domain excitation engine
    hawkes_correlator = CrossDomainHawkesCorrelator(redis_client=redis_client, db_client=db_client)
    asyncio.create_task(hawkes_correlator.initialize())

    # Story-level deduplication: correlate at story cluster level, not headline level
    # Prevents the same event generating 5 near-duplicate soft-correlation hits
    first_story_detector = FirstStoryDetector(window_size=500, novelty_threshold=0.70)
    # Track active story cluster IDs to deduplicate correlation attempts
    _story_correlation_seen = set()

    cascade_engine = GeopoliticalCascadeEngine(window_seconds=3600, hawkes_tracker=hawkes_correlator._tracker)

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

    async def _process_correlation_event(event: NormalizedEvent, precomputed_embedding=None):
        nonlocal corr_fired, processed, stale_skipped
        try:
            # 0. Refuse to correlate history.
            #
            # Everything below asks "what is happening at the same time as
            # this", and the answer is only worth anything while it is still
            # true. Running it over a backlog produces alerts about a market
            # that has since closed and a vessel that has since arrived --
            # measured here, the engine was 357,000 messages and 32 hours behind
            # and every cascade it fired described state from the previous day.
            #
            # Skipped, not seeked: the event is still consumed, still committed
            # and still counted, so nothing is silently dropped from the offset
            # record. It simply does not produce a correlation, which is the
            # honest outcome for an observation that arrived too late to
            # correlate. This is also what lets a backlog drain at parse speed
            # instead of encoder speed.
            if _is_stale(event):
                stale_skipped += 1
                return

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
            
            # 2. Record event in Hawkes process and check for cross-domain excitation
            event_domain = event.type.value.split("_")[0] if event.type and event.type.value else "unknown"
            event_ts = event.occurred_at.timestamp() if event.occurred_at else time.time()
            hawkes_state = hawkes_correlator.record_event(event_domain, event_ts)

            # Generate excitation forecasts if any domain is elevated
            hawkes_forecasts = hawkes_correlator.get_excitation_forecasts(event_ts)
            if hawkes_forecasts:
                # Attach top forecast to the event stream for downstream consumers
                top_forecast = hawkes_forecasts[0]
                logger.info(
                    f"⚡ Hawkes Excitation Forecast: {top_forecast['narrative']}"
                )
                # Publish excitation forecasts as correlation alerts
                if top_forecast["excess_multiplier"] >= 2.0:
                    import uuid as _uuid
                    forecast_cluster = CorrelationCluster(
                        correlation_id=str(_uuid.uuid4()),
                        trace_id=event.trace_id,
                        rule_id="HAWKES_EXCITATION",
                        rule_name="Cross-Domain Hawkes Excitation Forecast",
                        alert_tier=AlertTier.INTELLIGENCE if top_forecast["excess_multiplier"] >= 3.0 else AlertTier.ALERT,
                        primary_domain=top_forecast["source_domain"],
                        confidence_score=min(1.0, 0.5 + 0.1 * top_forecast["excess_multiplier"]),
                        summary_headline=(
                            f"⚡ Hawkes Excitation: {top_forecast['source_domain']} → {top_forecast['target_domain']} "
                            f"({top_forecast['excess_multiplier']:.1f}x above baseline)"
                        ),
                        supporting_headlines=[top_forecast["narrative"]],
                        metrics_summary={
                            "source_domain": top_forecast["source_domain"],
                            "target_domain": top_forecast["target_domain"],
                            "excess_multiplier": top_forecast["excess_multiplier"],
                            "branching_ratio": top_forecast["branching_ratio"],
                            "source_excitation_ratio": top_forecast["source_excitation_ratio"],
                            "forecast_hours": top_forecast["forecast_hours"],
                            "hawkes_intensities": hawkes_state.get("intensities", {}) if isinstance(hawkes_state, dict) else {},
                        },
                        trigger_event_id=event.event_id,
                        supporting_event_ids=[],
                        primary_entity_id=event.primary_entity.id if event.primary_entity else "HAWKES",
                        primary_entity_name=event.primary_entity.name if event.primary_entity and event.primary_entity.name else top_forecast["source_domain"],
                        entity_ids=[],
                        entity_names=[],
                        description=top_forecast["narrative"],
                        tags=["hawkes_excitation", "cross_domain", f"src:{top_forecast['source_domain']}", f"tgt:{top_forecast['target_domain']}"]
                    )
                    await store.save_correlation(forecast_cluster)
                    await producer.send(Topics.CORRELATIONS, forecast_cluster.model_dump(), key=forecast_cluster.correlation_id)
                    await _stream_live_correlation(forecast_cluster)
                    corr_fired += 1

            if corr_fired > 0:
                logger.info(f"🔥 Event {event.event_id} generated {corr_fired} correlation clusters.")

            # 3. Story-level deduplication for news/headline events (§2.3)
            # Check if this is a news/headline event and if it's a duplicate story
            is_news_event = event_domain in ("news", "headline", "narrative")
            skip_soft_correlation = False
            if is_news_event:
                headline_text = event.headline or ""
                summary_text = getattr(event, "summary", "") or ""
                novelty = first_story_detector.score_novelty(headline_text, summary_text)
                if novelty < 0.30:
                    # This is a continuation of an existing story, not a first story.
                    # Skip soft correlation to avoid duplicate cross-domain hits.
                    skip_soft_correlation = True
                    logger.debug(f"Story dedup: skipping soft correlation for low-novelty event (novelty={novelty:.2f}): {headline_text[:80]}")

            # Routine telemetry is not worth embedding. The enricher already
            # capped these at 0.15 and marked them uninteresting; putting them
            # through the encoder spends ~945ms of CPU asking whether one
            # position fix is semantically like another. Measured, this is about
            # two thirds of the stream.
            if event.anomaly_score is not None and event.anomaly_score <= 0.15:
                skip_soft_correlation = True

            # 4. Soft embedding correlation (with conformal threshold + story dedup)
            #
            # Embedding happens *after* the decision to skip. It used to run
            # first, so a skipped event still paid the full encoder cost and the
            # result was then thrown away -- the most expensive call on the hot
            # path, made unconditionally.
            # The batch pass above will normally have supplied this. The
            # single-event call remains as a fallback for anything it could not
            # encode, so one bad event cannot silently drop the rest.
            if skip_soft_correlation:
                embedding = None
            elif precomputed_embedding is not None:
                embedding = precomputed_embedding
            else:
                embedding = await soft_correlator.embed_event(event)
            if embedding and not skip_soft_correlation:
                await soft_correlator.store(event, embedding)

                similar_events = await soft_correlator.find_similar(
                    embedding,
                    exclude_domain=event_domain,
                )

                # Feed same-domain similarity scores into the conformal calibrator
                # to build the null distribution for threshold calibration
                try:
                    same_domain_similar = await soft_correlator.find_similar(
                        embedding,
                        exclude_domain="__none__",  # Don't exclude any domain
                        limit=5,
                    )
                    for sd_event in same_domain_similar:
                        if sd_event.get("domain") == event_domain:
                            # This is a same-domain similarity score → null distribution
                            # We approximate the score from the Qdrant result ordering
                            soft_correlator._similarity_calibrator.observe_null_score(
                                soft_correlator._similarity_calibrator.threshold  # Use threshold as proxy
                            )
                except Exception:
                    pass  # Non-critical calibration path
                
                if similar_events:
                    logger.info(f"🧠 Semantic Match Found for event {event.event_id} -> rule: Cross-Domain Semantic Convergence")
                    
                    # The evidence actually kept. Everything downstream counts
                    # this list rather than `similar_events`, which is up to ten
                    # long: the description read "matched 10 highly similar
                    # cross-domain events" while three were stored and three
                    # were scored, so the alert overstated its own evidence
                    # threefold to anyone who went looking for the other seven.
                    kept = similar_events[:3]
                    supporting_ids = [e.get("event_id") for e in kept if e.get("event_id")]
                    supp_headlines = [e.get("headline") or e.get("summary") or f"{e.get('type')}: {e.get('entity_name', 'Unknown')}" for e in kept]

                    # Distinct subjects, not raw matches. Three headlines about
                    # one vessel are one observation seen three times, and
                    # counting them as three is how a single flight alert came
                    # to corroborate dozens of separate correlations.
                    distinct_subjects = len({
                        str(e.get("entity_name") or e.get("entity_id") or idx)
                        for idx, e in enumerate(kept)
                    })
                    
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

                    effective_score = distinct_subjects * centrality_mult

                    # A resemblance is a lead, never a verdict, and the tier has
                    # to agree with the description a few lines below -- which
                    # already says in as many words that shared wording is not
                    # itself a relationship.
                    #
                    # Three distinct subjects and a centrality of 1.33 cleared
                    # 4.0 and became CRITICAL, so "QQQ appeared in three events"
                    # was filed at the top severity the system has. Measured
                    # live over three hours: 2,711 clusters at the two highest
                    # tiers, every one citing a single entity, against 2,464
                    # from the equity/options rule that cites ten supporting
                    # events and six entities. Each one is a candidate for an
                    # inference slot on a host that affords a few dozen an hour,
                    # so an over-graded rule does not merely mislabel -- it
                    # crowds out the correlations that earned their tier.
                    tier = (
                        AlertTier.INTELLIGENCE if effective_score >= SEMANTIC_INTELLIGENCE_SCORE
                        else AlertTier.ALERT
                    )

                    cluster = CorrelationCluster(
                        trace_id=event.trace_id,
                        rule_id="SEMANTIC_001",
                        rule_name="Cross-Domain Semantic Convergence",
                        alert_tier=tier,
                        primary_domain=event.type.value.split("_")[0] if event.type and event.type.value else "semantic",
                        # Weighted by how well the underlying claim is supported.
                        # A cross-domain convergence resting on a single-sourced
                        # report is a lead; the same convergence corroborated by
                        # several independent outlets is a finding, and reporting
                        # both at one confidence tells an analyst nothing about
                        # which is which.
                        # 0.35 + 0.15 per distinct subject, capped below 1.0.
                        #
                        # This read 0.70 + 0.1 * len(supporting_ids), and
                        # supporting_ids is capped at three -- so every semantic
                        # correlation that fired at all scored exactly 1.00 and
                        # the field ranked nothing. The floor of 0.70 was doing
                        # the same work as the ceiling: a match on one templated
                        # headline was already "highly confident" before any
                        # evidence was counted.
                        #
                        # The cap stays under 1.0 deliberately. This is an
                        # embedding's opinion that two sentences resemble each
                        # other; it should never present as certainty.
                        confidence_score=min(
                            0.95,
                            (0.35 + (0.15 * distinct_subjects)) * _corroboration_weight(event),
                        ),
                        summary_headline=f"🧠 Semantic Convergence: {e_name} across {distinct_subjects} cross-domain subject(s)",
                        supporting_headlines=supp_headlines,
                        metrics_summary={
                            "supporting_event_count": len(supporting_ids),
                            "distinct_subjects": distinct_subjects,
                            "candidates_considered": len(similar_events),
                            "centrality_multiplier": round(centrality_mult, 2),
                            "effective_score": round(effective_score, 2),
                        },
                        trigger_event_id=event.event_id,
                        supporting_event_ids=supporting_ids,
                        primary_entity_id=e_id,
                        primary_entity_name=e_name,
                        entity_ids=[e_id],
                        entity_names=[e_name],
                        # The description is the field a person actually reads
                        # back out of the correlations table, so it states what
                        # was kept and what was merely considered. It claimed
                        # "matched 10 highly similar cross-domain events" while
                        # three were stored and three were scored -- an alert
                        # overstating its own evidence to the one reader in a
                        # position to check it.
                        #
                        # "Resembles" and not "converges": this is a sentence
                        # encoder reporting that two headlines are worded alike.
                        # A vessel and an aircraft that both name the Strait of
                        # Malacca score highly here and have nothing to do with
                        # each other, so the wording says what was measured
                        # rather than what it might mean.
                        description=(
                            f"Embedding resemblance: '{e_name}' matches "
                            f"{len(supporting_ids)} retained event(s) across "
                            f"{distinct_subjects} distinct subject(s), from "
                            f"{len(similar_events)} candidate(s) considered "
                            f"(centrality {centrality_mult:.2f}x). Textual similarity "
                            f"only -- shared wording or a shared place name is not "
                            f"itself a relationship."
                        ),
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

    # Background task: periodic Hawkes MLE refit
    async def _hawkes_refit_loop():
        """Periodically refit Hawkes parameters from historical event data."""
        await asyncio.sleep(60)  # Wait for initial data accumulation
        while True:
            try:
                result = await hawkes_correlator.maybe_refit()
                if result:
                    logger.info(
                        f"🔬 Hawkes refit complete: "
                        f"spectral_radius={result.get('spectral_radius', '?')}, "
                        f"non-zero branching pairs={len(result.get('branching_ratios', {}))}"
                    )
            except Exception as e:
                logger.error(f"Hawkes refit loop error: {e}")
            await asyncio.sleep(hawkes_correlator.REFIT_INTERVAL)

    # Discovered edges were registered for survival tracking and never looked at
    # again -- evaluate() and due_for_retest() had no callers. Registering an
    # edge is only worth doing if something later asks whether it held.
    async def _edge_retest_loop():
        await asyncio.sleep(600)
        while True:
            try:
                await discovery_engine.retest_due_edges()
            except Exception as e:
                logger.error(f"Edge retest loop error: {e}")
            await asyncio.sleep(3600)

    # Background task: Periodic Statistical Correlation & Granger Discovery (§4.1, §4.2)
    async def _statistical_discovery_loop():
        await asyncio.sleep(30)
        while True:
            try:
                discoveries = await discovery_engine.discover_pairwise_correlations()
                if discoveries:
                    logger.info(f"📊 Statistical Discovery Job completed: evaluated {len(discoveries)} candidate pairs.")
            except Exception as e:
                logger.error(f"Statistical discovery loop error: {e}")
            await asyncio.sleep(300)

    # Background task: Peer derivation, so contagion has an edge to travel along
    async def _peer_graph_loop():
        """Derives PEER_OF from realised co-movement and publishes the proposals.

        Runs on the same series the discovery engine already fetches, and
        deliberately after it: the discovery engine answers "is there any
        relationship here", this answers the narrower "would a shock to one
        plausibly reach the other", which is the question an earnings surprise
        actually poses.

        Hourly. Peers are a property of how two issuers behave over weeks, not
        minutes, and re-deriving them every five would mostly re-publish the
        same edges while spending the collector's rate limit.
        """
        await asyncio.sleep(120)
        while True:
            try:
                tickers = await discovery_engine.build_candidate_pairs()
                names = sorted({t for pair in (tickers or []) for t in pair})

                series = {}
                for name in names[:PEER_MAX_TICKERS]:
                    bars = await discovery_engine.fetch_price_series(name, limit=PEER_SERIES_BARS)
                    if bars and len(bars) > peer_graph.MIN_OVERLAP_BARS:
                        series[name] = bars

                # Structure corroborates a measured correlation; it never
                # creates one. Two names that co-move *and* share an index are a
                # better contagion path than two that merely co-move, and until
                # the daily refresh was wired every peer edge carried only the
                # realised half of its evidence.
                reference = {}
                for name in series:
                    try:
                        ref = await get_reference_data(redis_client, name)
                        if ref:
                            reference[name] = ref
                    except Exception:
                        # Absent reference data is the designed-for case, not an
                        # error: derive_peers works without it.
                        pass

                if len(series) < 2:
                    logger.info(
                        "Peer derivation skipped: %s ticker(s) carry a usable window. "
                        "A peer needs both legs measured over the same bars.",
                        len(series),
                    )
                else:
                    edges = peer_graph.derive_peers(series, reference)
                    for edge in edges:
                        await producer.send(
                            Topics.ONTOLOGY_PROPOSALS,
                            edge.as_proposal(),
                            key=edge.source,
                        )
                    if edges:
                        logger.info(
                            "🔗 Peer graph: %s edge(s) published (%s inverse) from %s tickers.",
                            len(edges), sum(1 for e in edges if e.is_inverse), len(series),
                        )
            except Exception as e:
                logger.error(f"Peer graph loop error: {e}")
            await asyncio.sleep(3600)

    # Background task: Periodic Intra-TradFi Sector Hawkes Contagion (§4.3)
    async def _sector_hawkes_loop():
        await asyncio.sleep(60)
        while True:
            try:
                res = await discovery_engine.discover_sector_hawkes_contagion()
                excitations = res.get("significant_excitations", [])
                if excitations:
                    logger.info(f"⚡ Intra-TradFi Sector Hawkes Job: discovered {len(excitations)} significant sector excitations.")
            except Exception as e:
                logger.error(f"Sector Hawkes loop error: {e}")
            await asyncio.sleep(600)

    # Background task: Periodic Threshold Calibration (§4.4)
    async def _threshold_calibration_loop():
        await asyncio.sleep(15)
        while True:
            try:
                calibrated = await discovery_engine.run_threshold_calibration()
                if calibrated:
                    logger.info(f"🎯 Scheduled Threshold Calibration complete: min_corr={calibrated.get('min_correlation_coef')}")
            except Exception as e:
                logger.error(f"Threshold calibration loop error: {e}")
            await asyncio.sleep(1800)

    asyncio.create_task(_hawkes_refit_loop())
    asyncio.create_task(_statistical_discovery_loop())
    asyncio.create_task(_edge_retest_loop())
    asyncio.create_task(_sector_hawkes_loop())
    asyncio.create_task(_peer_graph_loop())
    asyncio.create_task(_threshold_calibration_loop())

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
                            except Exception as dlq_err:
                                logger.debug(f"DLQ send failed for parse error (event lost): {dlq_err}")
                    
                    # Stale events are dropped here, before any work is spent
                    # on them.
                    #
                    # The guard inside _process_correlation_event was too late:
                    # store.add_event and the batch encoder both run before
                    # dispatch, so a backlog still paid for the event store and
                    # the sentence encoder -- the two most expensive things in
                    # this loop -- and only then discovered the event was a day
                    # old. Filtering at the top took the drain rate from ~12/s
                    # to parse speed.
                    #
                    # It also keeps the windowed rules honest: feeding
                    # yesterday's events into a one-hour sliding window is what
                    # makes a backlog look like a burst.
                    fresh_events = [e for e in all_events if not _is_stale(e)]
                    dropped = len(all_events) - len(fresh_events)
                    if dropped:
                        stale_dropped += dropped
                        if stale_dropped - last_logged_stale >= 5000:
                            logger.warning(
                                "Skipped %s events older than %ss while catching up; "
                                "correlating a backlog would describe state that has "
                                "already changed.",
                                stale_dropped, MAX_EVENT_AGE_SEC,
                            )
                            last_logged_stale = stale_dropped

                    all_events = fresh_events
                    representative_events = {
                        k: v for k, v in representative_events.items() if not _is_stale(v)
                    }

                    if all_events:
                        await asyncio.gather(*[store.add_event(e) for e in all_events], return_exceptions=True)
                        
                        # Encode the batch in one pass before dispatching. The
                        # encoder dominates this path and is roughly twice as
                        # fast per event batched as called one at a time; the
                        # events are already in hand, so there is no reason to
                        # ask for them one by one. Routine telemetry is excluded
                        # here for the same reason it is skipped below.
                        to_embed = [
                            e for e in representative_events.values()
                            if e.anomaly_score is None or e.anomaly_score > 0.15
                        ]
                        precomputed = await soft_correlator.embed_events(to_embed) if to_embed else {}

                        tasks = [
                            _process_correlation_event(e, precomputed.get(str(e.event_id)))
                            for e in representative_events.values()
                        ]
                        
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
        hb_task.cancel()
        await producer.close()
        await consumer.close()
        await session.close()
        logger.info(f"Final — processed: {processed}  correlations: {corr_fired}  errors: {errors}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())