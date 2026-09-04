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
import math
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
from shared.utils.freshness import is_stale

# Reasoning is slower by nature than correlation -- a scenario is minutes of
# inference, not microseconds of window arithmetic -- so it gets a longer
# window than the 900s the correlation engine uses. An hour is still well
# inside "current" for a geopolitical or market judgement, and well outside the
# eight-hour backlog a single overnight suspend produced.
REASONING_MAX_CLUSTER_AGE_SEC = int(os.getenv("REASONING_MAX_CLUSTER_AGE_SEC", "3600"))
from shared.utils.inference_budget import InferenceBudget
from shared.utils.tasks import safe_create_task
from shared.utils.heartbeat import start_heartbeat_task

def _jsonable(value):
    """Plain data from Pydantic models, for a json.dumps that cannot see them.

    `json.dumps(scenario.hypotheses)` was handed a list of ScenarioHypothesis
    instances and raised "Object of type ScenarioHypothesis is not JSON
    serializable" -- caught by the enclosing handler, logged, and swallowed. The
    scenario was still broadcast to Kafka, so the pipeline looked healthy from
    every angle except the one that mattered: the scenarios table stood at zero
    rows for the entire life of the deployment.
    """
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "dict"):
        return value.dict()
    return value


def _narrative_summary(scenario) -> str:
    """A one-paragraph account of what this scenario says, for list views.

    Composed from what the scenario already contains rather than asked of a
    model: the column was empty on every row, and the platform affords about
    thirty-five inferences an hour, so spending one to restate an analysis it
    has already produced would be the wrong trade. The leading hypothesis is the
    one the confidence refers to, so it is the one worth naming here.
    """
    parts = [str(getattr(scenario, "headline", "") or "").strip()]

    hypotheses = list(getattr(scenario, "hypotheses", None) or [])
    if hypotheses:
        lead = max(
            hypotheses,
            key=lambda h: float(getattr(h, "probability", 0) or 0),
        )
        label = str(getattr(lead, "label", "") or "").strip()
        probability = getattr(lead, "probability", None)
        if label:
            if probability is not None:
                parts.append(f"Leading hypothesis: {label} ({probability}%).")
            else:
                parts.append(f"Leading hypothesis: {label}.")

    confidence = getattr(scenario, "confidence_overall", None)
    if confidence is not None:
        parts.append(f"Overall confidence {confidence}%.")

    significance = str(getattr(scenario, "significance", "") or "").strip()
    if significance:
        parts.append(significance)

    return " ".join(p for p in parts if p)[:2000]


async def _save_scenario(db, scenario):
    """Persists the AI-generated scenario to PostgreSQL for frontend retrieval."""
    try:
        await db.execute("""
            INSERT INTO scenarios (
                scenario_id, correlation_id, status,
                headline, significance, hypotheses,
                recommended_monitoring, confidence_overall,
                confidence_rationale, supporting_event_ids,
                trace_id, narrative_summary
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::uuid[],$11::uuid,$12)
        """, 
            scenario.scenario_id,
            scenario.correlation_id,
            scenario.status.value,
            scenario.headline,
            scenario.significance,
            # The list itself, not json.dumps(...) of it. The connection pool
            # registers a jsonb codec whose encoder is already json.dumps, so a
            # pre-serialised string is encoded a second time and lands as a
            # jsonb *string* rather than an array -- jsonb_typeof() returns
            # "string" and every reader that indexes into it gets nothing.
            _jsonable(scenario.hypotheses),
            scenario.recommended_monitoring,
            scenario.confidence_overall,
            scenario.confidence_rationale,
            # The insert named nine columns and the table defines thirteen, so
            # this one stayed null on every row ever written.
            [str(e) for e in (scenario.supporting_event_ids or [])],
            # Carried on the model since it was written and never persisted, so
            # a scenario could not be joined back to the events and inferences
            # that produced it -- which is the one thing a trace id is for.
            #
            # None rather than "" when absent: the column is uuid, and an empty
            # string fails the cast and takes the whole insert with it.
            (str(getattr(scenario, "trace_id", "") or "").strip() or None),
            _narrative_summary(scenario),
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
    hb_shared_task = safe_create_task(start_heartbeat_task(redis_client, "reasoning"))
    
    sem = asyncio.Semaphore(3)

    # Shared with the agent swarm: one Ollama, one budget.
    # A reserved lane, not the swarm's shared slot.
    #
    # Reasoning runs the same model as the agents-fast tier, so it shared one
    # budget key with five agents consuming a far busier stream. They re-claimed
    # the slot before it expired -- sampled every ten seconds it was never free
    # -- and because this service sheds a cluster whenever the slot is busy, it
    # shed every single one. Zero scenarios were persisted in the lifetime of
    # the deployment while the correlation topic grew past 299,000 messages.
    #
    # The cooldown is short because a scenario is the platform's headline
    # output; the lane bounds concurrency to one reasoning inference at a time.
    _budget = InferenceBudget(
        redis_client,
        os.getenv("AGENT_MODEL", "qwen2.5:1.5b"),
        cooldown_sec=int(os.getenv("REASONING_COOLDOWN_SEC", "120")),
        lane="reasoning",
    )
    _shed = 0
    _stale = 0

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
                eligible = []
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
                                
                            # Agent analysis shares this topic and is not a cluster.
                            #
                            # stock_correlation_agent declares Topics.CORRELATIONS
                            # as its output_topic, so its cross-asset analysis
                            # lands here and was fed straight into
                            # CorrelationCluster(**raw_data). Its payload shares
                            # none of the five required fields -- rule_id,
                            # rule_name, alert_tier, trigger_event_id,
                            # description -- so every one raised five validation
                            # errors, went to the DLQ, exhausted its retries and
                            # was written to failed_events as permanently
                            # failed. The agent ran, produced its analysis,
                            # published it, and the consumer discarded all of it.
                            #
                            # Kept the way intel briefs are: cached for the
                            # generator to read as context, rather than parsed
                            # as something it never was.
                            if "agent" in raw_data and "correlation_id" not in raw_data:
                                agent_name = str(raw_data.get("agent") or "unknown")
                                try:
                                    await redis_client.raw.set(
                                        f"sentinel:agents:correlation_analysis:{agent_name}",
                                        json.dumps(raw_data),
                                        ex=3600,
                                    )
                                except Exception as e:
                                    logger.debug(f"Could not cache {agent_name} analysis: {e}")
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
                            # Age before capacity. A cluster older than the
                            # window cannot be reasoned about usefully however
                            # much capacity exists, and checking it first means
                            # a backlog drains at parse speed instead of
                            # occupying the budget peek.
                            #
                            # This service held 26,405 correlations after a
                            # single overnight suspend. Without this it works
                            # forward through all of them, spending minutes of
                            # inference each on describing a world that has
                            # already moved.
                            if is_stale(cluster, REASONING_MAX_CLUSTER_AGE_SEC):
                                _stale += 1
                                if _stale % 500 == 1:
                                    logger.warning(
                                        "Reasoning skipped %s cluster(s) older than %ss. "
                                        "Analysing a backlog describes a world that has "
                                        "already changed.",
                                        _stale, REASONING_MAX_CLUSTER_AGE_SEC,
                                    )
                                continue

                            # Held for ranking rather than admitted on arrival.
                            #
                            # Admission was: first fresh cluster to find the slot
                            # free wins. About 0.29% of correlations reach
                            # reasoning, so the platform's scarcest resource --
                            # two model passes at several minutes each, roughly
                            # 36 an hour -- was being spent on an arbitrary
                            # sample of the stream rather than on its best 0.29%.
                            # A CRITICAL cross-domain cluster citing thirty
                            # events lost the slot to whatever ordinary match
                            # happened to be parsed a millisecond earlier.
                            #
                            # Kafka already delivers in batches, so the choice
                            # costs nothing: rank what is in hand and spend the
                            # slot on the best of it.
                            eligible.append((cluster, raw_data))

                        except Exception as parse_e:
                            logger.error(f"Failed parsing reasoning message: {parse_e}", exc_info=True)
                            await producer.send(Topics.DLQ, {"error": str(parse_e), "raw": str(message.value)})
                # Spend the available capacity on the best of the batch.
                for cluster, raw_data in sorted(
                    eligible, key=_reasoning_priority, reverse=True
                ):
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
 
# Tier weights for reasoning admission. A tier is the correlation layer's own
# judgement of how much a cluster matters, and it was reaching the scheduler as
# no input at all.
_REASONING_TIER_WEIGHT = {
    "CRITICAL": 1.0,
    "INTELLIGENCE": 0.75,
    "ALERT": 0.5,
    "MONITOR": 0.25,
}


def _reasoning_priority(item) -> float:
    """How much a cluster is worth spending an inference slot on.

    Reasoning admits roughly 36 clusters an hour against a stream producing far
    more, so admission is a ranking problem and was being answered by arrival
    order. Everything this reads is already carried on the cluster; nothing here
    costs a query.

      tier        the correlation layer's own severity judgement
      confidence  how well the cluster believes it is evidenced
      breadth     how many events it actually cites
      domains     whether it genuinely spans domains, which is the thing this
                  platform exists to find and the rarest property in the stream

    Deterministic and pure, so two replicas facing the same batch make the same
    choice and the ordering can be reasoned about without running the service.
    """
    cluster, _raw = item
    tier = getattr(getattr(cluster, "alert_tier", None), "value", None) or str(
        getattr(cluster, "alert_tier", "") or ""
    )
    tier_w = _REASONING_TIER_WEIGHT.get(str(tier).upper(), 0.25)

    try:
        confidence = float(getattr(cluster, "confidence_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    n_support = len(getattr(cluster, "supporting_event_ids", None) or [])
    breadth = min(1.0, math.log1p(max(0, n_support - 1)) / math.log1p(49))

    metrics = getattr(cluster, "metrics_summary", None) or {}
    try:
        n_domains = int(metrics.get("domain_count") or 0) if isinstance(metrics, dict) else 0
    except (TypeError, ValueError):
        n_domains = 0
    cross_domain = 0.0 if n_domains <= 1 else min(1.0, (n_domains - 1) / 2.0)

    return round(
        0.40 * tier_w + 0.25 * confidence + 0.15 * breadth + 0.20 * cross_domain,
        6,
    )


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AI REASONING SERVICE")
    logger.info("=" * 60)
 
    db              = await get_timescale()
    redis_client    = await get_redis()
    # Publish this process's metrics, so inference can be accounted for.
    #
    # MetricsCollector.increment("ollama_calls_total") has run in this service
    # since it was written, and never left the process: bind_redis() had exactly
    # one caller, a collector-specific helper, so only the collectors ever
    # published. The module's own docstring describes cross-process aggregation
    # as the problem it solves, and the services doing all the inference were
    # not participating in it.
    #
    # The cost was not a missing dashboard. It made "how much model time does
    # each agent consume" unanswerable from inside, which left parsing Ollama's
    # access log by container IP as the only option -- and Docker reassigns
    # those on restart, so the attribution was wrong in a way that took two
    # corrections to notice.
    try:
        from shared.utils.metrics import bind_redis
        await bind_redis(redis_client, service_name=os.getenv("SENTINEL_SERVICE", "reasoning"))
    except Exception as e:
        logger.debug("Metrics binding skipped: %s", e)

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