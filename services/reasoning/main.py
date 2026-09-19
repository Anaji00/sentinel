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
from typing import Optional
from shared.utils.agent_conclusions import (
    AGENT_CORRELATION_ANALYSIS_PREFIX,
    HAWKES_BRANCHING_RATIOS_KEY,
    MACRO_INVERSE_CORRELATION_PREFIX,
    MACRO_RATES_REGIME_KEY,
    MACRO_SPREAD_2Y10Y_KEY,
    TRADFI_BACKFILL_REPORT_KEY,
)
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

# Supporting events a cluster must cite before it can be scheduled for scenario
# generation. One is enough to make it a claim about something observed; zero
# means the trigger id refers to nothing in the store.
MIN_SUPPORTING_EVENTS = 1

# The most supporting events any publisher attaches to a cluster.
#
# `services.correlation.main` truncates at ten on the rule path and at three on
# the semantic path; nothing writes more. It is the top of the breadth scale
# below, and it lives here as a name so the next change to either cap is a
# change to one number rather than a silent narrowing of the ranking.
MAX_CITED_EVENTS = 10

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
from shared.utils.dlq_payload import encode_dlq_payload
from shared.utils.backpressure import declare_pressure, clear_pressure
from shared.utils.rule_feedback import conversion_rates, conversion_weight
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

    It says what the headline does not: which hypothesis the confidence refers
    to, how confident the system is, and why the scenario matters.
    """
    # The headline is deliberately not repeated here.
    #
    # This opened with it, and every surface that renders a narrative_summary
    # renders the headline directly above it -- so the list view showed
    # "XRPUSDT Signals Reveal Geopolitical Shifts and Cryptocurrency Trends"
    # twice, once as the title and again as the first clause of its own summary.
    # A summary that begins by restating the thing it sits under wastes the only
    # line a reader gets to learn something new.
    parts = []

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
                trace_id, narrative_summary,
                primary_entity_id, primary_entity_name
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::uuid[],$11::uuid,$12,$13,$14)
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
            # The subject. `Scenario` resolves both of these from `entity_ids`
            # and `entity_names` in a validator on every scenario it builds,
            # and the table had no column to put them in -- so the feed's
            # scenario card and its detail modal, which both read
            # `primary_entity_name`, showed "Multi-Entity" for every scenario
            # the platform has ever produced, including the single-entity ones.
            (scenario.primary_entity_id or None),
            (scenario.primary_entity_name or None),
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

    # How often this rule has actually been borne out, alongside the examples.
    # Cheap -- one aggregate over an indexed join -- and it is the single most
    # calibrating fact the prompt can carry.
    try:
        context["rule_base_rate"] = await library.outcome_base_rate(cluster.rule_id)
    except Exception as e:
        logger.debug("Base rate unavailable for %s: %s", cluster.rule_id, e)

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
        topics=[Topics.CORRELATIONS, Topics.INTEL_BRIEFS],
        group_id="reasoning-service-group",
        auto_offset_reset="latest",
    )
    producer = SentinelProducer(service_name="reasoning")
    await consumer.start()
    await producer.start()

    # A TCPConnector, an aiohttp.ClientSession and an OllamaClient were built
    # here and never referenced again. The session was never closed either, so
    # it was held for the process lifetime and surfaced as an unclosed-session
    # warning at shutdown. The real inference path is in scenario_generator,
    # which lazily creates its own session and its own client -- and passes a
    # `model` argument this construction omitted, which is what dated it.

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
    _queue_expired = 0

    # Per-rule conversion record, refreshed in the background.
    #
    # Read by the ranker, which stays pure: it is handed the map rather than
    # fetching it, so two replicas facing the same batch and the same priors
    # still make the same choice. Empty until the first refresh, and an empty
    # map means every rule competes on the cluster's own merits -- which is the
    # behaviour this had before, so a cold start loses nothing.
    _conversion_priors: dict = {}

    async def _conversion_prior_loop():
        nonlocal _conversion_priors
        while True:
            try:
                rates = await conversion_rates(db, days=2)
                if rates:
                    _conversion_priors = rates
                    logger.info(
                        "Rule conversion priors refreshed for %d rule(s); "
                        "best %.3f%%, worst %.3f%%",
                        len(rates), 100 * max(rates.values()), 100 * min(rates.values()),
                    )
            except Exception as e:
                logger.debug("Conversion prior refresh failed: %s", e)
            await asyncio.sleep(1800)

    safe_create_task(_conversion_prior_loop(), name="reasoning-conversion-priors")

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

        # Release the lane the dispatch claimed.
        #
        # The claim is sized for a worker that never returns, so without this
        # the slot sits idle from the moment the synthesis finishes until the
        # cooldown expires. Unconditional, and before the error branches: a
        # synthesis that failed has stopped using the model just as surely as
        # one that succeeded, and holding the lane for a cluster that is already
        # in the DLQ would shed live work to protect nothing.
        safe_create_task(_budget.finish(), name="reasoning-budget-finish")

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
            # Freshness is a property of the moment work is done, not of the
            # moment it was queued.
            #
            # The admission check above proves a cluster was fresh when it was
            # accepted. It says nothing about when it is synthesised, and the
            # gap is the whole queue: MAX_INFLIGHT_SYNTHESES clusters are
            # admitted against a tier that completes roughly eight an hour, so
            # a slot is reached hours after it was claimed. Measured on this
            # deployment, every one of the last ten scenarios was synthesised
            # 3.5 to 4.3 hours after its correlation was detected -- against a
            # declared ceiling of one hour. The service was not ignoring its
            # own bound; it was checking it at the only point where it was
            # guaranteed to pass.
            #
            # Re-checking here costs nothing and makes the bound true. A
            # cluster that expired while waiting is dropped at its turn rather
            # than argued through two model passes, which also lets the ones
            # behind it run sooner.
            nonlocal _queue_expired
            if is_stale(cluster, REASONING_MAX_CLUSTER_AGE_SEC):
                _queue_expired += 1
                if _queue_expired % 100 == 1:
                    logger.warning(
                        "Reasoning dropped %s cluster(s) that expired while "
                        "queued (older than %ss at their turn). Admission is "
                        "outrunning synthesis.",
                        _queue_expired, REASONING_MAX_CLUSTER_AGE_SEC,
                    )
                return None
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
                            
                            if tp.topic == Topics.INTEL_BRIEFS:
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
                                        f"{AGENT_CORRELATION_ANALYSIS_PREFIX}{agent_name}",
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
                            await producer.send(Topics.DLQ, {"error": str(parse_e), "raw": encode_dlq_payload(message.value)})
                # Spend the available capacity on the best of the batch.
                for cluster, raw_data in sorted(
                    eligible, key=lambda it: _reasoning_priority(it, _conversion_priors), reverse=True
                ):
                    # Claim the lane, do not merely look at it.
                    #
                    # This was `is_available()`, which is documented as a
                    # read-only peek that deliberately does not claim. Nothing
                    # else in the tree ever claimed this lane either --
                    # `try_acquire` has one call site and it is the agent tier,
                    # on the un-laned key -- so
                    # `sentinel:inference:budget:reasoning:qwen2.5:1.5b` had
                    # never existed. Sampled four times its TTL was -2.
                    #
                    # A peek at a key nothing sets is always free, so the gate
                    # below never refused, `declare_pressure` was never called,
                    # the Redis flag stayed unset and the correlation engine's
                    # semantic-path pause -- wired on both sides -- could not
                    # fire. Four mechanisms downstream of a condition that was
                    # unsatisfiable by construction: "Reasoning shed N clusters"
                    # logged 0 times in 9.4 hours of uptime.
                    #
                    # The lane itself is right and stays. Sharing the agents'
                    # key was tried and is what the lane exists to undo: five
                    # agents on a busier stream re-claimed the slot before it
                    # expired, this service sheds whenever the slot is busy, and
                    # it shed every cluster for the lifetime of the deployment.
                    # The bound this lane is supposed to enforce -- one
                    # reasoning inference at a time -- is only real if something
                    # takes it.
                    if not await _budget.try_acquire(
                        score=_reasoning_priority((cluster, raw_data)),
                        domain=getattr(cluster, "primary_domain", None),
                    ):
                        _shed += 1

                        # Say so upstream.
                        #
                        # This is the moment the service knows it cannot keep
                        # up, and until now it was the only thing that knew.
                        # The correlation layer went on building, embedding,
                        # persisting and publishing clusters at full rate into
                        # a tier managing roughly thirty-six an hour -- every
                        # one of those costs paid for work discarded here.
                        #
                        # Declared on a short TTL, so recovery or a crash stops
                        # throttling producers within ninety seconds.
                        safe_create_task(
                            declare_pressure(
                                redis_client, "reasoning",
                                reason="inference budget exhausted",
                            ),
                            name="reasoning-backpressure",
                        )

                        if _shed % 500 == 1:
                            logger.info(
                                f"Reasoning shed {_shed} clusters to stay within "
                                f"inference capacity (consumer stays current)"
                            )
                        continue

                    # Admitting work means the budget is available again.
                    #
                    # The TTL would clear the declaration eventually, but a
                    # consumer that has recovered should say so rather than
                    # leave its producers throttled for the rest of the window.
                    safe_create_task(
                        clear_pressure(redis_client, "reasoning"),
                        name="reasoning-backpressure-clear",
                    )

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

# How long after start the first sweep runs, and the interval thereafter.
#
# The loop slept a full interval before its first execution, so a service
# redeployed more often than every thirty minutes never ran this at all -- and
# during this audit the reasoning service was restarted many times an hour. The
# expiry decay was repaired, deployed, and still showed zero denied scenarios,
# because the sweep that applies it had not once been reached.
#
# The short initial delay is to let the database pool and consumer settle; it is
# not a throttle.
TRACKER_FIRST_RUN_SEC = 120
TRACKER_INTERVAL_SEC = 1800


async def _tracker_loop(tracker: ScenarioTracker):
    await asyncio.sleep(TRACKER_FIRST_RUN_SEC)
    while True:
        try:
            await tracker.check_all()
        except Exception as e:
            logger.error(f"Scenario Tracker error: {e}")
        await asyncio.sleep(TRACKER_INTERVAL_SEC)
 
# Tier weights for reasoning admission. A tier is the correlation layer's own
# judgement of how much a cluster matters, and it was reaching the scheduler as
# no input at all.
# Severity, as the platform already orders it.
#
# This table named MONITOR, which is not an AlertTier, and omitted ELEVATED,
# which is -- so every ELEVATED cluster fell to the 0.25 default and ranked
# below both of the tiers beneath it. Measured over the scenario set: ELEVATED
# findings scored 0.31 against ALERT at 0.52, on a queue that admits about
# thirty-six clusters an hour and sheds the rest.
#
# Derived from `AlertTier` rather than restated, so a tier added to the enum
# cannot silently acquire the floor weight. The rank-to-weight curve is linear
# from 0.25 at the least severe to 1.0 at the most, which preserves the three
# weights the old table got right.
def _tier_weights() -> dict:
    from shared.models.events import AlertTier

    # The canonical order, matching `event_store._tier_rank`.
    order = ["WATCH", "ALERT", "ELEVATED", "INTELLIGENCE", "CRITICAL"]
    declared = [t.value.upper() for t in AlertTier]
    ranked = [name for name in order if name in declared]
    # A tier the enum gained and this order does not know about sorts last
    # rather than vanishing.
    ranked += [name for name in declared if name not in ranked]

    span = max(1, len(ranked) - 1)
    return {
        name: round(0.25 + 0.75 * (i / span), 4)
        for i, name in enumerate(ranked)
    }


_REASONING_TIER_WEIGHT = _tier_weights()

# Every tier the enum declares has a weight, checked at import for the same
# reason the rule-evidence guard is: a severity band silently ranked at the
# floor is invisible in the output -- the queue simply admits fewer of them.
assert not {
    t.value.upper() for t in __import__(
        "shared.models.events", fromlist=["AlertTier"]
    ).AlertTier
} - set(_REASONING_TIER_WEIGHT), "an AlertTier has no reasoning weight"

# What a cluster keeps when the score behind it measured nothing at all.
#
# A warm-up score is weaker evidence, not absent evidence, so this is a floor
# rather than a gate: an unmeasured CRITICAL cross-domain cluster still outranks
# a fully-measured ordinary one, which is the correct ordering.
REASONING_COVERAGE_FLOOR = 0.6


def _reasoning_priority(item, priors: Optional[dict] = None) -> float:
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

    # A cluster citing no events is not a weak claim about the world; it is not
    # a claim about the world.
    #
    # breadth is a 0.15-weighted term, so an evidence-free cluster lost at most
    # 0.15 of priority and still competed for scenario generation -- the
    # multi-minute inference that is the scarcest resource this platform has.
    # The quant engine publishes exactly such clusters: supporting_event_ids=[]
    # with a synthetic trigger id matching no stored event.
    if n_support < MIN_SUPPORTING_EVENTS:
        return 0.0

    # Normalised against what a cluster can actually cite.
    #
    # This divided by log1p(49), as though fifty supporting events were the
    # top of the scale. No publisher can produce fifty: the rule path caps
    # `supporting_event_ids` at ten, the semantic path at three, and the quant
    # path emits none. So the term reached at most 0.589 of its range and a
    # maximally-evidenced cluster was scored as though it were two thirds
    # evidenced -- the same unreachable-denominator mistake as the correlation
    # engine's own breadth term, in the ranker that decides which clusters are
    # worth an inference at all.
    breadth = min(
        1.0,
        math.log1p(max(0, n_support - 1)) / math.log1p(MAX_CITED_EVENTS - 1),
    )

    metrics = getattr(cluster, "metrics_summary", None) or {}
    try:
        n_domains = int(metrics.get("domain_count") or 0) if isinstance(metrics, dict) else 0
    except (TypeError, ValueError):
        n_domains = 0
    cross_domain = 0.0 if n_domains <= 1 else min(1.0, (n_domains - 1) / 2.0)

    ranked = 0.40 * tier_w + 0.25 * confidence + 0.15 * breadth + 0.20 * cross_domain

    # What backed the trigger's score, where the cluster says.
    #
    # The streaming detectors report coverage and the enrichers now carry it
    # onto the event, but the inference budget still admitted on score alone --
    # so a 0.4 from a warm-up curve and a 0.4 from a full percentile window
    # competed for the same slot as equals. That was the half of the coverage
    # repair that was never done: the reporting half worked, the ranking half
    # did not exist.
    #
    # Absent coverage is not zero coverage. A cluster from a path that does not
    # report it keeps its priority exactly as before, so this changes the
    # ordering only where there is something to read.
    coverage = metrics.get("evidence_coverage") if isinstance(metrics, dict) else None
    if coverage is not None:
        try:
            frac = max(0.0, min(1.0, float(coverage)))
        except (TypeError, ValueError):
            frac = None
        if frac is not None:
            # A floor, not a gate: a warm-up score is weaker evidence, not no
            # evidence, and a cold detector on a CRITICAL cross-domain cluster
            # should still outrank a warm one on an ordinary single-domain
            # match.
            ranked *= REASONING_COVERAGE_FLOOR + (1.0 - REASONING_COVERAGE_FLOOR) * frac

    # What this rule has produced before.
    #
    # Everything above is a property of the cluster in hand. None of it is a
    # memory of whether this rule's clusters have ever become a scenario, so
    # HAWKES_EXCITATION -- 5,786 firings in 48 hours converting at 0.05% --
    # competed on equal terms with chokepoint evasion at 5.26%, and won on
    # volume. Two rules are 77% of everything this tier is offered.
    #
    # A multiplier with a floor, not a gate: a rule that has never converted
    # still competes, and a rule with no record competes exactly as it did
    # before. Applied last so it scales the judgement rather than replacing it.
    ranked *= conversion_weight(str(getattr(cluster, "rule_id", "") or ""), priors)

    return round(ranked, 6)


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
    tracker_producer = SentinelProducer(service_name="reasoning-tracker")
    await tracker_producer.start()
    # Redis, which this was never given.
    #
    # `ScenarioTracker.__init__` takes (db, producer, redis) and this passed
    # two, so `self._redis` was None for the life of the service. Everything
    # that needs it returned early and silently: the calibration outcome write
    # closed in Phase 4.12, the open-questions offer, and the backfill. Three
    # mechanisms reported closed in this audit, all inert, none of them saying
    # so -- 562 resolved scenarios against 0 calibration samples was the visible
    # end of it.
    tracker         = ScenarioTracker(db, tracker_producer, redis_client)
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