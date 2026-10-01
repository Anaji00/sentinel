"""
services/telemetry-worker/main.py

Subscribes to agents.telemetry and writes agent lifecycle and performance metrics to TimescaleDB.
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.utils.logging import setup_sentinel_logging, ThrottledLogger

logger = setup_sentinel_logging("telemetry-worker", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
throttled_logger = ThrottledLogger(logger, default_interval_sec=10.0)

from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.db import get_timescale, get_redis
from shared.utils.metrics import MetricsCollector
from shared.utils.heartbeat import start_heartbeat_task
from shared.utils.tasks import safe_create_task
from shared.utils.entity_prediction_resolver import resolve_entity_predictions
from shared.utils.quiet_failures import swallowed

try:
    from drift_scheduler import ModelDriftScheduler
except ImportError:
    try:
        from services.telemetry_worker.drift_scheduler import ModelDriftScheduler
    except ImportError:
        import importlib.util
        spec = importlib.util.spec_from_file_location("drift_scheduler", Path(__file__).parent / "drift_scheduler.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        ModelDriftScheduler = mod.ModelDriftScheduler

async def init_db(db):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS agent_telemetry (
            id SERIAL PRIMARY KEY,
            agent_name VARCHAR(255) NOT NULL,
            task_id VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            system_prompt_length INT,
            user_prompt_length INT,
            latency_ms FLOAT,
            output_payload JSONB,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS agent_predictions (
            id SERIAL PRIMARY KEY,
            prediction_id VARCHAR(255),
            correlation_id VARCHAR(255),
            predicted_target VARCHAR(255),
            confidence FLOAT,
            simulated_vector JSONB,
            recommendations JSONB,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

async def process_telemetry(consumer, db):
    while True:
        try:
            batches = await consumer.get_batch(timeout_ms=1000)
            if not batches:
                continue
                
            for tp, messages in batches.items():
                for message in messages:
                    try:
                        data = json.loads(message.value.decode('utf-8'))
                        if message.topic == Topics.AGENTS_PREDICTIONS:
                            await db.execute("""
                                INSERT INTO agent_predictions (
                                    prediction_id, correlation_id, predicted_target,
                                    confidence, simulated_vector, recommendations, occurred_at,
                                    agent_name
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            """,
                                # Keys the wargamer actually publishes.
                                #
                                # Five of the six columns read names that appear
                                # in no message it sends. Measured over all 30
                                # rows: prediction_id was the literal
                                # "wargame_sim" every time, correlation_id was
                                # empty every time -- so no row could be joined
                                # back to the cluster that caused it --
                                # confidence was 0.0 every time, and both jsonb
                                # columns were empty. Only predicted_target
                                # carried anything.
                                #
                                # A live message reads:
                                #   simulation_run_id, primary_vulnerability_isolated,
                                #   cascade_failure_probability, predicted_next_target_entity_id,
                                #   remediation_recommendation, agent, agent_run_id,
                                #   source_correlation_id
                                # The older names are kept as fallbacks so a
                                # different producer on this topic still lands.
                                str(data.get("simulation_run_id") or data.get("prediction_id")
                                    or data.get("trace_id") or "wargame_sim"),
                                str(data.get("source_correlation_id") or data.get("correlation_id") or ""),
                                str(data.get("predicted_next_target_entity_id") or data.get("predicted_target") or "unknown"),
                                # cascade_failure_probability is the only number
                                # the wargamer quantifies, published 0-100. It is
                                # the model's probability of cascade failure, not
                                # a self-reported confidence in the prediction;
                                # it is stored here because the column is
                                # otherwise dead, and readers should treat it as
                                # that probability on a 0-1 scale.
                                _prediction_confidence(data),
                                # The objects themselves. The pool's jsonb codec
                                # encodes with json.dumps already, so serialising
                                # here stored a jsonb *string* -- true of all 30
                                # rows for both columns.
                                data.get("simulated_trajectory_vector") or [],
                                _recommendations(data),
                                datetime.now(timezone.utc),
                                # The message says which agent it came from and
                                # this insert did not store it, so every row
                                # this worker has ever written is unattributed:
                                # 50 of them, against 24 from the agents' own
                                # `record_prediction` path, which does set it.
                                # A scorecard cannot credit or debit a
                                # prediction it cannot trace to an author.
                                str(data.get("agent") or "") or None,
                            )
                            MetricsCollector.increment("agent_predictions_consumed_total")
                            logger.info("🔮 Persisted agent prediction for target: %s", data.get("predicted_next_target_entity_id"))
                        else:
                            await db.execute("""
                                INSERT INTO agent_telemetry (
                                    agent_name, task_id, status, 
                                    system_prompt_length, user_prompt_length, 
                                    latency_ms, output_payload, occurred_at
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            """,
                                data.get("agent", "unknown"),
                                data.get("task_id") or _event_task_id(data),
                                data.get("status") or _event_status(data),
                                data.get("system_prompt_length"),
                                data.get("user_prompt_length"),
                                data.get("latency_ms"),
                                _telemetry_payload(data),
                                datetime.now(timezone.utc)
                            )
                    except Exception as parse_e:
                        throttled_logger.error("parse_error", f"Failed parsing telemetry/prediction message: {parse_e}")
            await consumer.commit()
        except Exception as batch_error:
            logger.error(f"Batch execution failed. Backing off 5s. Error: {batch_error}")
            await asyncio.sleep(5)

# Topics.TELEMETRY carries two shapes, and only one of them is telemetry.
#
# `_execute_with_telemetry` sends an inference record -- status, task_id,
# prompt lengths, latency. `rule_agent` sends domain events on the same topic:
# {agent, event: "rule_created", rule_id, rule_name, timestamp}. This worker
# treated every non-prediction message as an inference record, so a rule
# creation became a row reading agent_name=rule_synthesizer, task_id=unknown,
# status=unknown and NULL for everything else. Measured: 7 such rows, and the
# rule_id and rule_name on every one of them were discarded.
#
# Those rows are the record of the rule synthesiser succeeding, which is the
# one output that agent exists for. Handled here rather than in the producer
# because the flattening is this boundary's, and any producer can hit it.
def _is_domain_event(data: dict) -> bool:
    """A message with no inference status is not an inference record."""
    return not data.get("status") and bool(data.get("event"))


def _event_status(data: dict) -> str:
    if _is_domain_event(data):
        return f"EVENT:{data['event']}"
    return "unknown"


def _event_task_id(data: dict) -> str:
    if _is_domain_event(data):
        # Whatever the event is about, so the row can be joined back.
        for key in ("rule_id", "event_id", "correlation_id", "trace_id"):
            if data.get(key):
                return str(data[key])
    return "unknown"


def _telemetry_payload(data: dict):
    """The output payload, or the whole event when there is nothing else.

    Returned as the object, not as JSON text. The pool's jsonb codec encodes
    once already, so `json.dumps` here stored a jsonb *string* -- measured on
    the live table, 4,483 rows carried a payload and `output_payload->>'key'`
    returned NULL on every one of them, with 0 rows of type `object`. Every
    model output this platform has ever stored was unqueryable.

    This is the same defect, and the same remedy, as the two jsonb columns on
    `agent_predictions` twenty lines above -- fixed there, never applied here,
    and those columns now store proper arrays.
    """
    if "output_payload" in data:
        return data.get("output_payload")
    if _is_domain_event(data):
        return data
    return None


def _prediction_confidence(data: dict) -> float:
    """A 0-1 figure from whatever the producer quantified.

    cascade_failure_probability arrives as 0-100; the explicit confidence keys,
    when a producer sends them, are already 0-1.
    """
    cascade = data.get("cascade_failure_probability")
    if cascade is not None:
        try:
            return max(0.0, min(1.0, float(cascade) / 100.0))
        except (TypeError, ValueError) as _exc:
            swallowed("telemetry_worker._prediction_confidence", _exc)
    try:
        return float(data.get("simulation_confidence") or data.get("confidence") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _recommendations(data: dict) -> list:
    """The recommendation list, or the single remediation string as one.

    preemptive_recommendations is never sent; remediation_recommendation is.
    """
    listed = data.get("preemptive_recommendations")
    if isinstance(listed, list) and listed:
        return listed
    single = data.get("remediation_recommendation")
    return [single] if single else []


# How often the entity-prediction sweep runs.
#
# The horizon is measured in hours, so a sweep every ten minutes grades each
# row within a few per cent of its deadline and costs two indexed queries per
# due prediction.
ENTITY_RESOLVE_INTERVAL_SEC = int(os.getenv("ENTITY_RESOLVE_INTERVAL_SEC", "600"))


async def _entity_prediction_loop(db):
    """Sweeps for next-target predictions whose horizon has elapsed."""
    while True:
        try:
            await asyncio.sleep(ENTITY_RESOLVE_INTERVAL_SEC)
            await resolve_entity_predictions(db)
        except asyncio.CancelledError:
            break
        except Exception as e:
            # Counted rather than whispered: a resolver that stops grading
            # leaves a scorecard that looks stable because nothing moves it.
            swallowed("telemetry_worker.entity_prediction_loop", e, logger)


async def main():
    logger.info("Starting Telemetry Worker")
    db = await get_timescale()
    await init_db(db)
    
    redis_client = None
    try:
        redis_client = await get_redis()
    except Exception as re:
        logger.warning(f"Redis unavailable for drift scheduler: {re}")

    # Published to Redis so the gateway's /metrics can see them.
    #
    # bind_redis() is what moves a process-local counter into the cross-process
    # aggregate the /metrics endpoint sums. Only the collectors, the agents and
    # the reasoning engine were calling it, so this service's counters --
    # model_drift_detected_total, agent_predictions_consumed_total and the
    # drift_scheduler_active gauge -- incremented into a dict nothing read and
    # a restart discarded. A drift detection nobody can see is the same as no
    # drift detection, and this is the service whose entire job is noticing that
    # the models have stopped describing the world.
    if redis_client:
        try:
            from shared.utils.metrics import bind_redis
            await bind_redis(redis_client, service_name=os.getenv("SENTINEL_SERVICE", "telemetry-worker"))
        except Exception as e:
            logger.debug("Metrics binding skipped: %s", e)

    producer = SentinelProducer(service_name="telemetry-worker")
    await producer.start()

    # ── WIRE MODEL DRIFT SCHEDULER (§6.1, §6.2) ──────────────────────────────
    drift_scheduler = ModelDriftScheduler(
        redis_client=redis_client,
        producer=producer,
        check_interval_sec=3600,
    )
    drift_task = safe_create_task(drift_scheduler.start())

    # Regression Guard: Confirm background task is running (§6.2)
    await asyncio.sleep(0.05)
    if drift_task.done() and drift_task.exception():
        logger.error(f"❌ ModelDriftScheduler failed to launch: {drift_task.exception()}")
        raise RuntimeError(f"ModelDriftScheduler startup failure: {drift_task.exception()}")
    
    logger.info("🛡️ ModelDriftScheduler startup regression guard passed: background task is active.")
    MetricsCollector.set_gauge("drift_scheduler_active", 1.0)
    
    consumer = SentinelConsumer(
        topics=[Topics.TELEMETRY, Topics.AGENTS_PREDICTIONS],
        group_id="telemetry-worker-group",
        auto_offset_reset="latest",
    )
    await consumer.start()

    # §1.1 Universal heartbeat
    hb_task = safe_create_task(start_heartbeat_task(redis_client, "telemetry-worker"))

    # Grading the predictions this worker persists.
    #
    # It writes them, so it grades them: the agents' own resolution loop scores
    # a directional price call and a next-target prediction carries no ticker,
    # entry price or horizon, so all fifty of the wargamer's rows sat unresolved
    # with no deadline at which they could be judged.
    resolver_task = safe_create_task(_entity_prediction_loop(db))

    try:
        await process_telemetry(consumer, db)
    except asyncio.CancelledError:
        pass
    finally:
        hb_task.cancel()
        resolver_task.cancel()
        drift_scheduler.stop()
        drift_task.cancel()
        await consumer.close()
        await producer.flush()
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
