"""
shared/kafka/__init__.py

Producer, Consumer, and topic registry.
Every service imports from here — never from kafka-python directly.

NOTE ON CONSUMER TIMEOUTS:
  We do NOT set consumer_timeout_ms.
  Without consumer_timeout_ms the consumer blocks indefinitely, which is
  correct behaviour for a long-running service. The consume loops in
  enrichment and correlation wrap the iterator in `while True` anyway.
"""

import json
import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaConsumer as _Consumer
from aiokafka import AIOKafkaProducer as _Producer
from aiokafka.errors import KafkaError, KafkaConnectionError
import sys

import time
from shared.utils.logging import suppress_noisy_loggers
from shared.utils.metrics import MetricsCollector
import time as _time
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# Suppress noisy Kafka library loggers across all microservices to WARNING level
suppress_noisy_loggers(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("aiokafka").setLevel(logging.WARNING)
logging.getLogger("shared.kafka").setLevel(logging.WARNING)


# ── BATCH KAFKA LOGGER ────────────────────────────────────────────────────────

class BatchKafkaLogger:
    """
    Aggregates throughput and error statistics for Kafka production & consumption loops.
    Periodically flushes summary statistics instead of logging per-event or per-poll.
    """

    def __init__(self, service_name: str, flush_interval_sec: float = 10.0):
        self.service_name = service_name
        self.flush_interval_sec = flush_interval_sec
        self.logger = logging.getLogger(f"kafka.batch.{service_name}")
        self._last_flush = time.monotonic()
        self._topic_counts: Dict[str, int] = {}
        self._error_counts: Dict[str, int] = {}

    def log_produced(self, topic: str, count: int = 1):
        self._topic_counts[topic] = self._topic_counts.get(topic, 0) + count
        self._maybe_flush()

    def log_consumed(self, topic: str, count: int = 1):
        self._topic_counts[topic] = self._topic_counts.get(topic, 0) + count
        self._maybe_flush()

    def log_error(self, topic: str, error_type: str = "general"):
        key = f"{topic}:{error_type}"
        self._error_counts[key] = self._error_counts.get(key, 0) + 1
        self._maybe_flush()

    def _maybe_flush(self):
        now = time.monotonic()
        if now - self._last_flush >= self.flush_interval_sec:
            self.flush()

    def flush(self):
        now = time.monotonic()
        elapsed = max(now - self._last_flush, 0.001)
        self._last_flush = now

        if not self._topic_counts and not self._error_counts:
            return

        total_msgs = sum(self._topic_counts.values())
        rate = total_msgs / elapsed
        topic_summary = (
            ", ".join(f"{t}: {c}" for t, c in self._topic_counts.items())
            if self._topic_counts
            else "none"
        )
        err_summary = (
            ", ".join(f"{e}: {c}" for e, c in self._error_counts.items())
            if self._error_counts
            else "none"
        )

        self.logger.warning(
            f"📡 KAFKA BATCH [{self.service_name}] Processed {total_msgs} msgs in {elapsed:.1f}s ({rate:.1f}/s) | Topics: [{topic_summary}] | Errors: [{err_summary}]"
        )
        self._topic_counts.clear()
        self._error_counts.clear()


# ── TOPIC REGISTRY ────────────────────────────────────────────────────────────

class Topics:
    # RAW TOPICS: The "Firehose".
    # These contain messy, raw data straight from the collectors (APIs/scrapers).
    # It might be missing fields or have weird formatting.
    RAW_MARITIME = "events.raw.maritime"
    RAW_TRADFI     = "events.raw.tradfi"
    RAW_CRYPTO     = "events.raw.crypto"
    RAW_PREDICTION = "events.raw.prediction"
    RAW_NEWS = "events.raw.news"
    RAW_SOCIAL = "events.raw.social"
    RAW_AVIATION = "events.raw.aviation"
    RAW_CYBER = "events.raw.cyber"
    RAW_RADAR            = "events.raw.radar"
    RAW_FILINGS = "events.raw.filings"
    SCENARIOS_GENERATED = "scenarios.generated"
    INTEL_BRIEFS         = "agents.intel.briefs"         # NewsIntelAgent output
    QUANT_DISCOVERIES    = "agents.quant.discoveries"    # QuantResearcherAgent output
    ONTOLOGY_UPDATES     = "agents.ontology.updates"     # OntologyMasterAgent output
    UNKNOWN_ENTITIES     = "agents.ontology.unknown_entities"  # Classification requests
    ONTOLOGY_PROPOSALS   = "sentinel.ontology.proposals"
    RULES_FEEDBACK       = "agents.rules.feedback"
    RULES_SYNTHESIZED    = "agents.rules.synthesized"
    # Co-occurring event types that no rule connects.
    #
    # The rule synthesizer's real input. It subscribed to nine topics and
    # received, in practice, only CORRELATIONS -- which carry a rule_id and are
    # therefore rule *firings*, so synthesising from them re-derives the rule
    # that produced them. This carries the opposite signal: patterns the rule
    # set does not cover.
    RULE_CANDIDATES      = "agents.rules.candidates"
    CONSENSUS_REPORTS    = "agents.consensus.reports"
    # Declared, never produced to. Heartbeats travel through Redis
    # (sentinel:heartbeat:{component}), which is where every reader already
    # looks; this topic had two subscribers and no producer anywhere in the
    # tree, so each was a consumer-group assignment and a rebalance participant
    # for a stream that has never carried a message. Kept as a constant only so
    # an external reference does not break on import.
    SYSTEM_HEARTBEAT     = "sentinel.system.heartbeat"
    RADAR_DECISIONS      = "agents.radar.decisions"
    TELEMETRY            = "agents.telemetry"
    FINANCIAL_ADVICE     = "agents.financial.advice"
    RATES_REGIME         = "agents.macro.rates_regime"
    VOL_SURFACE          = "agents.options.vol_surface"
    INSIDER_CLUSTERS     = "agents.insider.clusters"
    MACRO_ASSESSMENT     = "agents.macro.assessment"
    MACRO_DECOUPLING     = "agents.macro.decoupling"
    AGENTS_PREDICTIONS   = "agents.predictions.output"
    # ENRICHED: The "Clean Water".
    # We take the raw stuff, fix the dates, add coordinates, and standardize the format
    # into 'NormalizedEvent' so the database can understand it easily.
    ENRICHED_EVENTS = "enriched.events"

    # CORRELATIONS: The "Findings".
    # When our engine notices a pattern (e.g., 2 vessels meeting at night), it
    # bundles those events together into a Correlation and puts it here.
    CORRELATIONS = "sentinel.correlations"
    # ALERTS: The "Megaphone".
    # High-priority stuff that needs to go to a Dashboard, SMS, or Email immediately.
    ALERTS = "alerts.outbound"

    # DLQ (Dead Letter Queue): The "Trash Can" (with recycling).
    # If a message is so broken it crashes our code, we dump it here so we don't
    # get stuck in a loop trying to process it forever.
    DLQ = "dead.letter"

    ALL_RAW = [RAW_MARITIME, RAW_TRADFI, RAW_CRYPTO, RAW_PREDICTION, RAW_NEWS, RAW_SOCIAL, RAW_FILINGS, RAW_AVIATION, RAW_CYBER, RAW_RADAR]


# ── SERIALIZATION ─────────────────────────────────────────────────────────────

def _serialize(obj: Any) -> bytes:
    # Kafka only understands Bytes (0s and 1s).
    # We translate Python Objects (Dictionaries, Pydantic Models, Dates) into JSON text bytes.
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        if hasattr(o, "model_dump"):
            return o.model_dump(mode="json")
        if hasattr(o, "dict") and callable(getattr(o, "dict")):
            return o.dict()
        if isinstance(o, set):
            return list(o)
        if hasattr(o, "__str__"):
            return str(o)
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
        return json.dumps(obj.model_dump(mode="json"), default=default).encode("utf-8")
    if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
        return json.dumps(obj.dict(), default=default).encode("utf-8")
    return json.dumps(obj, default=default).encode("utf-8")


# ── PRODUCER ────────────────────────────────────────────────────────────────

class SentinelProducer:
    def __init__(self, bootstrap_servers: str = None, service_name: str = "producer"):
        self._servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._p = _Producer(
            bootstrap_servers=self._servers,
            value_serializer=_serialize,
            acks="all",
            linger_ms=10,
            compression_type="gzip",
        )
        self._started = False
        # Retained for metric labelling, not only for the batch logger: every
        # counter emitted from send() is attributed to this service.
        self.service_name = service_name
        self.batch_logger = BatchKafkaLogger(service_name, flush_interval_sec=10.0)
        logger.warning(f"Kafka Producer -> {self._servers}")

    async def start(self, max_retries: int = 15):
        """Must be called inside the async event loop to initialize network sockets."""
        if self._started:
            return
        for attempt in range(max_retries):
            try:
                await self._p.start()
                self._started = True
                logger.warning("✅ Kafka Producer successfully connected and bootstrapped.")
                return
            except Exception as e:
                wait_time = min(2 ** attempt, 30)
                logger.warning(f"⏳ Kafka broker not ready. Producer retrying in {wait_time}s... ({e})")
                await asyncio.sleep(wait_time)
                
        raise ConnectionError(f"Fatal: Could not connect to Kafka Producer at {self._servers} after {max_retries} attempts.")

    async def send(self, topic: str, data: Dict[str, Any], key: str = None, headers: list = None):
        """
        Emits events to Kafka.
        Headers support OpenTelemetry span injection across distributed boundaries.
        Key enforces partition-hashing for strict chronological ordering per entity.
        """
        if not self._started:
            raise RuntimeError("Cannot send: SentinelProducer is not started.")
        try:
            k_bytes = str(key).encode("utf-8") if key is not None else None
            await self._p.send_and_wait(
                topic,
                value=data,
                key=k_bytes,
                headers=headers
            )
            self.batch_logger.log_produced(topic, 1)
            # Throughput is counted here rather than at each call site: this is
            # the single chokepoint every producer passes through, so one
            # instrumentation point covers all eleven collectors and every
            # downstream stage. A collector that stops producing shows up as a
            # flat counter, which a heartbeat alone can never reveal.
            MetricsCollector.increment(f"produced_total:{self.service_name}", 1)
            MetricsCollector.increment(f"produced_by_topic:{self.service_name}:{topic}", 1)
            MetricsCollector.set_gauge(f"last_produced_epoch:{self.service_name}", _time.time())
        except KafkaError as e:
            self.batch_logger.log_error(topic, type(e).__name__)
            MetricsCollector.increment(f"produce_errors_total:{self.service_name}", 1)
            logger.error(f"Failed to send message to Kafka: {e}")
            raise

    async def close(self):
        self.batch_logger.flush()
        try:
            await self._p.stop()
        except Exception as _exc:
            swallowed("kafka.close", _exc, logger)
        self._started = False

# ── CONSUMER ────────────────────────────────────────────────────────────────

class SentinelConsumer:
    def __init__(
            self, 
            topics: list,
            group_id: str,
            bootstrap_servers: str = None,
            auto_offset_reset: str = "latest",
            max_poll_records: Optional[int] = None,
            max_poll_interval_ms: Optional[int] = None,
    ):
        self.topics = topics
        self._servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._c = _Consumer(
            *topics,
            bootstrap_servers=self._servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            max_poll_records=max_poll_records if max_poll_records is not None else 15,
            max_poll_interval_ms=max_poll_interval_ms if max_poll_interval_ms is not None else 1800000,
        )
        self._started = False
        self.batch_logger = BatchKafkaLogger(f"consumer.{group_id}", flush_interval_sec=10.0)
        logger.warning(f"Kafka Consumer: {self._servers} | Group: {group_id} --> Topics: {topics}")
        
    async def start(self, max_retries: int = 15):
        """Starts the consumer with exponential backoff for broker readiness."""
        if self._started:
            return
            
        for attempt in range(max_retries):
            try:
                await self._c.start()
                self._started = True
                logger.warning(f"✅ Kafka Consumer successfully connected and subscribed to {self.topics}.")
                return
            except Exception as e:
                wait_time = min(2 ** attempt, 30)
                logger.warning(f"⏳ Kafka broker not ready. Consumer retrying in {wait_time}s... ({e})")
                await asyncio.sleep(wait_time)
                
        raise ConnectionError(f"Fatal: Could not connect to Kafka Consumer at {self._servers} after {max_retries} attempts.")

    async def get_batch(self, timeout_ms=1000):
        """
        Native async polling. Replaces the old loop.run_in_executor hack.
        Returns a dictionary of {TopicPartition: [ConsumerRecord]}
        """
        if not self._started:
            raise RuntimeError("CANNOT FETCH MESSAGES -- Consumer not started.")
        batches = await self._c.getmany(timeout_ms=timeout_ms)
        if batches:
            for tp, records in batches.items():
                self.batch_logger.log_consumed(tp.topic, len(records))
        return batches

    async def commit(self, offsets: Optional[Dict[Any, int]] = None):
        """Advance offsets. Call only after the pipeline has completed its writes.

        `offsets` maps TopicPartition to the next offset to consume. Passing it
        commits those partitions alone; omitting it commits everything assigned,
        which is only correct when every assigned partition succeeded.

        The parameter exists because this wrapper did not accept one. A caller
        that had computed per-partition offsets -- so that succeeding on one
        partition would not commit another whose batch had failed -- called
        `commit({tp: offset + 1})` and got
        `TypeError: commit() takes 1 positional argument but 2 were given`. The
        caller's except clause logged it as "rebalance or timeout", so the
        enrichment service processed 9,085 events at 4.2/s while committing
        none of them: its consumer group held no offsets, did not appear in
        `list_consumer_groups`, and would have replayed the entire topic on
        restart. Throughput looked healthy the whole time.
        """
        if not self._started:
            raise RuntimeError("Cannot commit: SentinelConsumer is not started.")
        if offsets:
            await self._c.commit(offsets)
        else:
            await self._c.commit()
        
    async def lag_report(self) -> Dict[str, Any]:
        """How far behind this consumer is, and how much of its safety margin is left.

        Two different questions, and only the first was ever asked -- badly:
        `consumer_lag` reached the heartbeat through
        `getattr(self, "_consumer_lag", None)` against an attribute nothing in
        the tree ever assigned, so every heartbeat this platform has published
        carried `consumer_lag: null`, and `scripts/healthcheck.py` read that
        null as "no lag information" forever.

        `lag` is end_offset - position: messages produced and not yet read.

        `retention_headroom` is position - beginning_offset: messages still
        available *behind* this consumer. It is the one that matters and the one
        nothing measured. Kafka deletes by retention, not by consumption, so a
        consumer that falls far enough behind has its unread messages deleted
        under it -- and the visible symptom is lag *falling*, which reads as
        recovery. Headroom going to zero is the only signal that distinguishes
        "caught up" from "the backlog was deleted".

        Returns per-partition detail plus totals. Never raises: this is called
        from a heartbeat loop, and a metrics failure must not stop a heartbeat.
        """
        empty: Dict[str, Any] = {
            "lag": None, "retention_headroom": None, "partitions": [],
            "at_retention_edge": False,
        }
        if not self._started:
            return empty
        try:
            assignment = self._c.assignment()
            if not assignment:
                # Subscribed but not yet assigned: no partitions, no answer.
                return empty

            partitions = list(assignment)
            end_offsets = await self._c.end_offsets(partitions)
            begin_offsets = await self._c.beginning_offsets(partitions)

            detail = []
            total_lag = 0
            total_headroom = 0
            at_edge = False
            for tp in partitions:
                position = await self._c.position(tp)
                end = end_offsets.get(tp)
                begin = begin_offsets.get(tp)
                if position is None or end is None or begin is None:
                    continue
                lag = max(0, int(end) - int(position))
                headroom = max(0, int(position) - int(begin))
                total_lag += lag
                total_headroom += headroom
                # The consumer's next read is the oldest message the broker
                # still holds: anything older has already been deleted unread.
                edge = int(position) <= int(begin) and lag > 0
                at_edge = at_edge or edge
                detail.append({
                    "topic": tp.topic, "partition": tp.partition,
                    "position": int(position), "begin": int(begin), "end": int(end),
                    "lag": lag, "retention_headroom": headroom,
                    "at_retention_edge": edge,
                })

            if not detail:
                return empty
            return {
                "lag": total_lag,
                "retention_headroom": total_headroom,
                "partitions": detail,
                "at_retention_edge": at_edge,
            }
        except Exception as e:
            swallowed("kafka.lag_report", e, logger)
            return empty

    async def close(self):
        self.batch_logger.flush()
        try:
            await self._c.stop()
        except Exception as _exc:
            swallowed("kafka.close", _exc, logger)
        self._started = False

    

    