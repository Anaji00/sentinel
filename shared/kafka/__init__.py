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
from kafka.errors import KafkaError, KafkaConnectionError
import sys

import time
from shared.utils.logging import suppress_noisy_loggers

logger = logging.getLogger(__name__)

# Suppress noisy Kafka library loggers across all microservices
suppress_noisy_loggers(logging.WARNING)


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

        self.logger.info(
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
    RAW_AVIATION = "events.raw.aviation"
    RAW_CYBER = "events.raw.cyber"
    RAW_RADAR            = "events.raw.radar"
    SCENARIOS_GENERATED = "scenarios.generated"
    INTEL_BRIEFS         = "agents.intel.briefs"         # NewsIntelAgent output
    QUANT_DISCOVERIES    = "agents.quant.discoveries"    # QuantResearcherAgent output
    ONTOLOGY_UPDATES     = "agents.ontology.updates"     # OntologyMasterAgent output
    UNKNOWN_ENTITIES     = "agents.ontology.unknown_entities"  # Classification requests
    ONTOLOGY_PROPOSALS   = "sentinel.ontology.proposals"
    RULES_FEEDBACK       = "agents.rules.feedback"
    SYSTEM_HEARTBEAT     = "sentinel.system.heartbeat"
    RADAR_DECISIONS      = "agents.radar.decisions"
    TELEMETRY            = "agents.telemetry"
    FINANCIAL_ADVICE     = "agents.financial.advice"
    RATES_REGIME         = "agents.macro.rates_regime"
    VOL_SURFACE          = "agents.options.vol_surface"
    INSIDER_CLUSTERS     = "agents.insider.clusters"
    MACRO_ASSESSMENT     = "agents.macro.assessment"
    MACRO_DECOUPLING     = "agents.macro.decoupling"
    # ENRICHED: The "Clean Water".
    # We take the raw stuff, fix the dates, add coordinates, and standardize the format
    # into 'NormalizedEvent' so the database can understand it easily.
    ENRICHED_EVENTS = "enriched.events"

    # CORRELATIONS: The "Findings".
    # When our engine notices a pattern (e.g., 2 vessels meeting at night), it
    # bundles those events together into a Correlation and puts it here.
    CORRELATIONS = "sentinel.correlations"
    NORMALIZED     = "events.normalized"
    # ALERTS: The "Megaphone".
    # High-priority stuff that needs to go to a Dashboard, SMS, or Email immediately.
    ALERTS = "alerts.outbound"

    # DLQ (Dead Letter Queue): The "Trash Can" (with recycling).
    # If a message is so broken it crashes our code, we dump it here so we don't
    # get stuck in a loop trying to process it forever.
    DLQ = "dead.letter"

    ALL_RAW = [RAW_MARITIME, RAW_TRADFI, RAW_CRYPTO, RAW_PREDICTION, RAW_NEWS, RAW_AVIATION, RAW_CYBER, RAW_RADAR]


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
        self.batch_logger = BatchKafkaLogger(service_name, flush_interval_sec=10.0)
        logger.info(f"Kafka Producer -> {self._servers}")

    async def start(self, max_retries: int = 15):
        """Must be called inside the async event loop to initialize network sockets."""
        if self._started:
            return
        for attempt in range(max_retries):
            try:
                await self._p.start()
                self._started = True
                logger.info("✅ Kafka Producer successfully connected and bootstrapped.")
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
        except KafkaError as e:
            self.batch_logger.log_error(topic, type(e).__name__)
            logger.error(f"Failed to send message to Kafka: {e}")
            raise

    async def close(self):
        self.batch_logger.flush()
        try:
            await self._p.stop()
        except Exception:
            pass
        self._started = False

# ── CONSUMER ────────────────────────────────────────────────────────────────

class SentinelConsumer:
    def __init__(
            self, 
            topics: list,
            group_id: str,
            bootstrap_servers: str = None,
            auto_offset_reset: str = "latest",
    ):
        self.topics = topics
        self._servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._c = _Consumer(
            *topics,
            bootstrap_servers=self._servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            max_poll_records=100,
            max_poll_interval_ms=600000,
        )
        self._started = False
        self.batch_logger = BatchKafkaLogger(f"consumer.{group_id}", flush_interval_sec=10.0)
        logger.info(f"Kafka Consumer: {self._servers} | Group: {group_id} --> Topics: {topics}")
        
    async def start(self, max_retries: int = 15):
        """Starts the consumer with exponential backoff for broker readiness."""
        if self._started:
            return
            
        for attempt in range(max_retries):
            try:
                await self._c.start()
                self._started = True
                logger.info(f"✅ Kafka Consumer successfully connected and subscribed to {self.topics}.")
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

    async def commit(self):
        """
        Explicitly advance the partition offset. 
        Must be called ONLY after the processing pipeline safely completes all writes.
        """
        if not self._started:
            raise RuntimeError("Cannot commit: SentinelConsumer is not started.")
        await self._c.commit()
        
    async def close(self):
        self.batch_logger.flush()
        try:
            await self._c.stop()
        except Exception:
            pass
        self._started = False

    

    