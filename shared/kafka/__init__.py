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

logger = logging.getLogger(__name__)

# Configure Kafka libraries to log at WARNING level across all services to prevent flooding
aiokafka_logger = logging.getLogger("aiokafka")
aiokafka_logger.setLevel(logging.WARNING)
logging.getLogger("aiokafka.producer").setLevel(logging.WARNING)
logging.getLogger("aiokafka.consumer").setLevel(logging.WARNING)

kafka_logger = logging.getLogger("kafka")
kafka_logger.setLevel(logging.WARNING)

_handler = logging.StreamHandler(sys.stdout)
_handler.setLevel(logging.WARNING)
_formatter = logging.Formatter(
    "%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
_handler.setFormatter(_formatter)

for _log in (aiokafka_logger, kafka_logger, logging.getLogger("aiokafka.producer"), logging.getLogger("aiokafka.consumer")):
    if not _log.handlers:
        _log.addHandler(_handler)
        _log.propagate = False

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

    ALL_RAW = [RAW_MARITIME, RAW_TRADFI, RAW_CRYPTO, RAW_PREDICTION, RAW_NEWS, RAW_AVIATION, RAW_CYBER, RAW_RADAR, SCENARIOS_GENERATED]


# ── SERIALIZATION ─────────────────────────────────────────────────────────────

def _serialize(obj: Any) -> bytes:
    # Kafka only understands Bytes (0s and 1s).
    # We need to translate our Python Objects (Dictionaries, Dates) into JSON text,
    # and then encode that text into Bytes.
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
    return json.dumps(obj, default=default).encode("utf-8")


# ── PRODUCER ────────────────────────────────────────────────────────────────

class SentinelProducer:
    def __init__(self, bootstrap_servers: str = None):
        self._servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._p = _Producer(
            bootstrap_servers=self._servers,
            value_serializer=_serialize,
            # RETRIES: The "Redial" button.
            # ACKS='all': The "Registered Mail" setting.
            # We don't consider a message "Sent" until the Leader AND all Backups confirm receipt.
            # This is slower but guarantees we never lose data if a server crashes.
            acks="all",
            # LINGER_MS: "Wait for the bus to fill".
            # Instead of sending every message instantly, wait 5ms to see if more messages arrive.
            # Sending 1 bus with 50 people is faster than 50 buses with 1 person.
            linger_ms=5,
            # COMPRESSION: Zip it up.
            # Makes the payload smaller. Saves network bandwidth and disk space.
            compression_type="gzip",
        )
        self._started = False
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
                # KEY: The "Sorting Hat".
                # Kafka guarantees that messages with the SAME key always go to the SAME partition.
                # This ensures Event 1, 2, and 3 for "Vessel_A" are processed in order (1->2->3).
                key=k_bytes,
                headers=headers
            )
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise
    async def close(self):
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
            # GROUP_ID: The "Team Name".
            # If you run 5 instances of this code with the SAME group_id, Kafka divides the work.
            # Instance 1 gets 20% of the messages, Instance 2 gets 20%, etc.
            # If one crashes, the others pick up the slack.
            group_id=group_id,
            # AUTO_OFFSET_RESET: What to do if we have no bookmark?
            # 'latest'   = Start reading only NEW messages arriving now. (Ignore history)
            # 'earliest' = Start from the beginning of time. (Reprocess everything)
            auto_offset_reset=auto_offset_reset,
            # AUTO_COMMIT: The "Automatic Bookmark".
            # Every 1 second, the code tells Kafka "I've finished reading up to Page 50".
            # If the code crashes and restarts, it looks up the bookmark and starts at Page 51.
            # (It's simpler than doing it manually, though slightly less precise).
            enable_auto_commit=False,
            max_poll_records=100,
        )
        self._started = False
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
        return await self._c.getmany(timeout_ms=timeout_ms)

    async def commit(self):
        """
        Explicitly advance the partition offset. 
        Must be called ONLY after the processing pipeline safely completes all writes.
        """
        if not self._started:
            raise RuntimeError("Cannot commit: SentinelConsumer is not started.")
        await self._c.commit()
        
    async def close(self):
        try:
            await self._c.stop()
        except Exception:
            pass
        self._started = False

    

    