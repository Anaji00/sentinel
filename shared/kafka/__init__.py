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
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from kafka import KafkaConsumer as _Consumer
from kafka import KafkaProducer as _Producer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# ── TOPIC REGISTRY ────────────────────────────────────────────────────────────

class Topics:
    # RAW TOPICS: The "Firehose".
    # These contain messy, raw data straight from the collectors (APIs/scrapers).
    # It might be missing fields or have weird formatting.
    RAW_MARITIME = "raw.maritime"
    RAW_FINANCIAL = "raw.financial"
    RAW_NEWS = "raw.news"
    RAW_AVIATION = "raw.aviation"
    RAW_CYBER = "raw.cyber"

    # ENRICHED: The "Clean Water".
    # We take the raw stuff, fix the dates, add coordinates, and standardize the format
    # into 'NormalizedEvent' so the database can understand it easily.
    ENRICHED_EVENTS = "enriched.events"

    # CORRELATIONS: The "Findings".
    # When our engine notices a pattern (e.g., 2 vessels meeting at night), it
    # bundles those events together into a Correlation and puts it here.
    CORRELATIONS = "correlations.detected"

    # ALERTS: The "Megaphone".
    # High-priority stuff that needs to go to a Dashboard, SMS, or Email immediately.
    ALERTS = "alerts.outbound"

    # DLQ (Dead Letter Queue): The "Trash Can" (with recycling).
    # If a message is so broken it crashes our code, we dump it here so we don't
    # get stuck in a loop trying to process it forever.
    DLQ = "dead.letter"

    ALL_RAW = [RAW_MARITIME, RAW_FINANCIAL, RAW_NEWS, RAW_AVIATION, RAW_CYBER]


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

def _deserialize(data: bytes) -> Any:
    return json.loads(data.decode("utf-8"))

# ── PRODUCER ────────────────────────────────────────────────────────────────

class SentinelProducer:
    def __init__(self, bootstrap_servers: str = None):
        servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._p = _Producer(
            bootstrap_servers=servers,
            value_serializer=_serialize,
            # RETRIES: The "Redial" button.
            # If the internet blips or the server is busy, try 5 times before giving up.
            retries=5,
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
        logger.info(f"Kafka Producer -> {servers}")

    
    def send(self, topic: str, data: Dict[str, Any], key: str = None):
        try:
            self._p.send(
                topic,
                value=data,
                # KEY: The "Sorting Hat".
                # Kafka guarantees that messages with the SAME key always go to the SAME partition.
                # This ensures Event 1, 2, and 3 for "Vessel_A" are processed in order (1->2->3).
                key=key.encode("utf-8") if key else None,
            )
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise
    def close(self):
        self._p.flush()
        self._p.close()

# ── CONSUMER ────────────────────────────────────────────────────────────────

class SentinelConsumer:
    def __init__(
            self, 
            topics: list,
            group_id: str,
            bootstrap_servers: str = None,
            auto_offset_reset: str = "latest",
    ):
        servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._c = _Consumer(
            *topics,
            bootstrap_servers=servers,
            # GROUP_ID: The "Team Name".
            # If you run 5 instances of this code with the SAME group_id, Kafka divides the work.
            # Instance 1 gets 20% of the messages, Instance 2 gets 20%, etc.
            # If one crashes, the others pick up the slack.
            group_id=group_id,
            value_deserializer=_deserialize,
            # AUTO_OFFSET_RESET: What to do if we have no bookmark?
            # 'latest'   = Start reading only NEW messages arriving now. (Ignore history)
            # 'earliest' = Start from the beginning of time. (Reprocess everything)
            auto_offset_reset=auto_offset_reset,
            # AUTO_COMMIT: The "Automatic Bookmark".
            # Every 1 second, the code tells Kafka "I've finished reading up to Page 50".
            # If the code crashes and restarts, it looks up the bookmark and starts at Page 51.
            # (It's simpler than doing it manually, though slightly less precise).
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
        logger.info(f"Kafka Consumer: {servers} | Group: {group_id} --> Topics: {topics}")

    def __iter__(self):
        return iter(self._c)

        
    def close(self):
        self._c.close()

    

    