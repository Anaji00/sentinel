"""
services/agents/base.py

SENTINEL AGENT BASE CLASS
==========================
Foundation for all autonomous Llama3-powered research agents.

OllamaClient and SchemaViolationError are now imported from
shared/utils/ollama.py so the reasoning service can use the same
client without duplicating the implementation.
"""

# BEST PRACTICE: Standard Library Imports First
# Always import built-in Python modules first, then third-party libraries (like aiohttp), 
# and finally local project modules. This keeps dependencies organized.
import asyncio
import json
import logging
import os
import time
import uuid

# CONCEPT: Abstract Base Classes (ABC)
# The 'abc' module allows us to create "blueprint" classes. We can define methods 
# that *must* be implemented by any child class that inherits from this base class.
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type

import aiohttp
from pydantic import BaseModel

# Import from shared — single implementation used by both agents and reasoning
from shared.utils.ollama import (
    OllamaClient,
    InferenceError,
    SchemaViolationError,
    OLLAMA_MODEL,
    OLLAMA_URL,
    _OLLAMA_SEMAPHORE,
)

logger = logging.getLogger(__name__)

# ── TASK QUEUE KEYS ───────────────────────────────────────────────────────────
# These are the Redis keys used to manage tasks of varying urgencies.
# Redis Lists (accessed via rpush/blpop) act as lightweight task queues.
TASK_QUEUE_HIGH   = "sentinel:agents:tasks:high"
TASK_QUEUE_NORMAL = "sentinel:agents:tasks:normal"
TASK_QUEUE_LOW    = "sentinel:agents:tasks:low"

# How often (in seconds) the agent reports its health and throughput to Redis.
HEARTBEAT_INTERVAL = 30


# By inheriting from ABC, we make SentinelAgent an Abstract Base Class.
# You cannot instantiate `SentinelAgent()` directly; you can only instantiate its subclasses.
class SentinelAgent(ABC):
    """
    Abstract base for all SENTINEL autonomous agents.

    Subclasses implement:
      handle(message: dict) -> Optional[dict]
      output_topic() -> str

    The base class handles:
      - Kafka consumption loop
      - OllamaClient management (from shared.utils.ollama)
      - Result publication
      - DLQ routing on unhandled exceptions
      - Heartbeat logging
      - Redis state key namespacing
      - Task queue helpers
    """

    def __init__(
        self,
        agent_name:    str,
        input_topics:  List[str],
        redis_client,
        db_client,
        neo4j_client,
        kafka_producer,
        kafka_consumer,
        dlq_producer,
        model: str = OLLAMA_MODEL,
    ):
        # BEST PRACTICE: Dependency Injection
        # Notice how we pass `redis_client`, `db_client`, etc., into the constructor.
        # We don't create new database connections inside the class. 
        # This allows multiple agents to share the same connection pool, saving memory.
        # It also makes the code easy to test (we can pass in "mock" databases during testing).
        self.name           = agent_name
        self.input_topics   = input_topics
        self.redis          = redis_client
        self.db             = db_client
        self.neo4j          = neo4j_client
        self._producer      = kafka_producer
        self._consumer      = kafka_consumer
        self._dlq           = dlq_producer
        self.model          = model
        self.logger         = logging.getLogger(f"agent.{agent_name}")
        self._processed     = 0
        self._errors        = 0
        self._started_at    = datetime.now(timezone.utc)
        # We initialize these as None and create them later when the async loop actually starts.
        self._session: Optional[aiohttp.ClientSession] = None
        self._llm:     Optional[OllamaClient]           = None

    # ── ABSTRACT INTERFACE ────────────────────────────────────────────────────

    @abstractmethod
    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        @abstractmethod forces any subclass (like NewsIntelAgent) to write their own
        version of this function. If they forget, Python will throw an error immediately.
        
        Core agent logic. Receives one deserialized Kafka message.
        Return a dict to publish to output_topic(), or None to suppress output.
        """

    @property
    @abstractmethod
    def output_topic(self) -> str:
        """Kafka topic this agent publishes results to."""

    # ── LIFECYCLE ─────────────────────────────────────────────────────────────

    async def run(self):
        # UNDER THE HOOD: Connection Pooling
        # Creating a TCP connection is slow. `aiohttp.TCPConnector` keeps connections 
        # alive and reuses them. We pass this session into our OllamaClient so every 
        # AI request is blazingly fast and doesn't require a new TLS handshake.
        connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(connector=connector)
        self._llm = OllamaClient(self._session, self.model)

        self.logger.info("=" * 60)
        self.logger.info(f"SENTINEL Agent: {self.name}")
        self.logger.info(f"Model: {self.model} @ {OLLAMA_URL}")
        self.logger.info(f"Topics: {self.input_topics}")
        self.logger.info("=" * 60)

        # Start the background heartbeat loop. `create_task` runs it concurrently.
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        try:
            # Start listening to Kafka forever
            await self._consume_loop()
        except asyncio.CancelledError:
            self.logger.info(f"{self.name} cancelled — shutting down")
        finally:
            heartbeat_task.cancel()
            await self._session.close()
            self._consumer.close()

    async def _consume_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            try:
                # UNDER THE HOOD: Thread Executors for Blocking I/O
                # `self._consumer.raw.poll` is a synchronous, blocking network call.
                # If we ran it normally, it would freeze the entire asyncio program.
                # `run_in_executor` pushes this blocking task into a background thread!
                messages = await loop.run_in_executor(
                    None, self._consumer.raw.poll, 1.0
                )
                if not messages:
                    continue
                for _, msg_list in messages.items():
                    for msg in msg_list:
                        # Fire and Forget: For every message received, we create a new background
                        # task to process it. This allows us to process multiple messages at once.
                        asyncio.create_task(self._dispatch(msg.value))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.error(f"Consume loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _dispatch(self, raw: Dict[str, Any]):
        t0 = time.monotonic()
        try:
            # Call the specific agent's custom handle() method
            result = await self.handle(raw)
            
            if result is not None:
                # If the agent returned data, forward it to the next Kafka topic
                self._producer.send(
                    self.output_topic,
                    result,
                    key=result.get("agent_run_id", str(uuid.uuid4())),
                )
            self._processed += 1
            elapsed = time.monotonic() - t0
            if elapsed > 10:
                self.logger.warning(f"Slow dispatch: {elapsed:.1f}s")

        except SchemaViolationError as e:
            # CONCEPT: Dead Letter Queues (DLQ)
            # If the AI hallucinates bad JSON, we don't just throw the message away!
            # We route it to a "Dead Letter" Kafka topic so engineers can inspect it later.
            self._errors += 1
            self.logger.error(f"Schema violation (no retry): {e}")
            self._send_dlq(raw, str(e), self.input_topics[0])

        except Exception as e:
            self._errors += 1
            self.logger.error(f"Dispatch error: {e}", exc_info=True)
            self._send_dlq(raw, str(e), self.input_topics[0])

    def _send_dlq(self, raw: Dict, error: str, topic: str):
        try:
            self._dlq.send(
                "dead.letter",
                {"error": error, "topic": topic, "raw": raw, "agent": self.name},
            )
        except Exception as e:
            self.logger.error(f"DLQ send failed: {e}")

    async def _heartbeat_loop(self):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            
            # Calculate how many messages we are processing per second
            elapsed = (datetime.now(timezone.utc) - self._started_at).total_seconds()
            rate = self._processed / elapsed if elapsed > 0 else 0
            self.logger.info(
                f"♥ {self.name} | processed={self._processed} "
                f"errors={self._errors} rate={rate:.2f}/s"
            )
            try:
                self.redis.raw.set(
                    f"sentinel:agents:health:{self.name}",
                    json.dumps({
                        "processed": self._processed,
                        "errors":    self._errors,
                        "uptime_s":  int(elapsed),
                        "ts":        datetime.now(timezone.utc).isoformat(),
                    }),
                    ttl=120,
                )
            except Exception:
                pass

    # ── REDIS STATE HELPERS ───────────────────────────────────────────────────

    def state_key(self, *parts: str) -> str:
        # Creates a consistent Redis key format, e.g., "sentinel:agents:news_intel:seen:1234"
        return f"sentinel:agents:{self.name}:{':'.join(parts)}"

    def is_recently_processed(self, entity_id: str, window_seconds: int = 3600) -> bool:
        # CONCEPT: Idempotency (Exactly-Once Processing)
        # In distributed systems, you might accidentally receive the same message twice.
        # Checking Redis before processing ensures we don't do expensive LLM work twice.
        return self.redis.raw.exists(self.state_key("seen", entity_id))

    def mark_processed(self, entity_id: str, window_seconds: int = 3600):
        # `ttl` stands for Time To Live. Redis will automatically delete this key 
        # after `window_seconds`. This prevents our database from growing infinitely!
        self.redis.raw.set(self.state_key("seen", entity_id), "1", ttl=window_seconds)

    # ── TASK QUEUE ────────────────────────────────────────────────────────────

    def enqueue_task(self, task_type: str, payload: Dict, priority: str = "normal"):
        # CONCEPT: Redis Queues
        # Instead of heavy tools like Celery, we use Redis Lists for simple queues.
        # `rpush` pushes a new task onto the Right side of the list. Background workers 
        # will pop them off the Left side (FIFO - First In, First Out).
        queue = {
            "high":   TASK_QUEUE_HIGH,
            "normal": TASK_QUEUE_NORMAL,
            "low":    TASK_QUEUE_LOW,
        }.get(priority, TASK_QUEUE_NORMAL)

        task = {
            "task_id":    str(uuid.uuid4()),
            "task_type":  task_type,
            "agent":      self.name,
            "payload":    payload,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.redis.raw.rpush(queue, json.dumps(task))