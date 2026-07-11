import asyncio
import json
import logging
import os
import time
import uuid

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type

import aiohttp
from pydantic import BaseModel

from shared.utils.ollama import (
    OllamaClient, SchemaViolationError,
    OLLAMA_MODEL, OLLAMA_URL
)

logger = logging.getLogger(__name__)

TASK_QUEUE_HIGH   = "sentinel:agents:tasks:high"
TASK_QUEUE_NORMAL = "sentinel:agents:tasks:normal"
TASK_QUEUE_LOW    = "sentinel:agents:tasks:low"
HEARTBEAT_INTERVAL = 30

class SentinelAgent(ABC):
    _global_received_count = 0
    def __init__(self, agent_name: str, input_topics: List[str], redis_client, db_client, neo4j_client, producer, consumer, dlq, model="llama3"):
        self.name = agent_name
        self.input_topics = input_topics
        self.redis = redis_client 
        self.db = db_client
        self.neo4j = neo4j_client
        self._producer = producer
        self._consumer = consumer
        self._dlq = dlq
        self.model = model
        self.logger = logging.getLogger(f"agent.{agent_name}")
        self._processed = 0
        self._errors = 0
        self._started_at = datetime.now(timezone.utc)
        
        # ── BEST PRACTICE: Declare Class Shape in __init__ ──
        # We declare them here so IDEs and Type Checkers know they exist,
        # but we wait to instantiate them until we are inside the async event loop.
        self._session: Optional[aiohttp.ClientSession] = None
        self._llm: Optional[OllamaClient] = None
        
        # Concurrency bound: Limit inflight tasks to prevent memory explosion and LLM timeouts
        self._dispatch_semaphore = asyncio.Semaphore(3)
    @abstractmethod
    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pass

    @property
    @abstractmethod
    def output_topic(self) -> str:
        pass

    async def run(self):
        connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(connector=connector)
        self._llm = OllamaClient(self._session, self.model)

        self.logger.info("=" * 60)
        self.logger.info(f"SENTINEL Agent: {self.name} | Model: {self.model} @ {OLLAMA_URL}")
        self.logger.info("=" * 60)

        await self._consumer.start()
        await self._producer.start()
        await self._dlq.start()
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            self.logger.info(f"{self.name} cancelled — shutting down")
        finally:
            heartbeat_task.cancel()
            if self._session:
                await self._session.close()
            await self._consumer.close()
            await self._producer.close()
            await self._dlq.close()


    async def _consume_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            try:
                batches = await self._consumer.get_batch(timeout_ms=1000)
                if not batches:
                    continue
                for tp, msg_list in batches.items():
                    batch_size = len(msg_list)
                    SentinelAgent._global_received_count += batch_size
                    if SentinelAgent._global_received_count >= 500:
                        logging.getLogger("agents.swarm").info(f"Swarm processed 500 messages across all agent services.")
                        SentinelAgent._global_received_count = 0
                    tasks = []
                    for msg in msg_list:
                        try:
                            payload = json.loads(msg.value.decode('utf-8'))
                            tasks.append(asyncio.create_task(self._dispatch(payload)))
                        except json.JSONDecodeError as e:
                            self.logger.error(f"POISON PILL dropped: {e}")
                            await self._send_dlq({"raw": str(msg.value)}, "JSONDecodeError", self.input_topics[0])

                    await asyncio.gather(*tasks, return_exceptions=True)
                await self._consumer.commit()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.error(f"Consume loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _dispatch(self, raw: Dict[str, Any]):
        async with self._dispatch_semaphore:
            t0 = time.monotonic()
            try:
                result = await self.handle(raw)
                if result is not None:
                    await self._producer.send(
                        self.output_topic,
                        result,
                        key=result.get("agent_run_id", str(uuid.uuid4())),
                    )
                self._processed += 1

                elapsed = time.monotonic() - t0
                if elapsed > 10:
                    self.logger.warning(f"Slow dispatch: {elapsed:.1f}s")
            except SchemaViolationError as e:
                self._errors += 1
                await self._send_dlq(raw, str(e), self.input_topics[0])
            except Exception as e:
                self._errors += 1
                self.logger.error(f"Dispatch error: {e}", exc_info=True)
                await self._send_dlq(raw, str(e), self.input_topics[0])

    async def _send_dlq(self, raw: Dict, error: str, topic: str):
        try:
            await self._dlq.send("dead.letter", {"error": error, "topic": topic, "raw": raw, "agent": self.name})
        except Exception as e:
            self.logger.error(f"DLQ send failed: {e}")

    async def _heartbeat_loop(self):
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            elapsed = (datetime.now(timezone.utc) - self._started_at).total_seconds()
            rate = self._processed / elapsed if elapsed > 0 else 0
            self.logger.info(f"♥ {self.name} | processed={self._processed} errors={self._errors} rate={rate:.2f}/s")
            try:
                await self.redis.raw.set(
                    f"sentinel:agents:health:{self.name}",
                    json.dumps({
                        "processed": self._processed,
                        "errors":    self._errors,
                        "uptime_s":  int(elapsed),
                        "ts":        datetime.now(timezone.utc).isoformat(),
                    }),
                    ex=120, 
                )
            except Exception:
                pass

    def state_key(self, *parts: str) -> str:
        return f"sentinel:agents:{self.name}:{':'.join(parts)}"

    async def is_recently_processed(self, entity_id: str, window_seconds: int = 3600) -> bool:
        return await self.redis.raw.exists(self.state_key("seen", entity_id))

    async def mark_processed(self, entity_id: str, window_seconds: int = 3600):
        await self.redis.raw.set(self.state_key("seen", entity_id), "1", ex=window_seconds)

    async def enqueue_task(self, task_type: str, payload: Dict, priority: str = "normal"):
        queue = {"high": TASK_QUEUE_HIGH, "normal": TASK_QUEUE_NORMAL, "low": TASK_QUEUE_LOW}.get(priority, TASK_QUEUE_NORMAL)
        task = {
            "task_id": str(uuid.uuid4()), "task_type": task_type, "agent": self.name,
            "payload": payload, "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.redis.raw.rpush(queue, json.dumps(task))

    async def _execute_with_telemetry(self, message: dict, system_prompt: str, user_prompt: str, schema: Optional[Type[BaseModel]] = None, temperature: float = 0.1) -> Any:
        
        start_time = time.monotonic()
        # Fallback to a UUID if no event_id is present (e.g., scheduled tasks)
        run_id = message.get("event_id", str(uuid.uuid4())[:8])
        
        await self._producer.send(
            "agents.telemetry", 
            {
                "agent": self.name, 
                "status": "THINKING", 
                "task_id": run_id,
                "system_prompt_length": len(system_prompt),
                "user_prompt_length": len(user_prompt)
            }
        )
        
        # 2. Execute LLM with Pydantic Enforcement
        response = await self._llm.infer(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema=schema,
            temperature=temperature
        )
        
        if hasattr(response, "model_dump"):
            output_payload = response.model_dump()
        elif hasattr(response, "dict"):
            output_payload = response.dict()
        else:
            output_payload = {"raw_text": str(response)}
        # 3. Emit Completion
        await self._producer.send(
            "agents.telemetry", 
            {
                "agent": self.name, 
                "status": "COMPLETE",
                "task_id": run_id,
                "latency_ms": round((time.monotonic() - start_time) * 1000, 2),
                "output_payload": output_payload
            }
        )
        return response