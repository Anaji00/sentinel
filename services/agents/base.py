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
from pydantic import BaseModel, Field

from shared.utils.ollama import (
    OllamaClient, SchemaViolationError, InferenceError,
    OLLAMA_MODEL, OLLAMA_FALLBACK_MODEL, OLLAMA_URL
)

# Task Queue Keys
TASK_QUEUE_HIGH   = "sentinel:tasks:high"
TASK_QUEUE_NORMAL = "sentinel:tasks:normal"
TASK_QUEUE_LOW    = "sentinel:tasks:low"

# Heartbeat interval in seconds for agent health reporting
HEARTBEAT_INTERVAL = 60

# ── STRUCTURED AGENT COMMUNICATION PROTOCOL ──────────────────────────────────

class AgentBulletin(BaseModel):
    """Typed inter-agent communication message. Replaces free-text episodic memory."""
    agent_name: str
    bulletin_type: str  # "regime_change", "signal", "alert", "thesis", "contradiction"
    primary_entity_id: Optional[str] = None
    primary_entity_name: Optional[str] = None
    ticker: Optional[str] = None
    conviction: float = 0.5  # 0.0 - 1.0
    expected_direction: Optional[str] = None  # "up", "down", "neutral"
    payload: Dict[str, Any] = Field(default_factory=dict)
    summary: str = ""
    published_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    expires_at: Optional[str] = None  # ISO timestamp
    ttl_seconds: int = 3600  # Default 1 hour


class AgentPrediction(BaseModel):
    """Tracked prediction for self-calibration."""
    prediction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    agent_name: str
    ticker: str
    direction: str  # "up", "down"
    conviction: float
    time_horizon_hours: int = 24
    entry_price: float = 0.0
    target_price: float = 0.0
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    verified: bool = False
    outcome_correct: Optional[bool] = None


class AgentScorecard(BaseModel):
    """Performance tracking for agent self-calibration."""
    agent_name: str
    predictions_made: int = 0
    predictions_correct: int = 0
    predictions_wrong: int = 0
    mean_conviction_on_correct: float = 0.0
    mean_conviction_on_wrong: float = 0.0
    brier_score: float = 0.5  # Lower is better (0=perfect, 1=worst)
    consensus_weight: float = 1.0  # Multiplier in consensus engine
    last_calibrated_at: Optional[str] = None

class ThrottledLogger:
    """
    Prevents log swarming during high-frequency data bursts.
    Throttles repeated log entries by key so they log at most once per interval_sec seconds.
    """
    def __init__(self, logger_instance: logging.Logger, default_interval_sec: float = 10.0):
        self.logger = logger_instance
        self.default_interval = default_interval_sec
        self._last_logged: Dict[str, float] = {}

    def info(self, key: str, msg: str, *args, interval_sec: Optional[float] = None, **kwargs):
        now = time.time()
        ttl = interval_sec if interval_sec is not None else self.default_interval
        if key not in self._last_logged or (now - self._last_logged[key]) >= ttl:
            self._last_logged[key] = now
            self.logger.info(msg, *args, **kwargs)

    def warning(self, key: str, msg: str, *args, interval_sec: Optional[float] = None, **kwargs):
        now = time.time()
        ttl = interval_sec if interval_sec is not None else self.default_interval
        if key not in self._last_logged or (now - self._last_logged[key]) >= ttl:
            self._last_logged[key] = now
            self.logger.warning(msg, *args, **kwargs)

class SentinelAgent(ABC):
    _global_received_count = 0
    def __init__(self, agent_name: str, input_topics: List[str], redis_client, db_client, neo4j_client, producer, consumer, dlq, model="llama3", fallback_model: Optional[str] = None):
        self.name = agent_name
        self.input_topics = input_topics
        self.redis = redis_client 
        self.redis_client = redis_client
        self.db = db_client
        self.neo4j = neo4j_client
        self._producer = producer
        self._consumer = consumer
        self._dlq = dlq
        self.model = model
        self.fallback_model = fallback_model
        self.logger = logging.getLogger(f"agent.{agent_name}")
        self._processed = 0
        self._errors = 0
        self._started_at = datetime.now(timezone.utc)
        
        # Declared here for IDE support; instantiated inside the async event loop in run().
        self._session: Optional[aiohttp.ClientSession] = None
        self._llm: Optional[OllamaClient] = None
        
        # Concurrency bound: Limit inflight tasks to prevent memory explosion and LLM timeouts
        self._dispatch_semaphore = asyncio.Semaphore(int(os.getenv("AGENT_CONCURRENCY", "5")))

        # Cross-agent state synchronization (§3.3):
        # Track recently processed event IDs and entities for context drift detection.
        # ConsensusEngine reads these digests to detect when agents have
        # divergent world-states before fusing their bulletins.
        from collections import deque
        self._recent_event_ids: deque = deque(maxlen=20)
        self._recent_entities: deque = deque(maxlen=20)
        self._current_regime: Optional[str] = None  # Set by subclass if applicable
    @abstractmethod
    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pass

    @property
    @abstractmethod
    def output_topic(self) -> str:
        pass

    @property
    def producer(self):
        return self._producer

    async def run(self):
        from shared.utils.ollama import OLLAMA_TIMEOUT
        connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
        self._llm = OllamaClient(self._session, self.model, redis_client=self.redis_client)

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

                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r, msg in zip(results, msg_list):
                        if isinstance(r, Exception):
                            self.logger.error(f"Dispatch task failed with unhandled exception: {r}", exc_info=r)
                            try:
                                payload = json.loads(msg.value.decode('utf-8'))
                            except Exception:
                                payload = {"raw": str(msg.value)}
                            topic_name = tp.topic if hasattr(tp, 'topic') else (self.input_topics[0] if self.input_topics else "unknown")
                            await self._send_dlq(payload, f"UnhandledException: {type(r).__name__}: {str(r)}", topic_name)
                try:
                    await self._consumer.commit()
                except Exception as commit_err:
                    self.logger.warning(f"Consumer commit skipped (partition rebalance/timeout): {commit_err}")

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
                    # Broadcast agent decision brief to Redis PubSub for live WebSocket UI streaming
                    try:
                        res_dict = result if isinstance(result, dict) else (result.model_dump() if hasattr(result, "model_dump") else {})
                        agent_pub_payload = {
                            "event_id": str(res_dict.get("agent_run_id") or uuid.uuid4()),
                            "type": f"agent_{self.name}",
                            "occurred_at": datetime.now(timezone.utc).isoformat(),
                            "source": f"Agent Swarm ({self.name})",
                            "primary_entity_id": str(res_dict.get("primary_entity") or res_dict.get("ticker") or self.name),
                            "primary_entity_name": str(res_dict.get("primary_entity") or res_dict.get("name") or self.name.replace("_", " ").title()),
                            "entity_name": str(res_dict.get("primary_entity") or res_dict.get("name") or self.name.replace("_", " ").title()),
                            "headline": f"🤖 AGENT [{self.name.upper()}]: {res_dict.get('headline') or res_dict.get('summary') or res_dict.get('hypothesis') or res_dict.get('recommendation') or 'Intelligence Brief Synthesized'}",
                            "summary": str(res_dict.get("summary") or res_dict.get("rationale") or res_dict.get("narrative") or str(res_dict)[:200]),
                            "anomaly_score": float(res_dict.get("confidence") or res_dict.get("anomaly_score") or 0.85),
                            "region": "GLOBAL",
                            "tags": ["agent_output", f"agent:{self.name}"],
                        }
                        await self.redis.raw.publish("sentinel:events:live", json.dumps(agent_pub_payload))
                    except Exception as pub_err:
                        self.logger.debug(f"Agent live feed pub bypass: {pub_err}")
                self._processed += 1

                # Track event ID & entity for cross-agent state synchronization (§3.3)
                event_id = raw.get("event_id") or raw.get("agent_run_id")
                if event_id:
                    self._recent_event_ids.append(str(event_id))

                ent = raw.get("primary_entity_id") or raw.get("ticker") or raw.get("asset") or raw.get("primary_entity")
                if ent:
                    if isinstance(ent, dict):
                        ent = ent.get("id") or ent.get("name")
                    if ent:
                        self._recent_entities.append(str(ent))

                elapsed = time.monotonic() - t0
                if elapsed > 10:
                    self.logger.warning(f"Slow dispatch: {elapsed:.1f}s")
            except SchemaViolationError as e:
                self._errors += 1
                await self._send_dlq(raw, f"SchemaViolationError: {str(e)}", self.input_topics[0])
            except InferenceError as e:
                self._errors += 1
                self.logger.error(f"Inference error in agent {self.name}: {e}")
                await self._send_dlq(raw, f"InferenceError: {str(e)}", self.input_topics[0])
            except ValueError as e:
                self._errors += 1
                await self._send_dlq(raw, f"ValueError: {str(e)}", self.input_topics[0])
            except Exception as e:
                self.logger.error(f"Transient or unhandled dispatch error: {e}", exc_info=True)
                # Re-raise to crash the batch, skip commit, and preserve At-Least-Once delivery
                raise

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

            # Publish state digest for cross-agent context drift detection (§3.3)
            try:
                digest = {
                    "agent_name": self.name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "recent_event_ids": list(self._recent_event_ids),
                    "recent_entities": list(self._recent_entities),
                    "input_topics": self.input_topics,
                    "processed_count": self._processed,
                    "current_regime": self._current_regime,
                    "model": self.model,
                }
                await self.redis.raw.set(
                    f"sentinel:agent:digest:{self.name}",
                    json.dumps(digest),
                    ex=300,  # 5-minute TTL: if not refreshed, agent is stale
                )
            except Exception:
                pass

    def state_key(self, *parts: str) -> str:
        return f"sentinel:agents:{self.name}:{':'.join(parts)}"

    async def write_agent_memory(self, memory_text: str, ttl: int = 86400):
        """
        Writes a timestamped episodic memory to a shared Redis sorted set.
        Allows cross-agent asynchronous communication (e.g. Quant -> News).
        """
        try:
            now = time.time()
            memory_payload = json.dumps({
                "agent": self.name,
                "text": memory_text,
                "ts": datetime.now(timezone.utc).isoformat()
            })
            
            # Use ZADD with current timestamp as score for easy chronological retrieval
            await self.redis.raw.zadd("sentinel:agents:episodic_memory", mapping={memory_payload: now})
            
            # Trim the memory stream to keep only the 100 most recent memories to prevent bloat
            await self.redis.raw.zremrangebyrank("sentinel:agents:episodic_memory", 0, -101)
            
            # Note: We don't set TTL on the ZSET itself because it's a shared stream, 
            # we just prune old entries by rank. Alternatively could prune by score.
            
            self.logger.debug(f"Episodic memory stored: {memory_text[:50]}...")
        except Exception as e:
            self.logger.warning(f"Failed to write agent memory: {e}")

    async def read_agent_memories(self, limit: int = 5) -> str:
        """
        Reads the most recent cross-agent episodic memories.
        Returns a formatted string ready for LLM prompt injection.
        """
        try:
            # ZREVRANGE to get most recent first
            raw_memories = await self.redis.raw.zrevrange("sentinel:agents:episodic_memory", 0, limit - 1)
            
            if not raw_memories:
                return "No recent agent memories."
                
            context = "\n### CROSS-AGENT MEMORIES ###\n"
            for raw in raw_memories:
                try:
                    mem = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                    ts = str(mem.get('ts', ''))[:19]
                    context += f"• [{ts}] {mem.get('agent', 'UnknownAgent')}: {mem.get('text', '')}\n"
                except Exception:
                    pass
            return context
        except Exception as e:
            self.logger.warning(f"Failed to read agent memories: {e}")
            return "Failed to fetch memories."

    async def get_cross_agent_context(self, ticker: Optional[str] = None, entity_id: Optional[str] = None, limit: int = 3) -> str:
        """
        Concise, fully dynamic helper for agents to retrieve active bulletins, cross-agent memories, and swarm consensus for LLM prompt injection.
        Filtering out self-memories ensures strictly peer-agent intelligence is provided.
        """
        lines = []
        lookup_key = ticker or entity_id
        try:
            bulletins = await self.read_bulletins(ticker=lookup_key)
            if bulletins:
                # Exclude self-bulletins for pure cross-agent context
                peer_bulletins = [b for b in bulletins if b.agent_name != self.name]
                if peer_bulletins:
                    bulletin_strs = [f"[{b.agent_name}->{b.bulletin_type}] {b.summary}" for b in peer_bulletins[:3]]
                    lines.append("Active Bulletins:\n- " + "\n- ".join(bulletin_strs))
        except Exception as e:
            self.logger.debug(f"Bulletin fetch error: {e}")

        try:
            raw_consensus = await self.redis.raw.get("sentinel:consensus:latest")
            if raw_consensus:
                cons = json.loads(raw_consensus if isinstance(raw_consensus, str) else raw_consensus.decode("utf-8"))
                summary = cons.get("summary") or cons.get("consensus_summary")
                if summary:
                    lines.append(f"Swarm Consensus: {summary[:200]}")
        except Exception as e:
            self.logger.debug(f"Consensus fetch error: {e}")

        try:
            raw_mems = await self.redis.raw.zrevrange("sentinel:agents:episodic_memory", 0, limit * 2)
            if raw_mems:
                mem_strs = []
                for raw in raw_mems:
                    try:
                        m = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                        text = m.get('text', '')
                        agent_name = m.get('agent', 'Agent')
                        if text and agent_name != self.name:
                            mem_strs.append(f"[{agent_name}]: {text}")
                    except Exception:
                        pass
                if mem_strs:
                    lines.append("Cross-Agent Memories:\n- " + "\n- ".join(mem_strs[:limit]))
        except Exception as e:
            self.logger.debug(f"Memory fetch error: {e}")

        return "\n".join(lines) if lines else ""

    # ── STRUCTURED BULLETIN SYSTEM ──────────────────────────────────────────

    async def publish_bulletin(
        self,
        bulletin_type: str,
        summary: str,
        ticker: Optional[str] = None,
        primary_entity_id: Optional[str] = None,
        primary_entity_name: Optional[str] = None,
        conviction: float = 0.5,
        expected_direction: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = 3600,
    ) -> None:
        """
        Publishes a structured AgentBulletin to Redis.
        Other agents can query this via read_bulletins() or subscribe_bulletins().
        """
        try:
            ent_id = primary_entity_id or ticker
            ent_name = primary_entity_name or ent_id
            bulletin = AgentBulletin(
                agent_name=self.name,
                bulletin_type=bulletin_type,
                primary_entity_id=ent_id,
                primary_entity_name=ent_name,
                ticker=ticker,
                conviction=conviction,
                expected_direction=expected_direction,
                payload=payload or {},
                summary=summary,
                ttl_seconds=ttl_seconds,
            )
            key = f"sentinel:bulletins:{self.name}:{bulletin_type}"
            if ticker:
                key += f":{ticker.upper()}"
            await self.redis.raw.set(key, bulletin.model_dump_json(), ex=ttl_seconds)

            # Also publish to PubSub for real-time listeners
            await self.redis.raw.publish(
                "sentinel:bulletins:stream",
                bulletin.model_dump_json(),
            )
            self.logger.debug(f"Published bulletin: {bulletin_type} | {summary[:60]}")
        except Exception as e:
            self.logger.warning(f"Failed to publish bulletin: {e}")

    def safe_create_task(self, coro, task_name: Optional[str] = None) -> asyncio.Task:
        """
        Launches an async coroutine as a background task with exception logging callback.
        """
        task = asyncio.create_task(coro, name=task_name)
        def _on_done(t: asyncio.Task):
            try:
                if not t.cancelled() and t.exception():
                    self.logger.warning(f"Background task '{t.get_name()}' failed: {t.exception()}")
            except Exception as ex:
                self.logger.debug(f"Task callback error: {ex}")
        task.add_done_callback(_on_done)
        return task

    async def read_bulletins(
        self,
        agent_name: Optional[str] = None,
        bulletin_type: Optional[str] = None,
        ticker: Optional[str] = None,
    ) -> List[AgentBulletin]:
        """
        Reads active agent bulletins from Redis.
        Filters by agent_name, bulletin_type, and/or ticker.
        Returns typed AgentBulletin objects with automatic expiry filtering.
        """
        try:
            pattern_parts = ["sentinel:bulletins"]
            pattern_parts.append(agent_name if agent_name else "*")
            pattern_parts.append(bulletin_type if bulletin_type else "*")
            if ticker:
                pattern_parts.append(ticker.upper())
            else:
                pattern_parts.append("*")
            pattern = ":".join(pattern_parts)

            bulletins = []
            cursor = 0
            while True:
                cursor, keys = await self.redis.raw.scan(cursor=cursor, match=pattern, count=50)
                if keys:
                    values = await self.redis.raw.mget(keys)
                    for val in values:
                        if val:
                            try:
                                raw = val if isinstance(val, str) else val.decode("utf-8")
                                bulletins.append(AgentBulletin(**json.loads(raw)))
                            except Exception:
                                pass
                if cursor == 0:
                    break

            # Sort by published_at descending
            bulletins.sort(key=lambda b: b.published_at, reverse=True)
            return bulletins
        except Exception as e:
            self.logger.warning(f"Failed to read bulletins: {e}")
            return []

    async def subscribe_bulletins(self, types: List[str], limit: int = 10) -> List[AgentBulletin]:
        """
        Convenience: reads bulletins matching any of the given types across all agents.
        Returns sorted by recency, limited to `limit` results.
        """
        all_bulletins = []
        for btype in types:
            bulletins = await self.read_bulletins(bulletin_type=btype)
            all_bulletins.extend(bulletins)
        all_bulletins.sort(key=lambda b: b.published_at, reverse=True)
        return all_bulletins[:limit]

    async def get_bulletins_for_prompt(self, types: Optional[List[str]] = None, limit: int = 5) -> str:
        """
        Returns a formatted string of active bulletins for LLM prompt injection.
        """
        types = types or ["regime_change", "signal", "alert", "thesis"]
        bulletins = await self.subscribe_bulletins(types, limit=limit)
        if not bulletins:
            return ""
        context = "\n### ACTIVE AGENT BULLETINS ###\n"
        for b in bulletins:
            direction_str = f" ({b.expected_direction})" if b.expected_direction else ""
            ticker_str = f" [{b.ticker}]" if b.ticker else ""
            context += (
                f"- [{b.agent_name}] {b.bulletin_type}{ticker_str}{direction_str} "
                f"(conviction: {b.conviction:.0%}): {b.summary}\n"
            )
        return context + "\n"

    # ── AGENT SELF-CALIBRATION & PREDICTION TRACKING ───────────────────────

    async def record_prediction(
        self,
        ticker: str,
        direction: str,
        conviction: float,
        entry_price: float,
        target_price: float = 0.0,
        time_horizon_hours: int = 24,
    ) -> str:
        """
        Records a prediction for later verification.
        Returns the prediction_id.
        """
        try:
            pred = AgentPrediction(
                agent_name=self.name,
                ticker=ticker.upper(),
                direction=direction,
                conviction=conviction,
                entry_price=entry_price,
                target_price=target_price,
                time_horizon_hours=time_horizon_hours,
            )
            key = f"sentinel:predictions:{self.name}:{pred.prediction_id}"
            ttl = max(time_horizon_hours * 3600 + 7200, 86400)  # Horizon + 2h buffer, min 24h
            await self.redis.raw.set(key, pred.model_dump_json(), ex=ttl)

            # Index by ticker for fast lookup
            idx_key = f"sentinel:predictions:by_ticker:{ticker.upper()}"
            await self.redis.raw.sadd(idx_key, key)
            await self.redis.raw.expire(idx_key, ttl)

            self.logger.debug(f"Recorded prediction {pred.prediction_id}: {ticker} {direction} @ {conviction:.0%}")
            return pred.prediction_id
        except Exception as e:
            self.logger.warning(f"Failed to record prediction: {e}")
            return ""

    async def get_scorecard(self) -> AgentScorecard:
        """Retrieves or initializes the agent's performance scorecard from Redis."""
        try:
            key = f"sentinel:agents:scorecard:{self.name}"
            raw = await self.redis.raw.get(key)
            if raw:
                data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                return AgentScorecard(**data)
        except Exception as e:
            self.logger.debug(f"Scorecard fetch failed: {e}")
        return AgentScorecard(agent_name=self.name)

    async def update_scorecard(
        self,
        prediction_correct: bool,
        conviction: float,
    ) -> None:
        """Updates the agent's scorecard with a verified prediction outcome."""
        try:
            card = await self.get_scorecard()
            card.predictions_made += 1

            if prediction_correct:
                card.predictions_correct += 1
                # Running average of conviction on correct predictions
                n = card.predictions_correct
                card.mean_conviction_on_correct = (
                    card.mean_conviction_on_correct * (n - 1) + conviction
                ) / n
            else:
                card.predictions_wrong += 1
                n = card.predictions_wrong
                card.mean_conviction_on_wrong = (
                    card.mean_conviction_on_wrong * (n - 1) + conviction
                ) / n

            # Brier score update: BS = (1/N) Σ (forecast - outcome)²
            outcome = 1.0 if prediction_correct else 0.0
            total = card.predictions_made
            card.brier_score = (
                card.brier_score * (total - 1) + (conviction - outcome) ** 2
            ) / total

            # Adjust consensus weight based on calibration
            # Well-calibrated agents (low Brier) get higher weight
            card.consensus_weight = max(0.1, 1.0 - card.brier_score)
            card.last_calibrated_at = datetime.now(timezone.utc).isoformat()

            key = f"sentinel:agents:scorecard:{self.name}"
            await self.redis.raw.set(key, card.model_dump_json(), ex=604800)  # 7 day TTL

            if card.predictions_made % 10 == 0:
                accuracy = card.predictions_correct / max(1, card.predictions_made)
                self.logger.info(
                    f"📊 SCORECARD [{self.name}] | Accuracy: {accuracy:.0%} "
                    f"| Brier: {card.brier_score:.3f} | Weight: {card.consensus_weight:.2f} "
                    f"| Correct Conviction: {card.mean_conviction_on_correct:.0%} "
                    f"| Wrong Conviction: {card.mean_conviction_on_wrong:.0%}"
                )

                # Overconfidence alert
                if card.mean_conviction_on_wrong > 0.7 and card.predictions_wrong > 5:
                    self.logger.warning(
                        f"⚠️ OVERCONFIDENCE DETECTED [{self.name}]: Mean conviction on wrong predictions "
                        f"is {card.mean_conviction_on_wrong:.0%}. Consider reducing conviction thresholds."
                    )
        except Exception as e:
            self.logger.warning(f"Failed to update scorecard: {e}")

    async def is_recently_processed(self, entity_id: str, window_seconds: int = 3600) -> bool:
        res = await self.redis.raw.exists(self.state_key("seen", entity_id))
        return bool(res) if isinstance(res, (bool, int)) else False

    async def mark_processed(self, entity_id: str, window_seconds: int = 3600):
        await self.redis.raw.set(self.state_key("seen", entity_id), "1", ex=window_seconds)

    async def enqueue_task(self, task_type: str, payload: Dict, priority: str = "normal"):
        queue = {"high": TASK_QUEUE_HIGH, "normal": TASK_QUEUE_NORMAL, "low": TASK_QUEUE_LOW}.get(priority, TASK_QUEUE_NORMAL)
        task = {
            "task_id": str(uuid.uuid4()), "task_type": task_type, "agent": self.name,
            "payload": payload, "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.redis.raw.rpush(queue, json.dumps(task))

    async def fetch_entity_context(self, entity_name: str) -> str:
        """
        Surgically fetches recent ML anomalies and news that explicitly mention the entity.
        Prevents cross-domain context pollution while giving the LLM deep awareness.
        """
        try:
            # We use TimescaleDB directly for the absolute source of truth.
            query = """
                SELECT type, headline, anomaly_score, occurred_at
                FROM events
                WHERE occurred_at > NOW() - INTERVAL '24 hours'
                  AND (
                    primary_entity_id ILIKE $1 
                    OR headline ILIKE $2
                  )
                  AND (anomaly_score >= 0.5 OR type = 'headline')
                ORDER BY occurred_at DESC
                LIMIT 5
            """
            rows = await self.db.query(query, entity_name, f"%{entity_name}%")
            if not rows:
                return ""
                
            context = f"\n### RECENT ML ANOMALIES & NEWS FOR {entity_name.upper()} ###\n"
            for r in rows:
                score = f"(Anomaly Score: {r['anomaly_score']:.2f})" if r.get('anomaly_score') else ""
                context += f"- [{r['type']}] {r['headline']} {score}\n"
            return context + "\n"
        except Exception as e:
            self.logger.error(f"Failed to fetch entity context for {entity_name}: {e}")
            return ""

    async def fetch_global_context(self) -> str:
        """
        Fetches the top ML anomalies, latest news, and live outputs from upstream swarm agents
        (Yield Curve Rates, News Intel, Quant Researcher) to build a unified World State context.
        """
        try:
            anomaly_query = """
                SELECT type, headline, anomaly_score, occurred_at
                FROM events
                WHERE occurred_at > NOW() - INTERVAL '24 hours'
                  AND anomaly_score >= 0.65
                ORDER BY anomaly_score DESC
                LIMIT 5
            """
            anomalies = await self.db.query(anomaly_query)
            
            news_query = """
                SELECT type, headline, occurred_at
                FROM events
                WHERE occurred_at > NOW() - INTERVAL '24 hours'
                  AND type = 'headline'
                ORDER BY occurred_at DESC
                LIMIT 5
            """
            news = await self.db.query(news_query)
            
            context = "\n### GLOBAL SENTINEL SWARM WORLD STATE (LAST 24 HOURS) ###\n"
            context += "TOP ML ANOMALIES:\n"
            for r in anomalies:
                context += f"- [{r['type']}] {r['headline']} (Score: {r['anomaly_score']:.2f})\n"
                
            context += "\nLATEST GLOBAL NEWS:\n"
            for r in news:
                context += f"- {r['headline']}\n"

            # Ingest live rate regime & macro state from Redis cache
            try:
                rates_raw = await self.redis.raw.get("sentinel:macro:rates_regime:latest")
                if rates_raw:
                    rates_data = json.loads(rates_raw.decode("utf-8") if isinstance(rates_raw, bytes) else rates_raw)
                    context += f"\nLATEST RATES & CREDIT REGIME (YieldCurveMacroRatesAgent):\n"
                    context += f"- Curve State: {rates_data.get('curve_state', 'N/A')} | Spread: {rates_data.get('yield_spread_2y10y_bps', 0):+.1f} bps\n"
                    context += f"- Breakeven Inflation: {rates_data.get('breakeven_inflation_bps', 0):.1f} bps | TIPS Yield: {rates_data.get('tips_yield', 0):.2f}%\n"
                    context += f"- Credit Risk: {rates_data.get('credit_spread_widening_signal', 'Stable')} | Macro Risk: {rates_data.get('macro_risk_level', 'LOW')}\n"
            except Exception as rx:
                self.logger.debug(f"Rates regime cache miss in global context: {rx}")

            # Ingest recent shared agent memories
            try:
                mems = await self.redis.raw.zrevrange("sentinel:memory:shared", 0, 4)
                if mems:
                    context += "\nSHARED SWARM MEMORIES & INTEL:\n"
                    for m in mems:
                        text = m.decode("utf-8") if isinstance(m, bytes) else str(m)
                        context += f"- {text}\n"
            except Exception as mx:
                self.logger.debug(f"Shared memory miss in global context: {mx}")

            return context + "\n"
        except Exception as e:
            self.logger.error(f"Failed to fetch global context: {e}")
            return ""

    async def _execute_with_telemetry(
        self,
        message: dict,
        system_prompt: str,
        user_prompt: str,
        schema: Optional[Type[BaseModel]] = None,
        temperature: float = 0.1,
        model: Optional[str] = None,
        fallback_model: Optional[str] = OLLAMA_FALLBACK_MODEL,
        num_predict: Optional[int] = None,
    ) -> Any:
        
        start_time = time.monotonic()
        # Fallback to a UUID if no event_id is present (e.g., scheduled tasks)
        run_id = message.get("event_id", str(uuid.uuid4())[:8])
        
        if not getattr(self._producer, "_started", False):
            await self._producer.start()

        await self._producer.send(
            "agents.telemetry", 
            {
                "agent": self.name, 
                "status": "THINKING", 
                "task_id": run_id,
                "trace_id": message.get("trace_id", "unknown"),
                "system_prompt_length": len(system_prompt),
                "user_prompt_length": len(user_prompt)
            }
        )
        
        # Lazy initialize LLM client if agent was dispatched out-of-band without run()
        if self._llm is None:
            from shared.utils.ollama import OllamaClient, OLLAMA_TIMEOUT
            if self._session is None or self._session.closed:
                connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
            self._llm = OllamaClient(self._session, self.model, redis_client=self.redis_client)

        # 2. Execute LLM with Pydantic Enforcement & Truncation Fallback Retry
        try:
            response = await self._llm.infer(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema=schema,
                temperature=temperature,
                model=model or self.model,
                fallback_model=fallback_model or self.fallback_model,
                num_predict=num_predict or 512,
            )
        except (SchemaViolationError, InferenceError) as err:
            self.logger.warning(f"LLM inference schema/truncation error ({err}). Executing temperature-decayed fallback retry...")
            try:
                response = await self._llm.infer(
                    system_prompt=system_prompt + "\nIMPORTANT: Return valid, complete, and un-truncated JSON adhering strictly to the schema.",
                    user_prompt=user_prompt,
                    schema=schema,
                    temperature=0.0,
                    model=fallback_model or self.fallback_model or self.model,
                    num_predict=max(768, (num_predict or 512) * 2),
                )
            except Exception as retry_err:
                await self._producer.send(
                    "agents.telemetry",
                    {
                        "agent": self.name,
                        "status": "FAILED",
                        "task_id": run_id,
                        "latency_ms": round((time.monotonic() - start_time) * 1000, 2),
                        "error": f"Original: {err} | Retry: {retry_err}",
                    }
                )
                raise
        except Exception as err:
            await self._producer.send(
                "agents.telemetry",
                {
                    "agent": self.name,
                    "status": "FAILED",
                    "task_id": run_id,
                    "latency_ms": round((time.monotonic() - start_time) * 1000, 2),
                    "error": str(err),
                }
            )
            raise
        
        if hasattr(response, "model_dump"):
            output_payload = response.model_dump()
        elif hasattr(response, "dict"):
            output_payload = response.dict()
        else:
            output_payload = {"raw_text": str(response)}

        # 3. Log beginning snippet of agent output and emit telemetry
        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
        try:
            out_str = json.dumps(output_payload, separators=(',', ':'), default=str)
        except Exception:
            out_str = str(output_payload)
        preview_text = out_str[:10] + "..." if len(out_str) > 10 else out_str
        self.logger.info(f"✅ [{self.name}] Inference completed ({elapsed_ms}ms) | Output: {preview_text}")

        await self._producer.send(
            "agents.telemetry", 
            {
                "agent": self.name, 
                "status": "COMPLETE",
                "task_id": run_id,
                "latency_ms": elapsed_ms,
                "output_payload": output_payload
            }
        )
        return response

    async def verify_ticker_with_reasoning(self, ticker: str) -> bool:
        """
        Reasoning Service:
        Uses an LLM agentic verification step to double-check that a symbol is a valid 
        primary US common equity (or BTC) and NOT a derivative ETF, option, or crypto altcoin.
        """
        from shared.utils.equities import is_valid_primary_equity

        if not ticker or not isinstance(ticker, str):
            return False

        clean_ticker = ticker.strip().upper()
        if not is_valid_primary_equity(clean_ticker):
            return False

        class TickerVerificationDecision(BaseModel):
            valid: bool
            asset_type: str
            rationale: str

        prompt = f"""
        You are an institutional market metadata verification service.
        Verify if the symbol '{clean_ticker}' is a valid primary US common equity (e.g. AAPL, NVDA, TSLA) or Bitcoin (BTC).
        
        Strict Rules:
        - If '{clean_ticker}' is a YieldMax, Roundhill, Defiance, T-REX, GraniteShares, or any derivative ETF of a primary equity, set valid=false.
        - If '{clean_ticker}' is a crypto altcoin (ETH, SOL, XRP, DOGE, etc.), set valid=false.
        - If '{clean_ticker}' is an option, warrant, preferred share, or invalid token, set valid=false.
        - If '{clean_ticker}' is a legitimate primary operating company stock or BTC, set valid=true.
        
        Return ONLY valid JSON.
        Schema: {{"valid": boolean, "asset_type": "string", "rationale": "string"}}
        """

        try:
            decision = await self._execute_with_telemetry(
                message={"system": "ticker_verification", "ticker": clean_ticker},
                system_prompt="You are an institutional market metadata verification service.",
                user_prompt=prompt,
                schema=TickerVerificationDecision,
                temperature=0.0,
                num_predict=128,
                fallback_model="gemma3:1b"
            )

            if decision.valid:
                self.logger.info(f"✅ REASONING VERIFICATION PASSED: {clean_ticker} verified as {decision.asset_type}. Rationale: {decision.rationale}")
                return True
            else:
                self.logger.warning(f"⚠️ REASONING VERIFICATION REJECTED: {clean_ticker} rejected as {decision.asset_type}. Rationale: {decision.rationale}")
                return False
        except Exception as e:
            self.logger.warning(f"Ticker reasoning verification fallback for {clean_ticker}: {e}")
            return is_valid_primary_equity(clean_ticker)

    async def close(self):
        """Cleanly close all Kafka connections upon agent shutdown."""
        if hasattr(self, "_consumer") and self._consumer:
            try:
                await self._consumer.close()
            except Exception:
                pass
        if hasattr(self, "_producer") and self._producer:
            try:
                await self._producer.close()
            except Exception:
                pass
        if hasattr(self, "_dlq") and self._dlq:
            try:
                await self._dlq.close()
            except Exception:
                pass