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
    OllamaClient, SchemaViolationError, InferenceError,
    OLLAMA_MODEL, OLLAMA_FALLBACK_MODEL, OLLAMA_URL
)

# Task Queue Keys
TASK_QUEUE_HIGH   = "sentinel:tasks:high"
TASK_QUEUE_NORMAL = "sentinel:tasks:normal"
TASK_QUEUE_LOW    = "sentinel:tasks:low"

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
                
            context = "\n### RECENT CROSS-AGENT MEMORIES ###\n"
            for raw in raw_memories:
                try:
                    mem = json.loads(raw)
                    context += f"- [{mem.get('ts', 'unknown')}] {mem.get('agent', 'UnknownAgent')}: {mem.get('text', '')}\n"
                except Exception:
                    pass
            return context + "\n"
        except Exception as e:
            self.logger.warning(f"Failed to read agent memories: {e}")
            return "Failed to fetch memories."

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
                fallback_model="gemma:2b"
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