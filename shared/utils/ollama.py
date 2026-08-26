"""
shared/utils/ollama.py
 
SENTINEL SHARED OLLAMA CLIENT (WITH DYNAMIC TIMEOUT OFFLOADING)
===============================================================
Single, shared HTTP client for all local inference across the platform.
Features:
  1. Performance Optimizations: Explicitly caps context size (`num_ctx: 4096`)
     to speed up prompt processing.
  2. Dynamic Offloading: Automatically switches and offloads requests to alternative
     pulled models (e.g. gemma:2b) upon detecting timeouts or inference errors.
"""

import asyncio
import hashlib
import json
import logging
import os
from functools import lru_cache
import re
import time
from typing import Any, Dict, Optional, Type
 
import aiohttp
from pydantic import BaseModel, ValidationError
from shared.utils.metrics import MetricsCollector
 
logger = logging.getLogger("sentinel.ollama")
 
OLLAMA_URL     = os.getenv("OLLAMA_URL", "http://sentinel-ollama:11434")
OLLAMA_MODEL   = os.getenv("AGENT_MODEL", "qwen2.5:7b")
OLLAMA_FALLBACK_MODEL = os.getenv("OLLAMA_FALLBACK_MODEL", "qwen2.5:1.5b")

# How long ollama holds a model in RAM after a request. Mirrors the server-side
# OLLAMA_KEEP_ALIVE so the client does not contradict it. Pinning permanently is
# the right trade on a CPU-only host with memory to spare: a cold load costs
# ~15s and the agent tiers are called in bursts.
#
# The env var and the REST field do NOT share a grammar. The server accepts the
# string "-1" to mean forever; the API's `keep_alive` field parses a *duration*,
# so "-1" fails with `time: missing unit in duration "-1"` and the request 400s.
# As a JSON number it is valid, and negative still means forever. Sending the
# env var through verbatim took the entire reasoning tier down: every inference
# returned 400, each agent burned its retry and fallback ladder on it, and the
# consumers wedged -- 0 messages/minute across every topic that calls a model.
def _coerce_keep_alive(raw: str):
    """Duration strings pass through; bare numbers become JSON numbers."""
    value = str(raw).strip()
    try:
        return int(value)
    except ValueError:
        return value


OLLAMA_KEEP_ALIVE = _coerce_keep_alive(os.getenv("OLLAMA_KEEP_ALIVE", "5m"))
# Dynamic timeout: Enforce 1200 seconds (20 mins) to allow local CPU/GPU heavy LLM inference completion
@lru_cache(maxsize=1)
def _inference_threads() -> Optional[int]:
    """llama.cpp worker threads, or None to let Ollama decide.

    Measured here: the ollama runner ran 16 threads against the 6-core cgroup
    quota in `cpus: '6.0'`. nproc and os.cpu_count() report the host's 12 CPUs
    and never see the quota, so llama.cpp sized its pool to CPUs the container
    may not use and the threads then contended for six cores' worth of
    scheduling -- visible as throughput below the cap (383% of 600%), not as an
    error.

    Read only from OLLAMA_NUM_THREAD, deliberately. The obvious implementation
    -- detect the quota from cgroup -- reads the *caller's* cgroup, and the
    caller is an agent container, not the model server: agents-heavy is capped
    at 2.5 cores, so it would have told a 6-core Ollama to use 2 threads and
    made inference slower than leaving it alone. Only the deployment knows the
    server's allocation, so it is stated rather than inferred.

    None means "send no num_thread", preserving Ollama's own default.
    """
    raw = os.getenv("OLLAMA_NUM_THREAD")
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Ignoring non-numeric OLLAMA_NUM_THREAD=%r", raw)
        return None
    if value < 1:
        logger.warning("Ignoring OLLAMA_NUM_THREAD=%s: must be >= 1", value)
        return None
    return value


# Token budgets for structured output.
#
# The ceilings exist because generation is linear in tokens on a CPU-only host.
# The defaults stay low for callers that do not state a need; a caller that asks
# for more has a schema that requires it, and truncating that request produces
# text no parser can use.
SMALL_MODEL_DEFAULT_TOKENS = int(os.getenv("OLLAMA_SMALL_DEFAULT_TOKENS", "384"))
# Measured: a scenario at 1024 tokens produced 4,056 characters and still had
# not closed its JSON. The schema is three nested hypotheses of seven fields
# each plus five top-level fields, so a complete answer runs past 1,500 tokens.
# Generation is linear in tokens -- 1024 took ~2m40s here -- which is why the
# default stays low and only a caller that declares a large schema pays for it.
SMALL_MODEL_MAX_TOKENS = int(os.getenv("OLLAMA_SMALL_MAX_TOKENS", "2048"))
LARGE_MODEL_DEFAULT_TOKENS = int(os.getenv("OLLAMA_LARGE_DEFAULT_TOKENS", "512"))
LARGE_MODEL_MAX_TOKENS = int(os.getenv("OLLAMA_LARGE_MAX_TOKENS", "2048"))


_raw_timeout = float(os.getenv("OLLAMA_TIMEOUT", "1200.0"))
OLLAMA_TIMEOUT = aiohttp.ClientTimeout(total=max(600.0, _raw_timeout))

# Circuit breaker config
CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("OLLAMA_CB_THRESHOLD", "3"))
CIRCUIT_BREAKER_COOLDOWN  = float(os.getenv("OLLAMA_CB_COOLDOWN", "15.0"))  # 15 seconds adaptive cooldown

# OLLAMA CONCURRENCY CAP (Option A: Static capacity partitioning across replicas)
# Rule: Client-side concurrency per process MUST satisfy:
#       OLLAMA_NUM_PARALLEL <= server_side_parallelism // known_replica_count
# Assumptions for current topology:
#   - Server-side Ollama max parallel runners (OLLAMA_SERVER_PARALLEL) = 4
#   - Active local agent process replicas running concurrently (AGENT_REPLICA_COUNT) = 4
#   - Process-local cap = floor(4 / 4) = 1 (prevents server-side queue congestion)
# Note: For dynamic scaling in cloud, replace with Redis distributed semaphore (Option B).
DEFAULT_PARALLEL_CAP = max(1, int(os.getenv("OLLAMA_SERVER_PARALLEL", "4")) // int(os.getenv("AGENT_REPLICA_COUNT", "1")))
OLLAMA_NUM_PARALLEL  = int(os.getenv("OLLAMA_NUM_PARALLEL", str(DEFAULT_PARALLEL_CAP)))


# Model Tier Preference Lists
MODEL_TIER_LIGHTWEIGHT = ["gemma:2b", "qwen2.5:7b", "llama3:latest"]
MODEL_TIER_HEAVY       = ["llama3:latest", "qwen2.5:7b", "gemma:2b"]

_GLOBAL_OLLAMA_SEMAPHORE: Optional[asyncio.Semaphore] = None
_GLOBAL_SEMAPHORE_LOOP: Optional[asyncio.AbstractEventLoop] = None


def get_ollama_semaphore(model_name: str = "default") -> asyncio.Semaphore:
    """Returns the process-global Ollama semaphore to enforce process-wide concurrency caps across all models and fallback chains."""
    global _GLOBAL_OLLAMA_SEMAPHORE, _GLOBAL_SEMAPHORE_LOOP
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if _GLOBAL_OLLAMA_SEMAPHORE is None or (current_loop is not None and _GLOBAL_SEMAPHORE_LOOP != current_loop):
        _GLOBAL_OLLAMA_SEMAPHORE = asyncio.Semaphore(OLLAMA_NUM_PARALLEL)
        _GLOBAL_SEMAPHORE_LOOP = current_loop
    return _GLOBAL_OLLAMA_SEMAPHORE



class InferenceError(Exception):
    """Ollama HTTP or network failure after retries."""

class SchemaViolationError(Exception):
    """LLM output could not be coerced to the required Pydantic schema."""


class OllamaClient:
    """
    Async Ollama client with schema enforcement, context sizing optimizations,
    adaptive circuit breaker, resilient model fallback retry chains, and optional Redis prompt caching.
    """
    def __init__(
        self,
        session: aiohttp.ClientSession,
        model: str = OLLAMA_MODEL,
        redis_client: Optional[Any] = None,
        service_name: Optional[str] = None,
    ):
        self._session = session
        self.model = model
        self.redis_client = redis_client
        self.service_name = service_name or os.getenv("SERVICE_NAME", "default")
        # Circuit breaker state (per-model)
        self._consecutive_timeouts: Dict[str, int] = {}
        # Set by _call_ollama when Ollama reports it stopped at the token limit,
        # so infer() can tell a truncated answer from a malformed one.
        self.last_truncated: bool = False
        self._circuit_open_until: Dict[str, float] = {}
        self.failures = self._consecutive_timeouts

    def is_circuit_open(self, model_name: str) -> bool:
        import time as _time
        consecutive = self._consecutive_timeouts.get(model_name, 0)
        open_until = self._circuit_open_until.get(model_name, 0.0)
        if consecutive >= CIRCUIT_BREAKER_THRESHOLD and _time.monotonic() < open_until:
            return True
        # Half-Open State: Cooldown expired, allow trial request
        if consecutive >= CIRCUIT_BREAKER_THRESHOLD and _time.monotonic() >= open_until:
            logger.info(f"⚡ Circuit breaker HALF-OPEN for model '{model_name}'. Allowing recovery trial.")
            return False
        return False

    async def _resolve_model(self, requested_model: str, exclude_models: Optional[set] = None) -> str:
        """Checks if the requested model exists in Ollama's tags; matches short/family names (e.g. qwen -> qwen2.5:7b) and avoids excluded/open circuit models."""
        exclude = set(exclude_models or [])
        try:
            async with self._session.get(f"{OLLAMA_URL}/api/tags", timeout=5.0) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    models = [m["name"] for m in data.get("models", [])]
                    
                    if requested_model in models:
                        return requested_model
                        
                    short_model = requested_model.split(":")[0].lower()
                    # 1. Match by exact base name (e.g. "llama3" matches "llama3:latest")
                    for m in models:
                        if m.split(":")[0].lower() == short_model:
                            return m
                            
                    # 2. Substring/family match (e.g. "qwen" matches "qwen2.5:latest" or "qwen2.5:7b")
                    for m in models:
                        base = m.split(":")[0].lower()
                        if short_model in base or base.startswith(short_model) or short_model.startswith(base):
                            return m

                    # 3. Fall back to an available non-excluded model if primary model tag is missing
                    available = [m for m in models if m not in exclude and not self.is_circuit_open(m)]
                    if available:
                        fallback = available[0]
                        logger.warning(
                            f"Model '{requested_model}' not found in Ollama. "
                            f"Automatically falling back to available model: '{fallback}'"
                        )
                        return fallback
        except Exception as e:
            logger.warning(f"Failed to query Ollama tags API: {e}. Defaulting to '{requested_model}'.")
            
        return requested_model

    async def _get_fallback_model(self, failed_model: str, exclude_models: Optional[set] = None) -> Optional[str]:
        """Finds an alternative model currently pulled in Ollama, avoiding failed/excluded models and open circuits."""
        exclude = set(exclude_models or [])
        exclude.add(failed_model)
        try:
            req_cm = self._session.get(f"{OLLAMA_URL}/api/tags", timeout=5.0)
            if hasattr(req_cm, "__aenter__"):
                async with req_cm as resp:
                    status = resp.status
                    data = await resp.json() if status == 200 else {}
            else:
                resp = await req_cm
                status = getattr(resp, "status", 200)
                data = await resp.json() if (status == 200 and callable(getattr(resp, "json", None))) else {}

            if status == 200 and isinstance(data, dict):
                models = [m["name"] for m in data.get("models", [])]
                
                def _base_name(name: str) -> str:
                    return name.split(":")[0].lower() if name else ""

                exclude_bases = {_base_name(m) for m in exclude if m}
                
                # Filter out models that failed, are excluded, or have open circuits
                alternatives = [
                    m for m in models 
                    if m not in exclude and _base_name(m) not in exclude_bases and not self.is_circuit_open(m)
                ]
                if not alternatives:
                    # Fallback recovery: Retry previously visited models if their circuit is half-open / recovered!
                    recovered = [m for m in models if not self.is_circuit_open(m)]
                    if recovered:
                        logger.info(f"🔄 All fallbacks attempted. Retrying recovered previous model: '{recovered[0]}'")
                        return recovered[0]
                    return None
                    
                # Prioritize lightweight models (gemma first, then 2b, 1.5b, 3b, qwen, tiny)
                priority_patterns = ["gemma", "2b", "1.5b", "3b", "qwen", "tiny"]
                for pattern in priority_patterns:
                    for alt in alternatives:
                        if pattern in alt.lower():
                            return alt
                            
                return alternatives[0]
        except Exception as e:
            logger.warning(f"Failed to fetch fallback models: {e}")
        return None

    async def infer(
        self, 
        system_prompt: str,
        user_prompt: str,
        schema: Type[BaseModel],
        temperature: float = 0.1,
        max_retries: int = 3,
        model: Optional[str] = None,
        fallback_model: Optional[str] = None,
        num_predict: Optional[int] = None,
        visited_models: Optional[set] = None,
    ) -> BaseModel:
        """
        Run inference and validate output against a Pydantic schema.
        Supports dynamic offloading to smaller models on timeout/inference failures.
        """
        visited = set(visited_models or [])
        active_model = model or self.model
        visited.add(active_model)

        if self.is_circuit_open(active_model):
            target_fallback = fallback_model if (fallback_model and fallback_model not in visited) else None
            if not target_fallback:
                target_fallback = await self._get_fallback_model(active_model, exclude_models=visited)
            if target_fallback and target_fallback not in visited:
                logger.warning(f"Circuit breaker OPEN for '{active_model}'. Offloading to fallback model: '{target_fallback}'")
                return await self.infer(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    schema=schema,
                    temperature=temperature,
                    max_retries=max_retries,
                    model=target_fallback,
                    fallback_model=None,
                    num_predict=num_predict,
                    visited_models=visited,
                )
            raise InferenceError(f"Circuit breaker OPEN for model '{active_model}'")

        # Check Redis prompt cache if available
        cache_key = None
        if self.redis_client is not None:
            try:
                h = hashlib.sha256(f"{active_model}:{system_prompt}:{user_prompt}".encode("utf-8")).hexdigest()
                cache_key = f"sentinel:llm_cache:{h}"
                cached_data = await self.redis_client.raw.get(cache_key)
                if cached_data:
                    parsed = json.loads(cached_data)
                    return schema(**parsed)
            except Exception as ce:
                logger.debug(f"Redis cache lookup bypass: {ce}")
                pass

        semaphore = get_ollama_semaphore(active_model)

        schema_dict = schema.model_json_schema() if hasattr(schema, "model_json_schema") else schema.schema()
        schema_dict.pop("title", None)
        schema_json = json.dumps(schema_dict, separators=(',', ':'))
        schema_instruction = (
            f"\n\nJSON SCHEMA:\n{schema_json}\n"
            "Return ONLY a raw JSON object conforming to this schema."
        )

        last_error: Optional[str] = None
        for attempt in range(max_retries):
            correction = ""
            if attempt > 0:
                correction = (
                    f"\n\n⚠️ CORRECTION REQUIRED (Attempt {attempt + 1}):\n"
                    f"Your previous response failed validation: {last_error}\n"
                    "You MUST return ONLY a raw JSON object.\n"
                    "No markdown. No explanation. No ```json fences.\n"
                    "Your entire response must start with { and end with }" 
                )

            full_prompt = f"{system_prompt}\n\n{user_prompt}{schema_instruction}{correction}"

            try:
                sem_start = time.monotonic()
                async with semaphore:
                    sem_wait = time.monotonic() - sem_start
                    MetricsCollector.observe_latency(f"ollama_semaphore_wait_seconds_{self.service_name}", sem_wait)

                    MetricsCollector.increment("ollama_calls_total")
                    MetricsCollector.increment(f"ollama_calls_{self.service_name}")
                    call_start = time.monotonic()
                    try:
                        raw_text = await self._call_ollama(full_prompt, temperature, active_model, format="json", num_predict=num_predict, exclude_models=visited)
                        MetricsCollector.observe_latency(f"ollama_latency_{self.service_name}", time.monotonic() - call_start)
                    except (InferenceError, asyncio.TimeoutError) as call_err:
                        MetricsCollector.increment(f"ollama_timeouts_{self.service_name}")
                        raise call_err

                parsed = self._extract_json(raw_text)
                if parsed is None:
                    if getattr(self, "last_truncated", False):
                        # Not a formatting failure. Retrying the same request
                        # reproduces it exactly, so the budget is raised first.
                        last_error = (
                            f"Response truncated at the token limit before the JSON closed "
                            f"({len(raw_text)} chars). Schema needs a larger num_predict."
                        )
                        logger.warning("Ollama attempt %s (%s): %s", attempt + 1, active_model, last_error)
                        num_predict = min(int((num_predict or SMALL_MODEL_DEFAULT_TOKENS) * 2), LARGE_MODEL_MAX_TOKENS)
                    else:
                        last_error = f"No valid JSON found in: {raw_text[:300]}"
                        logger.warning(f"Ollama attempt {attempt+1} ({active_model}): no JSON — {last_error[:100]}")
                    continue

                if isinstance(parsed, dict):
                    parsed = self._coerce_parsed_json(parsed, schema)

                if cache_key and self.redis_client is not None:
                    try:
                        await self.redis_client.raw.set(cache_key, json.dumps(parsed), ex=3600)
                    except Exception:
                        pass

                return schema(**parsed)
                
            except (InferenceError, asyncio.TimeoutError) as inf_err:
                last_error = f"Inference error: {inf_err}"
                logger.warning(f"Ollama attempt {attempt+1} ({active_model}) failed/timed out: {inf_err}")
                
                target_fallback = fallback_model if (fallback_model and fallback_model not in visited) else None
                if not target_fallback:
                    target_fallback = await self._get_fallback_model(active_model, exclude_models=visited)

                if target_fallback and target_fallback not in visited:
                    logger.warning(f"Failed calling '{active_model}'. Offloading to fallback model: '{target_fallback}'")
                    try:
                        return await self.infer(
                            system_prompt=system_prompt,
                            user_prompt=user_prompt,
                            schema=schema,
                            temperature=temperature,
                            max_retries=max_retries,
                            model=target_fallback,
                            fallback_model=None,
                            num_predict=num_predict,
                            visited_models=visited,
                        )
                    except InferenceError as fe:
                        raise fe
                else:
                    logger.info(f"No secondary fallback model pulled in local Ollama. Retrying on '{active_model}' (attempt {attempt+1}/{max_retries})...")
                
                # Exponential backoff before retrying
                await asyncio.sleep(2 ** attempt)

            except ValidationError as e:
                last_error = str(e)
                logger.warning(f"Ollama attempt {attempt+1} ({active_model}): invalid JSON — {last_error[:100]}")
                await asyncio.sleep(1.0)

        raise SchemaViolationError(
            f"Schema enforcement failed after {max_retries} attempts. Last: {last_error}"
        )
    
    async def infer_raw(
        self, 
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        max_retries: int = 3,
        model: Optional[str] = None,
        fallback_model: Optional[str] = None,
        num_predict: Optional[int] = None,
        visited_models: Optional[set] = None,
    ) -> str:
        """Raw inference without schema enforcement, supporting fallback offloading."""
        visited = set(visited_models or [])
        active_model = model or self.model
        visited.add(active_model)

        semaphore = get_ollama_semaphore(active_model)
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        
        # Circuit breaker check
        if self.is_circuit_open(active_model):
            consecutive = self._consecutive_timeouts.get(active_model, 0)
            logger.warning(f"Circuit breaker OPEN for model '{active_model}' ({consecutive} consecutive timeouts).")
            
            target_fallback = fallback_model if (fallback_model and fallback_model not in visited) else None
            if not target_fallback:
                target_fallback = await self._get_fallback_model(active_model, exclude_models=visited)

            if target_fallback and target_fallback not in visited:
                logger.warning(f"Rerouting to fallback model: '{target_fallback}'")
                return await self.infer_raw(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=temperature,
                    max_retries=max_retries,
                    model=target_fallback,
                    fallback_model=None,
                    num_predict=num_predict,
                    visited_models=visited,
                )
            raise InferenceError(
                f"Circuit breaker OPEN for model '{active_model}' — Ollama had {consecutive} "
                f"consecutive timeouts. Cooling down."
            )
        
        for attempt in range(max_retries):
            try:
                async with semaphore:
                    return await self._call_ollama(full_prompt, temperature, active_model, num_predict=num_predict, exclude_models=visited)
            except (InferenceError, asyncio.TimeoutError) as inf_err:
                logger.warning(f"Raw Ollama attempt {attempt+1} ({active_model}) failed/timed out: {inf_err}")
                
                target_fallback = fallback_model if (fallback_model and fallback_model not in visited) else None
                if not target_fallback:
                    target_fallback = await self._get_fallback_model(active_model, exclude_models=visited)

                if target_fallback and target_fallback not in visited:
                    logger.warning(f"Failed calling '{active_model}'. Recursively falling back to: '{target_fallback}'")
                    return await self.infer_raw(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        temperature=temperature,
                        max_retries=max_retries,
                        model=target_fallback,
                        fallback_model=None,
                        num_predict=num_predict,
                        visited_models=visited,
                    )

                await asyncio.sleep(2 ** attempt)
                
        raise InferenceError(f"Raw inference failed after {max_retries} attempts.")
        
    async def _call_ollama(self, prompt: str, temperature: float, active_model: str, format: Optional[str] = None, num_predict: Optional[int] = None, exclude_models: Optional[set] = None) -> str:
        import time as _time

        try:
            resolved_model = await self._resolve_model(active_model, exclude_models=exclude_models)
        except TypeError:
            resolved_model = await self._resolve_model(active_model)

        # Truncate prompt based on model capacity to prevent prompt-processing timeout on lightweight models
        clean_prompt = prompt
        model_lower = resolved_model.lower()
        is_small_model = any(tag in model_lower for tag in ["1b", "1.5b", "2b", "gemma", "tiny"])
        max_prompt_chars = 3500 if is_small_model else 6000

        if len(clean_prompt) > max_prompt_chars:
            clean_prompt = clean_prompt[:max_prompt_chars] + "\n...[truncated for speed]"

        payload = {
            "model": resolved_model,
            "prompt": clean_prompt,
            "stream": False,
            # Residency is a deployment decision, not a per-request one. This
            # was hardcoded to -1, which silently overrode the server's
            # OLLAMA_KEEP_ALIVE and pinned every model forever regardless of
            # what the container was configured with. Deferring to the server
            # makes the declared config the operative one.
            "keep_alive": OLLAMA_KEEP_ALIVE,
            "options": {
                "temperature": temperature,
                # The caller's request is honoured up to a ceiling, rather than
                # silently halved.
                #
                # This read min(num_predict or 384, 512) for small models. The
                # scenario generator explicitly asks for 1024 because its schema
                # is a dozen fields with three nested hypotheses; it received
                # 512, the response was cut off mid-object, and _extract_json
                # needs a closing brace, so every scenario failed to parse. The
                # log then reported "No valid JSON found", which points at the
                # model's formatting rather than at the token budget that caused
                # it. Zero scenarios have ever been persisted.
                #
                # A slower complete answer beats a fast unusable one: nine
                # minutes spent producing unparseable text is nine minutes lost.
                "num_predict": min(num_predict or SMALL_MODEL_DEFAULT_TOKENS, SMALL_MODEL_MAX_TOKENS)
                               if is_small_model
                               else min(num_predict or LARGE_MODEL_DEFAULT_TOKENS, LARGE_MODEL_MAX_TOKENS),
                "num_ctx": 3072 if is_small_model else 4096,  # Optimized context window size in tokens
                "stop": ["</json>", "Human:", "User:", "Assistant:"]
            }
        }
        _threads = _inference_threads()
        if _threads is not None:
            payload["options"]["num_thread"] = _threads
        if format:
            payload["format"] = format
            
        try:
            req_cm = self._session.post(
                f"{OLLAMA_URL}/api/generate",
                json=payload,
                timeout=OLLAMA_TIMEOUT,
            )
            if hasattr(req_cm, "__aenter__"):
                async with req_cm as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        raise InferenceError(f"Ollama HTTP {resp.status}: {body[:300]}")
                    data = await resp.json()
            else:
                resp = await req_cm
                if isinstance(resp, str):
                    try:
                        data = json.loads(resp)
                    except Exception:
                        data = {"response": resp}
                elif hasattr(resp, "status") and isinstance(resp.status, int) and resp.status != 200:
                    body = await resp.text() if hasattr(resp, "text") else str(resp)
                    raise InferenceError(f"Ollama HTTP {resp.status}: {body[:300]}")
                elif hasattr(resp, "json"):
                    import inspect
                    if inspect.iscoroutinefunction(resp.json) or asyncio.iscoroutinefunction(resp.json):
                        data = await resp.json()
                    else:
                        res = resp.json()
                        if inspect.iscoroutine(res) or hasattr(res, "__await__"):
                            data = await res
                        elif isinstance(res, (dict, list, str)):
                            data = res
                        elif hasattr(res, "return_value") and isinstance(getattr(res, "return_value"), (dict, list, str)):
                            ret = res.return_value
                            if isinstance(ret, str):
                                try:
                                    data = json.loads(ret)
                                except Exception:
                                    data = {"response": ret}
                            else:
                                data = ret
                        else:
                            data = {"response": str(res)}
                else:
                    data = {"response": str(resp)}

            self._consecutive_timeouts[active_model] = 0
            if not isinstance(data, dict):
                return str(data)

            # Ollama says why it stopped. "length" means the answer was cut off
            # at num_predict, which for a JSON schema means an object with no
            # closing brace -- unparseable by construction. Recording it here
            # lets the caller report a token-budget problem instead of blaming
            # the model's formatting, which is what "No valid JSON found" did
            # for every scenario this service ever attempted.
            if data.get("done_reason") == "length":
                logger.warning(
                    "Ollama truncated '%s' at the token limit (%s tokens produced). "
                    "The response is incomplete; raise num_predict for this schema.",
                    active_model, data.get("eval_count"),
                )
                self.last_truncated = True
            else:
                self.last_truncated = False
            return data.get("response", "")
            
        except asyncio.TimeoutError:
            self._consecutive_timeouts[active_model] = self._consecutive_timeouts.get(active_model, 0) + 1
            consec = self._consecutive_timeouts[active_model]
            
            # Extract first 150 chars of prompt for diagnostic preview
            prompt_preview = clean_prompt.replace("\n", " ")[:20]
            logger.error(
                f"⏱ OLLAMA TIMEOUT ({OLLAMA_TIMEOUT.total}s) | Model: '{active_model}' (resolved: '{resolved_model}') | "
                f"Prompt Length: {len(clean_prompt)} chars | Consecutive Timeouts: {consec} | "
                f"Prompt Snippet: '{prompt_preview}...'"
            )
            
            if consec >= CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open_until[active_model] = _time.monotonic() + CIRCUIT_BREAKER_COOLDOWN
                logger.warning(
                    f"🚨 CIRCUIT BREAKER OPENED for '{active_model}' after {consec} "
                    f"consecutive timeouts. Cooling down for {CIRCUIT_BREAKER_COOLDOWN}s."
                )
            raise InferenceError(f"Ollama timed out after {OLLAMA_TIMEOUT.total}s for model '{active_model}' (prompt_len={len(clean_prompt)})")
        except aiohttp.ClientError as e:
            raise InferenceError(f"Ollama connection error: {e}")

    @staticmethod
    def _extract_json(text: str) -> Optional[Dict]:
        text = text.strip()
        try:
            direct = json.loads(text)
            # A model that wraps its object in an array still meant the object.
            # Returning the list violated this function's own contract and blew
            # up at schema(**parsed) one frame later.
            if isinstance(direct, list) and len(direct) == 1 and isinstance(direct[0], dict):
                return direct[0]
            if isinstance(direct, dict):
                return direct
        except json.JSONDecodeError:
            pass

        stripped = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        # Only a *top-level* array, anchored at the start of the response.
        #
        # Unanchored, this matched the nested "hypotheses" array of a truncated
        # scenario and returned its first element as the whole object -- so a
        # cut-off scenario came back as a lone hypothesis with none of the
        # required top-level fields.
        stripped_text = text.strip()
        if stripped_text.startswith("["):
            match = re.search(r"\[.*\]", stripped_text, re.DOTALL)
            if match:
                try:
                    result = json.loads(match.group(0))
                    if isinstance(result, list) and len(result) == 1 and isinstance(result[0], dict):
                        return result[0]
                except json.JSONDecodeError:
                    pass

        return OllamaClient._repair_truncated_json(text)

    @staticmethod
    def _repair_truncated_json(text: str) -> Optional[Dict]:
        """Closes a JSON object the model ran out of tokens to finish.

        Small models pad: a scenario schema that needs ~2,000 characters came
        back at 7,596 and still had not closed, so raising the budget only buys
        more padding. A truncated object is not a corrupt one -- the prefix is
        valid, and everything up to the cut is exactly what the model meant.

        The incomplete trailing element is discarded and the open structures are
        closed. Nothing is invented: no value is supplied that the model did not
        produce, so a scenario recovered this way has fewer hypotheses, never
        fabricated ones. If required fields are missing the schema rejects it,
        which is the correct outcome.
        """
        start = text.find("{")
        if start < 0:
            return None
        body = text[start:]

        # Walk the text tracking structure, ignoring braces inside strings.
        stack, in_string, escaped, last_safe = [], False, False, None
        for i, ch in enumerate(body):
            if escaped:
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch in "{[":
                stack.append(ch)
            elif ch in "}]":
                if stack:
                    stack.pop()
            elif ch == "," and len(stack) <= 2:
                # A comma at shallow depth ends a complete element, so this is a
                # point the text can be truncated back to without losing a
                # half-written one.
                last_safe = i

        if not stack:
            return None                      # not truncated; earlier passes failed for another reason

        for candidate in (len(body), last_safe):
            if candidate is None:
                continue
            prefix = body[:candidate].rstrip().rstrip(",")
            # Close whatever is still open, innermost first.
            depth, in_str, esc = [], False, False
            for ch in prefix:
                if esc:
                    esc = False
                    continue
                if ch == "\\":
                    esc = True
                    continue
                if ch == '"':
                    in_str = not in_str
                    continue
                if in_str:
                    continue
                if ch in "{[":
                    depth.append(ch)
                elif ch in "}]" and depth:
                    depth.pop()
            repaired = prefix + ('"' if in_str else "")
            repaired += "".join("}" if ch == "{" else "]" for ch in reversed(depth))
            try:
                parsed = json.loads(repaired)
                if isinstance(parsed, dict):
                    dropped = OllamaClient._drop_partial_tail(parsed)
                    logger.info(
                        "Recovered a truncated JSON object (%s of %s chars kept%s).",
                        len(prefix), len(body),
                        f", {dropped} incomplete element(s) dropped" if dropped else "",
                    )
                    return parsed
            except json.JSONDecodeError:
                continue

        return None

    @staticmethod
    def _drop_partial_tail(obj: Dict) -> int:
        """Removes a trailing list element that was cut off mid-write.

        Closing the structure recovers a final element that has only the fields
        the model managed to emit -- {"label": "h2"} where its siblings carry
        seven keys. That is not invented data, but it is incomplete, and a
        required-field schema rejects the whole object because of it, which
        turns a partial success back into a total loss.

        Elements of a schema-driven array share a shape, so a last element with
        strictly fewer keys than its predecessor was truncated and is dropped.
        """
        dropped = 0
        for value in obj.values():
            if not isinstance(value, list) or len(value) < 2:
                continue
            last, previous = value[-1], value[-2]
            if isinstance(last, dict) and isinstance(previous, dict):
                if set(last) < set(previous):
                    value.pop()
                    dropped += 1
        return dropped

    @staticmethod
    def _coerce_parsed_json(parsed: Any, schema: Type[BaseModel]) -> Any:
        if not isinstance(parsed, dict):
            return parsed

        model_fields = getattr(schema, "model_fields", None)
        if not model_fields and hasattr(schema, "__fields__"):
            model_fields = schema.__fields__
        if not model_fields:
            return parsed

        coerced = dict(parsed)
        for field_name, field_info in model_fields.items():
            if field_name not in coerced or coerced[field_name] is None:
                continue

            val = coerced[field_name]
            annotation = getattr(field_info, "annotation", None) or getattr(field_info, "type_", None)
            if annotation is None:
                continue
            annotation_str = str(annotation)

            # 1. List fields receiving str or dict
            if "List[" in annotation_str or "list[" in annotation_str or annotation is list:
                if isinstance(val, str):
                    if "," in val:
                        coerced[field_name] = [x.strip() for x in val.split(",") if x.strip()]
                    elif val.strip():
                        coerced[field_name] = [val.strip()]
                    else:
                        coerced[field_name] = []
                elif isinstance(val, dict):
                    coerced[field_name] = [val]

            # 2. String fields receiving list or dict
            elif annotation is str or "str" in annotation_str:
                if isinstance(val, list):
                    coerced[field_name] = " ".join(str(x) for x in val)
                elif isinstance(val, dict):
                    coerced[field_name] = json.dumps(val)

            # 3. Numeric fields receiving str or float
            elif annotation is int or "int" in annotation_str:
                if isinstance(val, (str, float)):
                    try:
                        coerced[field_name] = int(float(val))
                    except (ValueError, TypeError):
                        pass

        return coerced