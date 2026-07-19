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
import json
import logging
import os
import re
from typing import Any, Dict, Optional, Type
 
import aiohttp
from pydantic import BaseModel, ValidationError
 
logger = logging.getLogger("sentinel.ollama")
 
OLLAMA_URL     = os.getenv("OLLAMA_URL", "http://sentinel-ollama:11434")
OLLAMA_MODEL   = os.getenv("AGENT_MODEL", "llama3")
# Fail faster (60 seconds) to enable quick fallback offloading rather than blocking for 2 mins
OLLAMA_TIMEOUT = aiohttp.ClientTimeout(total=float(os.getenv("OLLAMA_TIMEOUT", "60.0")))

# Circuit breaker config
CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("OLLAMA_CB_THRESHOLD", "3"))
CIRCUIT_BREAKER_COOLDOWN  = float(os.getenv("OLLAMA_CB_COOLDOWN", "300.0"))  # 5 minutes
OLLAMA_NUM_PARALLEL       = int(os.getenv("OLLAMA_NUM_PARALLEL", "4"))

_OLLAMA_SEMAPHORES = {}
 

def get_ollama_semaphore(model_name: str = "default"):
    global _OLLAMA_SEMAPHORES
    if model_name not in _OLLAMA_SEMAPHORES:
        _OLLAMA_SEMAPHORES[model_name] = asyncio.Semaphore(OLLAMA_NUM_PARALLEL)
    return _OLLAMA_SEMAPHORES[model_name]


class InferenceError(Exception):
    """Ollama HTTP or network failure after retries."""

class SchemaViolationError(Exception):
    """LLM output could not be coerced to the required Pydantic schema."""


class OllamaClient:
    """
    Async Ollama client with schema enforcement, context sizing optimizations, and dynamic fallback offloading.
    """
    def __init__(
        self,
        session: aiohttp.ClientSession,
        model: str = OLLAMA_MODEL,
    ):
        self._session = session
        self.model = model
        # Circuit breaker state (per-model)
        self._consecutive_timeouts: Dict[str, int] = {}
        self._circuit_open_until: Dict[str, float] = {}
        self.failures = self._consecutive_timeouts

    async def _resolve_model(self, requested_model: str) -> str:
        """Checks if the requested model exists in Ollama's tags; falls back to the first available model if missing."""
        try:
            async with self._session.get(f"{OLLAMA_URL}/api/tags", timeout=5.0) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    models = [m["name"] for m in data.get("models", [])]
                    
                    if requested_model in models:
                        return requested_model
                        
                    # Match by base name (e.g. "llama3" matches "llama3:latest")
                    short_model = requested_model.split(":")[0].lower()
                    for m in models:
                        if m.split(":")[0].lower() == short_model:
                            return m
                            
                    # Fall back to the first available model if primary is missing
                    if models:
                        fallback = models[0]
                        logger.warning(
                            f"Model '{requested_model}' not found in Ollama. "
                            f"Automatically falling back to first available model: '{fallback}'"
                        )
                        return fallback
        except Exception as e:
            logger.warning(f"Failed to query Ollama tags API: {e}. Defaulting to '{requested_model}'.")
            
        return requested_model

    async def _get_fallback_model(self, failed_model: str) -> Optional[str]:
        """Finds an alternative model currently pulled in Ollama, preferring smaller models."""
        try:
            async with self._session.get(f"{OLLAMA_URL}/api/tags", timeout=5.0) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    models = [m["name"] for m in data.get("models", [])]
                    
                    # Filter out the model that just failed
                    alternatives = [m for m in models if m != failed_model]
                    if not alternatives:
                        return None
                        
                    # Prioritize smaller/faster models (containing 2b, 1.5b, 3b, gemma, qwen, tiny)
                    priority_patterns = ["2b", "1.5b", "3b", "gemma", "qwen", "tiny"]
                    for pattern in priority_patterns:
                        for alt in alternatives:
                            if pattern in alt.lower():
                                return alt
                                
                    # Default to the first alternative
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
    ) -> BaseModel:
        """
        Run inference and validate output against a Pydantic schema.
        Supports dynamic offloading to smaller models on timeout/inference failures.
        """
        last_error: Optional[str] = None
        active_model = model or self.model
        semaphore = get_ollama_semaphore(active_model)

        # Circuit breaker check
        import time as _time
        current_time = _time.monotonic()
        consecutive = self._consecutive_timeouts.get(active_model, 0)
        open_until = self._circuit_open_until.get(active_model, 0.0)

        if consecutive >= CIRCUIT_BREAKER_THRESHOLD:
            if current_time < open_until:
                logger.warning(f"Circuit breaker OPEN for model '{active_model}' ({consecutive} consecutive timeouts).")
                if fallback_model:
                    logger.warning(f"Rerouting to fallback model: '{fallback_model}'")
                    return await self.infer(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        schema=schema,
                        temperature=temperature,
                        max_retries=max_retries,
                        model=fallback_model,
                        fallback_model=None,
                        num_predict=num_predict,
                    )
                raise InferenceError(
                    f"Circuit breaker OPEN for model '{active_model}' — Ollama had {consecutive} "
                    f"consecutive timeouts. Cooling down."
                )
            logger.info(f"Circuit breaker: cooldown expired for model '{active_model}', probing Ollama...")

        schema_json = json.dumps(schema.model_json_schema() if hasattr(schema, "model_json_schema") else schema.schema(), indent=2)
        schema_instruction = (
            f"\n\nYou MUST return a JSON object that strictly adheres to the following JSON schema:\n"
            f"```json\n{schema_json}\n```\n"
            "Do not include any explanation or markdown formatting outside of the JSON object. Output raw JSON only.\n"
            "CRITICAL: Output the actual data values that fit this schema, DO NOT output the schema definition itself."
        )

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
                async with semaphore:
                    raw_text = await self._call_ollama(full_prompt, temperature, active_model, format="json", num_predict=num_predict)

                parsed = self._extract_json(raw_text)
                if parsed is None:
                    last_error = f"No valid JSON found in: {raw_text[:300]}"
                    logger.warning(f"Ollama attempt {attempt+1} ({active_model}): no JSON — {last_error[:100]}")
                    continue

                return schema(**parsed)
                
            except (InferenceError, asyncio.TimeoutError) as inf_err:
                last_error = f"Inference error: {inf_err}"
                logger.warning(f"Ollama attempt {attempt+1} ({active_model}) failed/timed out: {inf_err}")
                
                # Recursive fallback
                if fallback_model:
                    logger.warning(f"Failed calling '{active_model}'. Recursively falling back to: '{fallback_model}'")
                    return await self.infer(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        schema=schema,
                        temperature=temperature,
                        max_retries=max_retries,
                        model=fallback_model,
                        fallback_model=None,
                        num_predict=num_predict,
                    )
                
                # Dynamic Offloading fallback
                fallback = await self._get_fallback_model(active_model)
                if fallback:
                    logger.warning(f"Offloading next attempt to fallback model: '{fallback}'")
                    return await self.infer(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        schema=schema,
                        temperature=temperature,
                        max_retries=max_retries,
                        model=fallback,
                        fallback_model=None,
                        num_predict=num_predict,
                    )
                else:
                    logger.warning("No fallback models available in local Ollama. Retrying with same model.")
                
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
    ) -> str:
        """Raw inference without schema enforcement, supporting fallback offloading."""
        active_model = model or self.model
        semaphore = get_ollama_semaphore(active_model)
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        
        # Circuit breaker check
        import time as _time
        current_time = _time.monotonic()
        consecutive = self._consecutive_timeouts.get(active_model, 0)
        open_until = self._circuit_open_until.get(active_model, 0.0)

        if consecutive >= CIRCUIT_BREAKER_THRESHOLD:
            if current_time < open_until:
                logger.warning(f"Circuit breaker OPEN for model '{active_model}' ({consecutive} consecutive timeouts).")
                if fallback_model:
                    logger.warning(f"Rerouting to fallback model: '{fallback_model}'")
                    return await self.infer_raw(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        temperature=temperature,
                        max_retries=max_retries,
                        model=fallback_model,
                        fallback_model=None,
                        num_predict=num_predict,
                    )
                raise InferenceError(
                    f"Circuit breaker OPEN for model '{active_model}' — Ollama had {consecutive} "
                    f"consecutive timeouts. Cooling down."
                )
            logger.info(f"Circuit breaker: cooldown expired for model '{active_model}', probing Ollama...")
        
        for attempt in range(max_retries):
            try:
                async with semaphore:
                    return await self._call_ollama(full_prompt, temperature, active_model, num_predict=num_predict)
            except (InferenceError, asyncio.TimeoutError) as inf_err:
                logger.warning(f"Raw Ollama attempt {attempt+1} ({active_model}) failed/timed out: {inf_err}")
                
                # Recursive fallback
                if fallback_model:
                    logger.warning(f"Failed calling '{active_model}'. Recursively falling back to: '{fallback_model}'")
                    return await self.infer_raw(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        temperature=temperature,
                        max_retries=max_retries,
                        model=fallback_model,
                        fallback_model=None,
                        num_predict=num_predict,
                    )

                fallback = await self._get_fallback_model(active_model)
                if fallback:
                    logger.warning(f"Offloading raw inference to fallback model: '{fallback}'")
                    return await self.infer_raw(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        temperature=temperature,
                        max_retries=max_retries,
                        model=fallback,
                        fallback_model=None,
                        num_predict=num_predict,
                    )
                await asyncio.sleep(2 ** attempt)
                
        raise InferenceError(f"Raw inference failed after {max_retries} attempts.")
        
    async def _call_ollama(self, prompt: str, temperature: float, active_model: str, format: Optional[str] = None, num_predict: Optional[int] = None) -> str:
        import time as _time

        resolved_model = await self._resolve_model(active_model)

        payload = {
            "model": resolved_model,
            "prompt": prompt,
            "stream": False,
            "keep_alive": -1,  # Keep model permanently loaded
            "options": {
                "temperature": temperature,
                "num_predict": num_predict or 1000,
                "num_ctx": 4096,  # Performance optimization for local inference
                "stop": ["</json>", "Human:", "User:", "Assistant:"]
            }
        }
        if format:
            payload["format"] = format
            
        try:
            async with self._session.post(
                f"{OLLAMA_URL}/api/generate",
                json=payload,
                timeout=OLLAMA_TIMEOUT,
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise InferenceError(f"Ollama HTTP {resp.status}: {body[:300]}")
                data = await resp.json()
                self._consecutive_timeouts[active_model] = 0
                return data.get("response", "")
            
        except asyncio.TimeoutError:
            self._consecutive_timeouts[active_model] = self._consecutive_timeouts.get(active_model, 0) + 1
            if self._consecutive_timeouts[active_model] >= CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open_until[active_model] = _time.monotonic() + CIRCUIT_BREAKER_COOLDOWN
                logger.warning(
                    f"Circuit breaker OPENED for '{active_model}' after {self._consecutive_timeouts[active_model]} "
                    f"consecutive timeouts. Cooling down for {CIRCUIT_BREAKER_COOLDOWN}s."
                )
            raise InferenceError(f"Ollama timed out after {OLLAMA_TIMEOUT.total}s")
        except aiohttp.ClientError as e:
            raise InferenceError(f"Ollama connection error: {e}")

    @staticmethod
    def _extract_json(text: str) -> Optional[Dict]:
        text = text.strip()
        try:
            return json.loads(text)
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

        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group(0))
                if isinstance(result, list) and len(result) == 1 and isinstance(result[0], dict):
                    return result[0]
            except json.JSONDecodeError:
                pass
 
        return None