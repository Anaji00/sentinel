"""
services/agents/base.py
 
SENTINEL AGENT BASE CLASS
==========================
Foundation for all autonomous Llama3-powered research agents.
 
Architecture decisions:
  - Kafka is the primary inter-agent communication bus (durable, replayable)
  - Redis RPUSH/BLPOP provides a lightweight priority task queue for long-running
    LLM inference (10-30s) without adding Celery/broker complexity.
  - asyncio.Semaphore serializes Ollama requests — local GPU is single-threaded.
  - JSON schema enforcement with retry loop (Llama3 occasionally drifts).
  - Every agent has a dead-letter queue fallback.
 
Ollama note: All agents use the local ollama:11434 endpoint (unmetered, private).
The existing Gemini path in reasoning/scenario_generator.py remains for final
Tier 3 synthesis — agents use Llama3 for research, Gemini for final product.
"""
 
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
from pydantic import BaseModel, ValidationError
 
logger = logging.getLogger(__name__)
 
# ── OLLAMA CONFIG ─────────────────────────────────────────────────────────────
OLLAMA_URL    = os.getenv("OLLAMA_URL", "http://sentinel-ollama:11434")
OLLAMA_MODEL  = os.getenv("AGENT_MODEL", "llama3")
OLLAMA_TIMEOUT = aiohttp.ClientTimeout(total=90)   # LLM can take 30-60s locally

# Global semaphore: one Ollama request at a time across ALL agents.
# Local GPU/CPU is the bottleneck — parallelism here causes degraded output, not speedup.
_OLLAMA_SEMAPHORE = asyncio.Semaphore(1)

# Lightweight priority task queue implemented on Redis lists.
# Agents push tasks with RPUSH, workers pull with BLPOP.
# No Celery required — Redis is already in the stack.

TASK_QUEUE_HIGH   = "sentinel:agents:tasks:high"    # Emergency intelligence
TASK_QUEUE_NORMAL = "sentinel:agents:tasks:normal"  # Standard research
TASK_QUEUE_LOW    = "sentinel:agents:tasks:low"     # Background maintenance

HEARTBEAT_INTERVAL = 30  # Seconds between agent heartbeats for liveness monitoring

class InferenceError(Exception):
    """Raised when Ollama inference fails after all retries."""
    pass
 
 
class SchemaViolationError(Exception):
    """Raised when LLM output cannot be coerced to the required schema."""
    pass

class OllamaClient:
    """
    Async HTTP client for local Ollama inference.
 
    JSON Schema Enforcement:
      Llama3 sometimes outputs markdown-wrapped JSON or prose before the object.
      We use a 3-attempt retry loop with progressively more explicit prompting.
      On all failures, we log and raise SchemaViolationError so the agent can
      route to DLQ rather than silently producing garbage data.
    """

    def __init__(self, session: aiohttp.ClientSession, model: str = OLLAMA_MODEL):
        self.session = session
        self.model = model

    async def infer(
        self,
        system_prompt: str,
        user_prompt: str,
        schema: Type[BaseModel],
        temperature: float = 0.1,
        max_retries: int = 3
    ) -> BaseModel:
        """
        Call Ollama and parse response into a Pydantic schema instance.
        Retries on schema violations with an increasingly explicit correction prompt.
        """
        last_error = Optional[str] = None

        for attempt in range(max_retries):
            correction_suffix = ""
            if attempt > 0:
                # Progressive correction: be more explicit on retry
                correction_suffix = (
                    f"\n\n⚠️ CORRECTION REQUIRED: Your previous response failed validation: {last_error}\n"
                    "You MUST return ONLY a raw JSON object. No markdown. No explanation. No ```json fences.\n"
                    "Start your response with {{ and end with }}"
                )
            
            full_prompt = f"{system_prompt}\n\n{user_prompt}{correction_suffix}"

            async with _OLLAMA_SEMAPHORE:
                raw_text = await self._call_ollama(full_prompt, temperature)

            parsed = self._extract_json(raw_text)
            if parsed is None:
                last_error = f"No valid JSON found in response {raw_text[:50]}"
                logger.warning(f"OLLAMA ATTEMPT: {attempt+1} NO JSON EXTRACTED")
                continue

            try:
                return schema(**parsed)
            except (ValidationError, TypeError) as e:
                last_error = str(e)[:300]
                logger.warning(f"OLLAMA ATTEMPT: {attempt+1} VAL/SCHENA ERROR: {last_error}")

        raise SchemaViolationError(
            f"Schema enforcement failed after {max_retries} attempts. Last error: {last_error}"
        )
    
    async def infer_raw(
            self,
            system_prompt: str,
            user_prompt: str,
            temperature: float = 0.1,
    ):
        """Inference without schema enforcement. Use for open-ended research tasks."""
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        async with _OLLAMA_SEMAPHORE:
            return await self._call_ollama(full_prompt, temperature)
        
    async def _call_ollama(self, prompt: str, temperature: float) -> str:
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": 2048,
                "stop": ["</json, Human:", "User:"],

            },

        }
        try:
            async with self.session.post(
                f"{OLLAMA_URL}/api/generate", 
                json=payload,
                timeout=OLLAMA_TIMEOUT,
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise InferenceError(f"Ollama HTTP {resp.status}: {text}")
                    data = await resp.json()

