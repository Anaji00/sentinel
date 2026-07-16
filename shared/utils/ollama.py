"""
shared/utils/ollama.py
 
SENTINEL SHARED OLLAMA CLIENT
==============================
Single, shared HTTP client for all local Llama3 inference across the platform.
 
Extracted here from services/agents/base.py so both the agent swarm and the
reasoning service share the same client implementation without duplication.
 
The global semaphore (_OLLAMA_SEMAPHORE) is per-process:
  - agents/main.py process: one semaphore shared across all three agents
  - reasoning/main.py process: its own semaphore
 
This is correct. Ollama queues concurrent cross-process requests internally.
The per-process semaphore prevents the same process from firing parallel calls,
which degrades output quality on a single GPU. Cross-process contention is
handled by Ollama's own request queue.
 
Usage:
    from shared.utils.ollama import OllamaClient, get_ollama_semaphore
 
    async with aiohttp.ClientSession() as session:
        client = OllamaClient(session)
        result = await client.infer(
            system_prompt=MY_SYSTEM,
            user_prompt=MY_USER,
            schema=MyPydanticModel,
        )
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
OLLAMA_TIMEOUT = aiohttp.ClientTimeout(total=float(os.getenv("OLLAMA_TIMEOUT", "1200.0")))  # Scenario synthesis can be verbose

# Per-process semaphore. Import this and use it as a context manager in any
# code that calls Ollama to prevent parallel requests within one process.
_OLLAMA_SEMAPHORE = asyncio.Semaphore(1)
 

def get_ollama_semaphore():
    global _OLLAMA_SEMAPHORE
    if _OLLAMA_SEMAPHORE is None:
        _OLLAMA_SEMAPHORE = asyncio.Semaphore(1)
    return _OLLAMA_SEMAPHORE


# Custom Exception classes to handle specific failure types cleanly.
class InferenceError(Exception):
    """Ollama HTTP or network failure after retries."""

class SchemaViolationError(Exception):
    """LLM output could not be coerced to the required Pydantic schema."""

class OllamaClient:
    """
    Async Ollama inference client with Pydantic schema enforcement.
 
    Schema enforcement strategy:
      1. Call Ollama at low temperature (0.05-0.15 for structured tasks).
      2. Extract JSON from response using four fallback strategies.
      3. Validate against Pydantic schema.
      4. On failure, retry up to max_retries with an explicit correction suffix
         that tells the model exactly what went wrong and demands raw JSON.
      5. After all retries, raise SchemaViolationError for DLQ routing.
 
    The correction suffix approach outperforms simply retrying the same prompt
    because it gives the model concrete error feedback rather than letting it
    repeat the same mistake.
    """
    def __init__(
        self,
        session: aiohttp.ClientSession,
        model: str = OLLAMA_MODEL,
    ):
        # We reuse the same aiohttp session for all requests. 
        # Reusing sessions (connection pooling) is much faster than opening 
        # a new TCP/IP connection for every single HTTP request.
        self._session = session
        self.model = model

    async def infer(
        self, 
        system_prompt: str,
        user_prompt: str,
        schema: Type[BaseModel],
        temperature: float = 0.1,
        max_retries: int = 3,
    ) -> BaseModel:
        """
        Run inference and validate output against a Pydantic schema.
        Returns a validated schema instance.
        Raises SchemaViolationError after max_retries.
        """

        last_error: Optional[str] = None
        semaphore = get_ollama_semaphore()

        schema_json = json.dumps(schema.model_json_schema() if hasattr(schema, "model_json_schema") else schema.schema(), indent=2)
        schema_instruction = (
            f"\n\nYou MUST return a JSON object that strictly adheres to the following JSON schema:\n"
            f"```json\n{schema_json}\n```\n"
            "Do not include any explanation or markdown formatting outside of the JSON object. Output raw JSON only.\n"
            "CRITICAL: Output the actual data values that fit this schema, DO NOT output the schema definition itself."
        )

        # Retry Loop: Give the AI multiple chances to fix its mistakes.
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

            # Wait in line until the GPU is free (using the semaphore we defined earlier)
            async with semaphore:
                raw_text = await self._call_ollama(full_prompt, temperature, format="json")

            # Try to pull the JSON out of the AI's raw text response
            parsed = self._extract_json(raw_text)
            if parsed is None:
                last_error = f"No valid JSON found in: {raw_text[:300]}"
                logger.warning(f"Ollama attempt {attempt+1}: no JSON — {last_error[:100]}")
                continue

            try:
                # Pydantic Magic: Unpack the dictionary (`**parsed`) into the strictly typed Pydantic schema.
                # If keys are missing or data types are wrong (e.g., got a string instead of an int),
                # Pydantic will raise a `ValidationError`, kicking us into the `except` block to retry.
                return schema(**parsed)
            except ValidationError as e:
                last_error = str(e)
                logger.warning(f"Ollama attempt {attempt+1}: invalid JSON — {last_error[:100]}")

            
        raise SchemaViolationError(
            f"Schema enforcement failed after {max_retries} attempts. Last: {last_error}"
        )
    
    async def infer_raw(
        self, 
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> str:
        semaphore = get_ollama_semaphore()
        """Raw inference without schema enforcement."""
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        async with semaphore:
            return await self._call_ollama(full_prompt, temperature)
        
    
    async def _call_ollama(self, prompt: str, temperature: float, format: Optional[str] = None) -> str:
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "keep_alive": -1,  # Keep model permanently loaded in GPU memory to prevent slow reload spikes
            "options": {
                "temperature": temperature,
                "num_predict": 1000, # Cap generation length to prevent runaway slow text generation
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
                return data.get("response", "")
            
        except asyncio.TimeoutError:
            raise InferenceError(f"Ollama timed out after {OLLAMA_TIMEOUT.total}s")
        except aiohttp.ClientError as e:
            raise InferenceError(f"Ollama connection error: {e}")
        


    @staticmethod
    def _extract_json(text: str) -> Optional[Dict]:
        """
        Robustly extract a JSON object from LLM output.
 
        Four strategies in order:
          1. Direct parse (ideal — model returned clean JSON)
          2. Strip markdown code fences then parse
          3. Find first {...} block via regex
          4. Find first [...] block via regex (for list outputs)
        """
        text = text.strip()

        # Strategy 1: direct parse
        # Sometimes the AI behaves perfectly and gives us clean JSON strings.
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass


        # Strategy 2: strip fences
        # AI loves to wrap code in markdown formatting like: ```json { "key": "val" } ```
        # Let's break down this regex: r"```(?:json)?"
        # 1. ` ``` ` -> Matches three literal backticks.
        # 2. `(?:json)` -> The `(?: ... )` means a "non-capturing group". It looks for the word "json"
        #                 but doesn't save it to memory (making it slightly faster than standard `(...)`).
        # 3. `?` -> Makes the previous group (`(?:json)`) OPTIONAL.
        # So this matches BOTH "```" and "```json".
        stripped = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

        # Strategy 3: extract first { } block
        # If the AI included conversational text like "Here is your data: { ... } Enjoy!",
        # we use regex to grab just the object part.
        # Let's break down this regex: r"\{.*\}"
        # 1. `\{` -> Matches a literal opening curly brace (we have to escape it with `\` because `{` is a regex special character).
        # 2. `.` -> Matches ANY character.
        # 3. `*` -> Means "match the previous character zero or more times". So `.*` means "match a bunch of anything".
        # 4. `\}` -> Matches a literal closing curly brace.
        # 
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                # match.group(0) returns the exact string that was successfully captured by our regex
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        # Strategy 4: extract first [ ] block (for list-wrapped objects)
        # Sometimes the AI returns an array wrapping a single object: `[ { "key": "val" } ]`
        # Let's break down this regex: r"\[.*\]"
        # 1. `\[` -> Matches a literal opening bracket.
        # 2. `.*` -> Matches any characters in between.
        # 3. `\]` -> Matches a literal closing bracket.
        # Note: We use `re.DOTALL` here for the same multiline reasons.
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group(0))
                # If it's a list with one dict, unwrap it
                if isinstance(result, list) and len(result) == 1 and isinstance(result[0], dict):
                    return result[0]
            except json.JSONDecodeError:
                pass
 
        return None