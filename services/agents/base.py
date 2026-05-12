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

    def __init(self)