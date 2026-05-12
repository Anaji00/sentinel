"""
services/agents/main.py

SENTINEL AGENT SWARM ORCHESTRATOR
===================================
Launches all three autonomous agents as concurrent asyncio tasks.
Each agent runs its own Kafka consumer loop in a thread executor,
dispatching work as async tasks on the shared event loop.

Orchestration architecture:
  - Kafka:        Primary inter-agent bus (durable, replayable, decoupled)
  - Redis Queue:  Priority task queue for long-running research (no Celery needed)
  - asyncio:      Single event loop manages all agents + background workers
  - Semaphore:    Shared across all agents via base.py (_OLLAMA_SEMAPHORE)
                  One Ollama request at a time = consistent throughput

Why not Celery?
  Celery adds: a broker config, result backend, worker processes, monitoring daemon.
  We already have Redis. RPUSH/BLPOP gives us priority queuing with zero extra deps.
  For LLM inference tasks that take 10-30s, asyncio.create_task() inside each
  agent's dispatch loop provides adequate concurrency without the operational burden.

Why not separate processes per agent?
  These agents share the Ollama semaphore — if they were separate processes,
  we'd need a distributed semaphore (Redis-based Redlock) to prevent parallel
  GPU saturation. Single process = single semaphore = simpler, more reliable.

Shutdown:
  SIGINT/SIGTERM → cancel all tasks → flush Kafka producers → exit cleanly.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("agents.orchestrator")

from shared.db import get_timescale, get_neo4j, get_redis
from shared.kafka import SentinelProducer, SentinelConsumer

from services.agents.news_intel import NewsIntelAgent
from services.agents.quant_researcher import QuantResearcherAgent
from services.agents.ontology_master import OntologyMasterAgent