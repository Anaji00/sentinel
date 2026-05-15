import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
# BEST PRACTICE: Dynamically adding the project root to sys.path ensures 
# that absolute imports (like 'from shared.db import...') work consistently from anywhere.
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


# ── TOPIC CONSTANTS ───────────────────────────────────────────────────────────
# New topics added by the agent swarm (add to shared/kafka/__init__.py Topics class)
TOPIC_ENRICHED_EVENTS    = "enriched.events"
TOPIC_INTEL_BRIEFS       = "agents.intel.briefs"
TOPIC_QUANT_DISCOVERIES  = "agents.quant.discoveries"
TOPIC_ONTOLOGY_UPDATES   = "agents.ontology.updates"
TOPIC_UNKNOWN_ENTITIES   = "agents.ontology.unknown_entities"
TOPIC_DLQ                = "dead.letter"


# ── TASK QUEUE WORKER ────────────────────────────────────────────────────────

async def run_task_queue_worker(redis_client, agents: dict):
    """
    Background worker that drains the Redis priority task queue.

    Task format (JSON in Redis list):
      {
        "task_id":   "uuid",
        "task_type": "classify_entity" | "research_ticker" | "prune_ontology",
        "agent":     "ontology_master" | "quant_researcher",
        "payload":   {...},
        "created_at": "iso timestamp"
      }

    Priority order: HIGH → NORMAL → LOW (blocking BLPOP with 1s timeout)
    """
    from services.agents.base import TASK_QUEUE_HIGH, TASK_QUEUE_NORMAL, TASK_QUEUE_LOW
    import json
    
    queues = [TASK_QUEUE_HIGH, TASK_QUEUE_NORMAL, TASK_QUEUE_LOW]
    logger.info("Task queue worker started")

    while True:
        try:
            # BLPOP blocks for 1s
            # CONCEPT: Event Loop Non-Blocking IO
            # Since Redis `blpop` is synchronous and blocking, running it directly would freeze the entire asyncio event loop.
            # `run_in_executor(None, ...)` safely offloads this blocking call to a background thread pool.
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: redis_client.raw.blpop(queues, timeout=1),
            )

            if not result:
                continue

            _, raw_task = result
            task = json.loads(raw_task)
            task_type = task.get("task_type")
            agent_name = task.get("agent")
            payload = task.get("payload", {})
            
            logger.debug(f"Task dequeued: {task_type} → {agent_name}")

            agent = agents.get(agent_name)
            if agent and hasattr(agent, "handle"):
                # FIRE AND FORGET: create_task schedules the coroutine to run in the background.
                # We don't `await` it here because we want the worker loop to immediately 
                # go back to listening for new tasks from Redis without waiting for the handler to finish.
                asyncio.create_task(agent.handle(payload))
            else:
                logger.warning(f"Unknown agent in task queue: {agent_name}")
        
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Task queue worker error: {e}", exc_info=True)
            await asyncio.sleep(1)
# ── AGENT FACTORY ─────────────────────────────────────────────────────────────

def build_agent(
    AgentClass, 
    agent_name:    str,
    input_topics:  list,
    group_id:      str,
    shared_infra:  dict,
) -> object:
    """
    Factory function: instantiates an agent with all shared infrastructure.
    Separates object construction from the async runtime.
    """
    # CONCEPT: Dependency Injection
    # Instead of each agent creating its own database connections, we pass them in from the outside.
    # This saves memory (via connection pooling) and makes testing easier because
    # we can effortlessly pass mock databases into this factory during unit tests.
    redis   = shared_infra["redis"]
    db      = shared_infra["db"]
    neo4j   = shared_infra["neo4j"]

    # Each agent gets its own Kafka consumer (separate group_id → independent offsets)
    # and its own Kafka producer. Shared producers would require locking.

    consumer = SentinelConsumer(
        topics=input_topics,
        group_id=group_id,
        auto_offset_reset="latest",
    )

    producer = SentinelProducer()
    dlq      = SentinelProducer()

    return AgentClass(
        agent_name=agent_name,
        input_topics=input_topics,
        redis_client=redis,
        db_client=db,
        neo4j_client=neo4j,
        kafka_producer=producer,
        kafka_consumer=consumer,
        dlq_producer=dlq,
        model=os.getenv("AGENT_MODEL", "llama3"),
    )

# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AGENT SWARM INITIALIZING")
    logger.info("=" * 60)

    # ── SHARED INFRASTRUCTURE ──────────────────────────────────────────────────
    # All agents share DB connections (connection-pooled — thread safe).
    # They do NOT share Kafka producers/consumers (not thread safe).
    shared_infra = {
        "redis": get_redis(),
        "db":    get_timescale(),
        "neo4j": get_neo4j(),
    }
    logger.info("Shared infrastructure connected")

    # ── AGENT INSTANTIATION ────────────────────────────────────────────────────
    news_agent = build_agent(
        NewsIntelAgent,
        agent_name="news_intel",
        input_topics=[TOPIC_ENRICHED_EVENTS],
        group_id="agent-news-intel",
        shared_infra=shared_infra,
    )

    quant_agent = build_agent(
        QuantResearcherAgent,
        agent_name="quant_researcher",
        input_topics=[TOPIC_ENRICHED_EVENTS],
        group_id="agent-quant-researcher",
        shared_infra=shared_infra,
    )

    ontology_agent = build_agent(
        OntologyMasterAgent,
        agent_name="ontology_master",
        input_topics=[TOPIC_UNKNOWN_ENTITIES],
        group_id="agent-ontology-master",
        shared_infra=shared_infra,
    )

    agents_by_name = {
        "news_intel":      news_agent,
        "quant_researcher": quant_agent,
        "ontology_master": ontology_agent,
    }

    logger.info(f"Agents built: {list(agents_by_name.keys())}")
    logger.info(f"Ollama model: {os.getenv('AGENT_MODEL', 'llama3')}")
    logger.info("=" * 60)

    # ── LAUNCH ALL AGENTS + TASK QUEUE WORKER ─────────────────────────────────
    tasks = [
        asyncio.create_task(news_agent.run(),      name="news_intel"),
        asyncio.create_task(quant_agent.run(),     name="quant_researcher"),
        asyncio.create_task(ontology_agent.run(),  name="ontology_master"),
        asyncio.create_task(
            run_task_queue_worker(shared_infra["redis"], agents_by_name),
            name="task_queue_worker",
        ),
    ]

    logger.info("All agents launched. Swarm is LIVE.")

    try:
        # Wait for all tasks. If any crashes, re-raise to restart via supervisor.
        # CONCEPT: asyncio.gather runs multiple asynchronous operations concurrently.
        # If any of these agent tasks raise an unhandled exception, gather will immediately throw it here.
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("SIGINT received — graceful shutdown")
    except Exception as e:
        logger.error(f"Agent swarm error: {e}", exc_info=True)
        raise
    finally:
        # CLEANUP: Always ensure background tasks are gracefully canceled before the script exits.
        for task in tasks:
            task.cancel()
        # Give tasks 5s to clean up
        # return_exceptions=True prevents CancelledError from bubbling up and interrupting the shutdown sequence.
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Agent swarm shut down cleanly")


if __name__ == "__main__":
    # OS FIX: The default ProactorEventLoop on Windows sometimes struggles with certain sockets/subprocesses.
    # Forcing the WindowsSelectorEventLoopPolicy ensures maximum compatibility and stability on Windows machines.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())