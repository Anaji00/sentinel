import asyncio
import logging
import os
import sys
import signal
import aiohttp
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
logger = logging.getLogger("agents.main")

from shared.db import get_timescale, get_neo4j, get_redis
from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.utils.ollama import OllamaClient
from services.agents.radar_agent import RadarAgent
from services.correlation.soft_correlator import SoftCorrelator
from services.agents.news_intel import NewsIntelAgent
from services.agents.quant_researcher import QuantResearcherAgent
from services.agents.ontology_master import OntologyMasterAgent
from services.agents.macro_cointegration_engine import MacroAssetCointegrationEngine
from services.agents.supervisor import GraphSupervisor
from services.agents.macro_strategist import MacroStrategistAgent
from services.agents.rule_agent import RuleSynthesizerAgent
# ── TOPIC CONSTANTS ───────────────────────────────────────────────────────────
# New topics added by the agent swarm (add to shared/kafka/__init__.py Topics class)
TOPIC_ENRICHED_EVENTS    = "enriched.events"
TOPIC_INTEL_BRIEFS       = "agents.intel.briefs"
TOPIC_QUANT_DISCOVERIES  = "agents.quant.discoveries"
TOPIC_ONTOLOGY_UPDATES   = "agents.ontology.updates"
TOPIC_UNKNOWN_ENTITIES   = "agents.ontology.unknown_entities"
TOPIC_DLQ                = "dead.letter"
TOPIC_RAW_RADAR          = "events.raw.radar"


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
            result = await redis_client.raw.blpop(queues, timeout=1.0)

            if not result:
                continue

            _, raw_task = result
            task = json.loads(raw_task)
            task_type = task.get("task_type")
            agent_name = task.get("agent")
            payload = task.get("payload", {})
            
            logger.debug(f"Task dequeued: {task_type} → {agent_name}")

            agent = agents.get(agent_name)
            if agent and hasattr(agent, "_dispatch"):
                # FIRE AND FORGET: create_task schedules the coroutine to run in the background.
                # We don't `await` it here because we want the worker loop to immediately 
                # go back to listening for new tasks from Redis without waiting for the handler to finish.
                asyncio.create_task(agent._dispatch(payload))
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
    **extra_kwargs
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
        producer=producer,
        consumer=consumer,
        dlq=dlq,
        model=os.getenv("AGENT_MODEL", "llama3"),
        **extra_kwargs
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
        "redis": await get_redis(),
        "db":    await get_timescale(),
        "neo4j": await get_neo4j(),
    }
    logger.info("Shared infrastructure connected")

    connector = aiohttp.TCPConnector(limit=20)
    main_session = aiohttp.ClientSession(connector=connector)
    ollama_client = OllamaClient(main_session)
    
    soft_correlator = SoftCorrelator(ollama_client)
    # Load soft correlator in the background to prevent blocking service startup
    asyncio.create_task(soft_correlator._load())
    # ── AGENT INSTANTIATION ────────────────────────────────────────────────────
    news_agent = build_agent(
        NewsIntelAgent,
        agent_name="news_intel",
        input_topics=[TOPIC_ENRICHED_EVENTS, TOPIC_QUANT_DISCOVERIES, TOPIC_RAW_RADAR],
        group_id="agent-news-intel",
        shared_infra=shared_infra,
    )

    quant_agent = build_agent(
        QuantResearcherAgent,
        agent_name="quant_researcher",
        input_topics=[TOPIC_ENRICHED_EVENTS, TOPIC_RAW_RADAR, TOPIC_ONTOLOGY_UPDATES],
        group_id="agent-quant-researcher",
        shared_infra=shared_infra,
    )

    ontology_agent = build_agent(
        OntologyMasterAgent,
        agent_name="ontology_master",
        input_topics=[TOPIC_UNKNOWN_ENTITIES, TOPIC_ENRICHED_EVENTS],
        group_id="agent-ontology-master",
        shared_infra=shared_infra,
        soft_correlator=soft_correlator,
    )

    radar_agent = build_agent(
        RadarAgent,
        agent_name="radar_agent",
        input_topics=[TOPIC_RAW_RADAR, TOPIC_QUANT_DISCOVERIES, TOPIC_ENRICHED_EVENTS],
        group_id="agent-radar-orchestrator",
        shared_infra=shared_infra,
    )

    macro_cointegration_agent = build_agent(
        MacroAssetCointegrationEngine,
        agent_name="macro_cointegration_engine",
        input_topics=[Topics.RAW_TRADFI, "raw.macro"],
        group_id="agent-macro-cointegration-engine",
        shared_infra=shared_infra,
    )

    supervisor_agent = build_agent(
        GraphSupervisor,
        agent_name="supervisor",
        input_topics=["sentinel.ontology.proposals"],
        group_id="supervisor-group",
        shared_infra=shared_infra,
    )

    macro_strategist_agent = build_agent(
        MacroStrategistAgent,
        agent_name="macro_strategist",
        input_topics=[TOPIC_ENRICHED_EVENTS, TOPIC_QUANT_DISCOVERIES, TOPIC_ONTOLOGY_UPDATES, TOPIC_UNKNOWN_ENTITIES],
        group_id="agent-macro-strategist",
        shared_infra=shared_infra,
    )

    rule_synthesizer_agent = build_agent(
        RuleSynthesizerAgent,
        agent_name="rule_synthesizer",
        input_topics=["agents.intel.briefs"],
        group_id="agent-rule-synthesizer",
        shared_infra=shared_infra,
    )

    agents_by_name = {
        "news_intel":      news_agent,
        "quant_researcher": quant_agent,
        "ontology_master": ontology_agent,
        "radar_agent": radar_agent,
        "macro_cointegration_engine": macro_cointegration_agent,
        "graph_supervisor": supervisor_agent,
        "macro_strategist": macro_strategist_agent,
        "rule_synthesizer": rule_synthesizer_agent,
    }

    logger.info(f"Agents built: {list(agents_by_name.keys())}")
    logger.info(f"Ollama model: {os.getenv('AGENT_MODEL', 'llama3')}")
    logger.info("=" * 60)

    # ── LAUNCH ALL AGENTS + TASK QUEUE WORKER ─────────────────────────────────
    tasks = [
        asyncio.create_task(news_agent.run(),      name="news_intel"),
        asyncio.create_task(quant_agent.run(),     name="quant_researcher"),
        asyncio.create_task(ontology_agent.run(),  name="ontology_master"),
        asyncio.create_task(radar_agent.run(),     name="radar_agent"),
        asyncio.create_task(macro_cointegration_agent.run(), name="macro_cointegration_engine"),
        asyncio.create_task(supervisor_agent.run(), name="graph_supervisor"),
        asyncio.create_task(macro_strategist_agent.run(), name="macro_strategist"),
        asyncio.create_task(rule_synthesizer_agent.run(), name="rule_synthesizer"),
        asyncio.create_task(
            run_task_queue_worker(shared_infra["redis"], agents_by_name),
            name="task_queue_worker",
        ),
    ]

    logger.info("All agents launched. Swarm is LIVE.")
    logger.info(f"Agent: {news_agent.name} | Topics: {len(news_agent.input_topics)}")
    logger.info(f"Agent: {quant_agent.name} | Topics: {len(quant_agent.input_topics)}")
    logger.info(f"Agent: {ontology_agent.name} | Topics: {len(ontology_agent.input_topics)}")
    logger.info(f"Agent: {radar_agent.name} | Topics: {len(radar_agent.input_topics)}")
    logger.info(f"Agent: {macro_cointegration_agent.name} | Topics: {len(macro_cointegration_agent.input_topics)}")
    logger.info(f"Agent: {supervisor_agent.name} | Topics: {len(supervisor_agent.input_topics)}")
    logger.info(f"Agent: {macro_strategist_agent.name} | Topics: {len(macro_strategist_agent.input_topics)}")
    logger.info(f"Agent: {rule_synthesizer_agent.name} | Topics: {len(rule_synthesizer_agent.input_topics)}")

    try:
        def handle_sigterm(signum, frame):
            logger.info("SIGTERM received — initiating graceful shutdown")
            raise KeyboardInterrupt()
            
        if sys.platform != "win32":
            signal.signal(signal.SIGTERM, handle_sigterm)

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
        if not main_session.closed:
            await main_session.close()
        logger.info("Agent swarm shut down cleanly")


if __name__ == "__main__":
    # OS FIX: The default ProactorEventLoop on Windows sometimes struggles with certain sockets/subprocesses.
    # Forcing the WindowsSelectorEventLoopPolicy ensures maximum compatibility and stability on Windows machines.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())