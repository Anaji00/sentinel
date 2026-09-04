import asyncio
import logging
import os
import sys
import signal
import aiohttp
from typing import Optional, Dict, List, Any
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

import warnings
from shared.utils.logging import setup_sentinel_logging

logger = setup_sentinel_logging("agents.main", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

from shared.db import get_timescale, get_neo4j, get_redis
from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.utils.ollama import DEFAULT_MODEL, OllamaClient
from services.agents.macro_intelligence_engine import MacroIntelligenceEngine
from services.agents.quant_trading_engine import QuantTradingEngine
from services.agents.knowledge_graph_engine import KnowledgeGraphEngine
from services.agents.radar_agent import RadarAgent
from services.agents.rule_agent import RuleSynthesizerAgent
from services.agents.supervisor import GraphSupervisor
from services.agents.consensus_engine import ConsensusEngine
from services.agents.adversarial_wargamer import AdversarialWargamerAgent
from services.agents.edge_validator import EdgeValidatorAgent
from services.agents.stock_correlation_agent import StockCorrelationAgent
from services.correlation.soft_correlator import SoftCorrelator
from shared.utils.tasks import safe_create_task
from shared.utils.heartbeat import start_heartbeat_task, touch_heartbeat
# ── TOPIC CONSTANTS ───────────────────────────────────────────────────────────
# All topics are now centrally managed in shared/kafka/__init__.py


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
                safe_create_task(agent._dispatch(payload), name=f"agent-dispatch-{agent_name}")
            else:
                logger.warning(f"Unknown agent in task queue: {agent_name}")
        
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Task queue worker error: {e}", exc_info=True)
            await asyncio.sleep(1)
# ── AGENT FACTORY ─────────────────────────────────────────────────────────────

# The heavy agents asked for qwen2.5:7b by name. That image is 4.7 GB and the
# ollama container is capped at 4 GB with OLLAMA_MAX_LOADED_MODELS=2, so it could
# never load: every call walked the retry-and-fallback ladder before landing on a
# small model anyway, paying the full timeout each time.
#
# AGENT_MODEL already carries the right choice per tier -- the compose file sets
# qwen2.5:3b for agents-heavy and qwen2.5:1.5b for agents-fast -- so the tier
# decides, not a literal buried in this file.
HEAVY_MODEL = os.getenv("AGENT_MODEL", "qwen2.5:3b")


def build_agent(
    AgentClass, 
    agent_name:    str,
    input_topics:  list,
    group_id:      str,
    shared_infra:  dict,
    model:         Optional[str] = None,
    fallback_model: Optional[str] = None,
    **extra_kwargs
) -> object:
    """
    Factory function: instantiates an agent with all shared infrastructure and per-agent model configurations.
    """
    redis   = shared_infra["redis"]
    db      = shared_infra["db"]
    neo4j   = shared_infra["neo4j"]

    consumer = SentinelConsumer(
        topics=input_topics,
        group_id=group_id,
        auto_offset_reset="latest",
    )

    producer = SentinelProducer()
    dlq      = SentinelProducer()

    # Per-agent environment variable resolution with tiered defaults
    env_name = agent_name.upper().replace("-", "_")
    selected_model = model or os.getenv(f"{env_name}_MODEL", os.getenv("AGENT_MODEL", DEFAULT_MODEL))
    selected_fallback = fallback_model or os.getenv(f"{env_name}_FALLBACK_MODEL", os.getenv("OLLAMA_FALLBACK_MODEL", "gemma:2b"))

    return AgentClass(
        agent_name=agent_name,
        input_topics=input_topics,
        redis_client=redis,
        db_client=db,
        neo4j_client=neo4j,
        producer=producer,
        consumer=consumer,
        dlq=dlq,
        model=selected_model,
        fallback_model=selected_fallback,
        **extra_kwargs
    )

# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AGENT SWARM INITIALIZING")
    logger.info("=" * 60)

    shared_infra = {
        "redis": await get_redis(),
        "db":    await get_timescale(),
        "neo4j": await get_neo4j(),
    }
    logger.info("Shared infrastructure connected")

    # Publish this process's metrics, so inference can be accounted for.
    #
    # MetricsCollector.increment("ollama_calls_total") has run in this service
    # since it was written, and never left the process: bind_redis() had exactly
    # one caller, a collector-specific helper, so only the collectors ever
    # published. The module's own docstring describes cross-process aggregation
    # as the problem it solves, and the services doing all the inference were
    # not participating in it.
    #
    # The cost was not a missing dashboard. It made "how much model time does
    # each agent consume" unanswerable from inside, which left parsing Ollama's
    # access log by container IP as the only option -- and Docker reassigns
    # those on restart, so the attribution was wrong in a way that took two
    # corrections to notice.
    try:
        from shared.utils.metrics import bind_redis
        await bind_redis(shared_infra["redis"], service_name=os.getenv("SENTINEL_SERVICE", "agents"))
    except Exception as e:
        logger.debug("Metrics binding skipped: %s", e)

    from shared.utils.ollama import OllamaClient, OLLAMA_TIMEOUT

    connector = aiohttp.TCPConnector(limit=20)
    main_session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
    ollama_client = OllamaClient(main_session, redis_client=shared_infra["redis"])
    
    soft_correlator = SoftCorrelator(ollama_client)
    safe_create_task(soft_correlator._load(), name="soft-correlator-load")

    # ── TIERED PER-AGENT MODEL ALLOCATION ───────────────────────────────────────
    # Heavy Analytical Tier: Deep reasoning (qwen2.5:7b -> qwen2.5:1.5b fallback)
    # Fast Operational Tier: Ultra-fast routing/classification (qwen2.5:1.5b -> gemma3:1b fallback)

    # ── CONSOLIDATED 5 CORE ENGINES ──────────────────────────────────────────

    macro_intelligence_engine = build_agent(
        MacroIntelligenceEngine,
        agent_name="macro_intelligence_engine",
        input_topics=[
            Topics.RAW_NEWS, Topics.RAW_TRADFI, Topics.RAW_CRYPTO, Topics.ENRICHED_EVENTS,
            Topics.CORRELATIONS, Topics.QUANT_DISCOVERIES,
            Topics.INSIDER_CLUSTERS
        ],
        group_id="agent-macro-intelligence",
        shared_infra=shared_infra,
        model=HEAVY_MODEL,
        fallback_model="qwen2.5:1.5b",
    )

    quant_trading_engine = build_agent(
        QuantTradingEngine,
        agent_name="quant_trading_engine",
        input_topics=[
            Topics.RAW_NEWS, Topics.RAW_TRADFI, Topics.ENRICHED_EVENTS, Topics.SCENARIOS_GENERATED,
            Topics.CORRELATIONS, Topics.INTEL_BRIEFS, Topics.MACRO_DECOUPLING,
            Topics.QUANT_DISCOVERIES, Topics.MACRO_ASSESSMENT, Topics.RADAR_DECISIONS
        ],
        group_id="agent-quant-trading",
        shared_infra=shared_infra,
        model=HEAVY_MODEL,
        fallback_model="qwen2.5:1.5b",
    )

    knowledge_graph_engine = build_agent(
        KnowledgeGraphEngine,
        agent_name="knowledge_graph_engine",
        input_topics=[
            Topics.RAW_NEWS, Topics.ENRICHED_EVENTS, Topics.UNKNOWN_ENTITIES,
            Topics.CORRELATIONS, Topics.ONTOLOGY_PROPOSALS, Topics.QUANT_DISCOVERIES,
            Topics.MACRO_ASSESSMENT, Topics.SCENARIOS_GENERATED
        ],
        group_id="agent-knowledge-graph",
        shared_infra=shared_infra,
        model="qwen2.5:1.5b",
        fallback_model="gemma3:1b",
    )

    radar_agent = build_agent(
        RadarAgent,
        agent_name="radar_agent",
        input_topics=[
            Topics.RAW_NEWS, Topics.RAW_RADAR, Topics.ENRICHED_EVENTS, Topics.CORRELATIONS,
            Topics.QUANT_DISCOVERIES, Topics.MACRO_ASSESSMENT
        ],
        group_id="agent-radar-orchestrator",
        shared_infra=shared_infra,
        model="qwen2.5:1.5b",
        fallback_model="gemma3:1b",
    )

    rule_synthesizer_agent = build_agent(
        RuleSynthesizerAgent,
        agent_name="rule_synthesizer",
        input_topics=[
            Topics.RAW_NEWS, Topics.INTEL_BRIEFS, Topics.RULES_FEEDBACK, Topics.SCENARIOS_GENERATED,
            Topics.CORRELATIONS, Topics.QUANT_DISCOVERIES, Topics.MACRO_DECOUPLING,
            Topics.MACRO_ASSESSMENT, Topics.INSIDER_CLUSTERS
        ],
        group_id="agent-rule-synthesizer",
        shared_infra=shared_infra,
        model="qwen2.5:1.5b",
        fallback_model="gemma3:1b",
    )

    supervisor_agent = build_agent(
        GraphSupervisor,
        agent_name="supervisor",
        input_topics=[
            Topics.RAW_NEWS, Topics.ONTOLOGY_PROPOSALS, Topics.CORRELATIONS, Topics.INTEL_BRIEFS,
            Topics.SCENARIOS_GENERATED
        ],
        group_id="supervisor-group",
        shared_infra=shared_infra,
        model=HEAVY_MODEL,
        fallback_model="qwen2.5:1.5b",
    )

    consensus_engine = build_agent(
        ConsensusEngine,
        agent_name="consensus_engine",
        input_topics=[
            Topics.RAW_NEWS, Topics.INTEL_BRIEFS, Topics.QUANT_DISCOVERIES, Topics.FINANCIAL_ADVICE,
            Topics.RULES_FEEDBACK, Topics.RULES_SYNTHESIZED,
            Topics.MACRO_ASSESSMENT, Topics.CORRELATIONS, Topics.INSIDER_CLUSTERS
        ],
        group_id="agent-consensus-engine",
        shared_infra=shared_infra,
        model="qwen2.5:1.5b",
        fallback_model="gemma3:1b",
    )

    adversarial_wargamer = build_agent(
        AdversarialWargamerAgent,
        agent_name="adversarial_wargamer",
        input_topics=[
            Topics.RAW_NEWS, Topics.CORRELATIONS, Topics.INTEL_BRIEFS, Topics.SCENARIOS_GENERATED
        ],
        group_id="agent-adversarial-wargamer",
        shared_infra=shared_infra,
        model=HEAVY_MODEL,
        fallback_model="qwen2.5:1.5b",
    )

    edge_validator_agent = build_agent(
        EdgeValidatorAgent,
        agent_name="edge_validator",
        input_topics=[
            Topics.RAW_NEWS, Topics.QUANT_DISCOVERIES, Topics.CORRELATIONS, Topics.INTEL_BRIEFS
        ],
        group_id="agent-edge-validator",
        shared_infra=shared_infra,
        model="qwen2.5:1.5b",
        fallback_model="gemma3:1b",
    )

    stock_correlation_agent = build_agent(
        StockCorrelationAgent,
        agent_name="stock_correlation_agent",
        input_topics=[
            Topics.RAW_NEWS, Topics.ENRICHED_EVENTS, Topics.QUANT_DISCOVERIES, Topics.MACRO_ASSESSMENT, Topics.CORRELATIONS
        ],
        group_id="agent-stock-correlation",
        shared_infra=shared_infra,
        model="qwen2.5:1.5b",
        fallback_model="gemma3:1b",
    )

    # Dictionary map with backwards-compatible aliases for task queue dispatch
    agents_by_name = {
        # Core Consolidated Engines
        "macro_intelligence_engine": macro_intelligence_engine,
        "quant_trading_engine":       quant_trading_engine,
        "knowledge_graph_engine":     knowledge_graph_engine,
        "radar_agent":                 radar_agent,
        "rule_synthesizer":            rule_synthesizer_agent,
        "supervisor":                  supervisor_agent,
        "consensus_engine":            consensus_engine,
        "adversarial_wargamer":        adversarial_wargamer,
        "edge_validator":              edge_validator_agent,
        "stock_correlation_agent":     stock_correlation_agent,

        # Backwards-compatible Task Routing Aliases
        "yield_curve_agent":          macro_intelligence_engine,
        "volatility_surface_agent":   macro_intelligence_engine,
        "macro_strategist":           macro_intelligence_engine,
        "macro_cointegration_engine": macro_intelligence_engine,
        "quant_researcher":           quant_trading_engine,
        "financial_advisor":          quant_trading_engine,
        "insider_clustering_agent":   quant_trading_engine,
        "news_intel":                 knowledge_graph_engine,
        "ontology_master":            knowledge_graph_engine,
        "wargamer":                   adversarial_wargamer,
        "stock_correlation":          stock_correlation_agent,
    }

    logger.info(f"Consolidated Swarm built: 8 core engines live.")
    logger.info(f"Ollama model: {os.getenv('AGENT_MODEL', DEFAULT_MODEL)}")
    logger.info("=" * 60)

    # ── LAUNCH CONSOLIDATED ENGINES ───────────────────────────────────────────
    tier_filter = os.getenv("AGENT_TIER_FILTER", "all").lower()

    agent_tier_map = [
        ("macro_intelligence_engine", macro_intelligence_engine, "heavy"),
        ("quant_trading_engine", quant_trading_engine, "heavy"),
        ("knowledge_graph_engine", knowledge_graph_engine, "fast"),
        ("radar_agent", radar_agent, "fast"),
        ("rule_synthesizer", rule_synthesizer_agent, "fast"),
        ("supervisor", supervisor_agent, "heavy"),
        ("consensus_engine", consensus_engine, "heavy"),
        ("adversarial_wargamer", adversarial_wargamer, "heavy"),
        ("edge_validator", edge_validator_agent, "fast"),
        ("stock_correlation_agent", stock_correlation_agent, "fast"),
    ]

    active_agents = {
        name: agent_inst
        for name, agent_inst, tier in agent_tier_map
        if tier_filter in ("all", tier)
    }

    tasks = [
        safe_create_task(agent_inst.run(), name=name)
        for name, agent_inst in active_agents.items()
    ]
    tasks.append(
        safe_create_task(
            run_task_queue_worker(shared_infra["redis"], active_agents),
            name="task_queue_worker",
        )
    )

    # Register liveness for this tier. Every other service already does this;
    # without it the agent tiers are invisible to the health system and the
    # gateway has to guess at their status.
    heartbeat_component = f"agents-{tier_filter}" if tier_filter != "all" else "agents"

    # The roster this process is actually running, republished on every beat.
    #
    # It used to be written once at startup and then erased: start_heartbeat_task
    # re-wrote the same key every 15 seconds with no metadata, so the roster
    # survived about fifteen seconds of uptime. /agents/processes reads
    # metadata["agents"] and so reported active_agents_count: 0 with null names
    # and null models, while ten agents ran -- which is what left the frontend
    # rendering a hardcoded list of invented agent statistics instead.
    def _tier_metadata() -> dict:
        return {
            "tier": tier_filter,
            "model": os.getenv("AGENT_MODEL", ""),
            "fallback_model": os.getenv("OLLAMA_FALLBACK_MODEL", ""),
            "agents": sorted(active_agents.keys()),
            "agent_detail": [
                {
                    "name": ag.name,
                    "model": ag.model,
                    "fallback_model": ag.fallback_model,
                    "topics": len(ag.input_topics),
                    "processed": int(getattr(ag, "_processed", 0) or 0),
                    "errors": int(getattr(ag, "_errors", 0) or 0),
                }
                for ag in agents_by_name.values()
            ],
        }

    await touch_heartbeat(shared_infra["redis"], heartbeat_component, metadata=_tier_metadata())
    tasks.append(
        safe_create_task(
            start_heartbeat_task(
                shared_infra["redis"], heartbeat_component, metadata=_tier_metadata
            ),
            name="agents_heartbeat",
        )
    )

    logger.info(f"Swarm launched with AGENT_TIER_FILTER='{tier_filter}' ({len(tasks)-1} active agents).")
    for ag in agents_by_name.values():
        logger.info(f"Agent: {ag.name:<26} | Model: {ag.model:<10} | Fallback: {ag.fallback_model:<10} | Topics: {len(ag.input_topics)}")

    try:
        def handle_sigterm(signum, frame):
            logger.info("SIGTERM received — initiating graceful shutdown")
            raise KeyboardInterrupt()
            
        if sys.platform != "win32":
            signal.signal(signal.SIGTERM, handle_sigterm)

        # Wait for all tasks. If any crashes, re-raise to restart via supervisor.
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
        await asyncio.gather(*tasks, return_exceptions=True)
        for ag in agents_by_name.values():
            try:
                await ag.close()
            except Exception:
                pass
        if not main_session.closed:
            await main_session.close()
        logger.info("Agent swarm shut down cleanly")


if __name__ == "__main__":
    # OS FIX: The default ProactorEventLoop on Windows sometimes struggles with certain sockets/subprocesses.
    # Forcing the WindowsSelectorEventLoopPolicy ensures maximum compatibility and stability on Windows machines.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())