"""
services/agents/news_intel.py

NEWS & INTEL AGENT
==================
Consumes enriched news events. Uses Llama3 to extract structured intelligence,
update the Neo4j knowledge graph with new relationships, and emit structured
"Intelligence Briefs" that are richer than the original enriched events.

Pipeline per message:
  1. Receive NormalizedEvent (type=HEADLINE) from enriched.events
  2. Fetch recent context from TimescaleDB (last 4h of high-anomaly events)
  3. Call Llama3 → IntelBrief (structured extraction)
  4. Extract entity relationships → call Llama3 again → GraphTriples
  5. Write triples to Neo4j
  6. Emit IntelBrief to agents.intel.briefs
  7. If geopolitical_theater detected → push keywords to news collector filter

Why two LLM calls per article?
  The intel brief task and the graph extraction task have different cognitive
  requirements. Combining them into one prompt degrades output quality on both.
  Two focused calls with lower token counts outperform one complex call.
  With local Ollama, the compute cost is near-zero compared to API billing.

Deduplication:
  We skip articles we've already processed (Redis 1h TTL keyed on URL hash).
  This handles duplicate RSS entries without database overhead.
"""
import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import SentinelAgent, SchemaViolationError, InferenceError
from shared.utils.equities import is_valid_primary_equity
from .prompts import (
    NEWS_INTEL_SYSTEM,
    build_news_intel_prompt,
    NEWS_INTEL_USER_TEMPLATE,
    GRAPH_EXTRACTION_SYSTEM,
    GRAPH_EXTRACTION_USER_TEMPLATE,
)

logger = logging.getLogger("agent.news_intel")

ANALYSIS_THRESHOLD = 0.35


# ── OUTPUT SCHEMAS ────────────────────────────────────────────────────────────

class IntelEntity(BaseModel):
    name: str
    type: str
    role: str
    sentiment: str = "neutral"
    is_threat_actor: bool = False


class GraphTriple(BaseModel):
    subject: str
    subject_type: str
    predicate: str
    object: str
    object_type: str
    confidence: float = 0.5
    temporal: str = "current"
    source_quote: Optional[str] = None


class IntelBrief(BaseModel):
    headline_summary: str
    catalyst_type: str
    severity: int
    entities: List[IntelEntity] = Field(default_factory=list)
    geographic_hotspots: List[str] = Field(default_factory=list)
    financial_instruments_affected: List[str] = Field(default_factory=list)
    intelligence_gaps: Optional[str] = None
    recommended_monitoring: List[str] = Field(default_factory=list)
    time_sensitivity: str = "days"
    geopolitical_theater: str = "unknown"
    graph_triples: List[GraphTriple] = Field(default_factory=list)


# ── VALID RELATIONSHIP TYPES (for Neo4j MERGE safety) ──────────────────────────
VALID_PREDICATES = {
    "OWNS", "OPERATES", "SUPPLIES", "PURCHASES_FROM", "ALLIED_WITH",
    "SANCTIONS_TARGET", "FLAGGED_BY", "CONTROLS", "SUBSIDIARY_OF",
    "COMPETES_WITH", "ADJACENT_TO", "ATTACKED", "TARGETED_BY",
    "REGISTERED_IN", "EMPLOYS", "POSITIVE_EXPOSURE_TO", "INVERSE_EXPOSURE_TO",
}


# Minimum anomaly score to run full LLM analysis on a news event
ANALYSIS_THRESHOLD = 0.35

# Financial instrument keywords → tickers to auto-flag for monitoring
GEO_KEYWORD_INSTRUMENT_MAP = {
    "hormuz":        ["USO", "BNO", "XOP"],
    "iran":          ["USO", "BNO", "GLD"],
    "taiwan":        ["TSM", "SMH", "NVDA"],
    "russia":        ["WEAT", "UNG", "GLD"],
    "red sea":       ["ZIM", "MAERSK", "BNO"],
    "ransomware":    ["CRWD", "PANW", "CIBR"],
    "semiconductor": ["NVDA", "TSM", "ASML", "SMH"],
    "nato":          ["LMT", "RTX", "NOC"],
}


class NewsIntelAgent(SentinelAgent):
    """
    Autonomous news intelligence researcher.
    Turns raw news signals into structured intelligence briefs
    and knowledge graph updates.
    """

    @property
    def output_topic(self) -> str:
        return "agents.intel.briefs"

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process one enriched news event.
        Returns None for events below the analysis threshold (no output published).
        """
        agent_source = message.get("agent")
        
        # ── SCENARIO HANDLING ───────────────────────────────────────────────
        if "scenario_id" in message:
            return await self._handle_scenario(message)

        if agent_source == "quant_researcher":
            discovery = message.get("discovery", {})
            new_keywords = [p.get("ticker") for p in discovery.get("peer_tickers", [])]
            if new_keywords:
                # Dynamically start looking for news about the newly discovered correlated peers
                await self.redis.raw.sadd("sentinel:news:keywords", *new_keywords)
                logger.info(f"Swarm Sync: News Agent now tracking quant peers {new_keywords}")
            return None

        # B. Handle Standard Enriched News
        event_type = message.get("type", "")
        if event_type not in ("headline", "news_article"):
            return None

        # ── GATE 2: Anomaly threshold ──────────────────────────────────────────
        anomaly_score = float(message.get("anomaly_score", 0))
        if anomaly_score < ANALYSIS_THRESHOLD:
            return None

        # ── GATE 3: Deduplication by URL hash ─────────────────────────────────
        url = message.get("url", message.get("event_id", ""))
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
        if await self.is_recently_processed(url_hash, window_seconds=3600):
            logger.debug(f"Skipping duplicate: {url_hash}")
            return None
        await self.mark_processed(url_hash)

        title   = message.get("headline", "")
        summary = message.get("summary", "")
        source  = message.get("source", "unknown")

        if not title:
            return None

        logger.info(f"Analyzing: {title[:80]}... (anomaly={anomaly_score:.2f})")

        # ── STEP 1: Fetch recent context & Neo4j profiles ─────────────────────
        recent_context, agent_memories, existing_entities = await asyncio.gather(
            self._fetch_recent_context(),
            self.read_agent_memories(limit=8),
            self._fetch_existing_graph_entities_by_names(message.get("named_entities", [])[:10])
        )

        # ── STEP 2: Primary LLM call — Intel Brief & Graph extraction ─────────
        user_prompt = NEWS_INTEL_USER_TEMPLATE.format(
            source=source,
            reliability=message.get("source_reliability", 0.8),
            published_at=message.get("occurred_at", "unknown"),
            named_entities=json.dumps(message.get("named_entities", [])[:15]),
            tags=json.dumps(message.get("tags", [])[:10]),
            title=title,
            body=(summary or title)[:1500],
            recent_context=json.dumps(recent_context[:5], default=str),
            agent_memories=agent_memories,
            existing_entities=json.dumps(existing_entities),
        )

        try:
            dynamic_sys_prompt = build_news_intel_prompt(domain=source, severity=3)
            brief: IntelBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt=dynamic_sys_prompt,
                user_prompt=user_prompt,
                schema=IntelBrief,
                temperature=0.1,
            )
        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Failed to publish intel brief: {e}", exc_info=True)
            return None

        if brief.entities:
            entity_tasks = [
                self._producer.send(
                    "agents.ontology.unknown_entities",
                    {
                        "name": entity.name,
                        "trace_id": message.get("trace_id"),
                        "context": (summary or title)[:500],
                        "source_domain": source,
                        "frequency": 1,
                    },
                    key=entity.name
                )
                for entity in brief.entities
            ]
            await asyncio.gather(*entity_tasks, return_exceptions=True)

        # ── STEP 3: Write pre-extracted Graph relationships to Neo4j ───────────
        if brief.graph_triples:
            await self._extract_and_write_graph(brief, message.get("trace_id"))

        # ── STEP 4: Auto-inject financial instruments into watched list ─────────
        await self._update_instrument_watchlist(brief, title.lower())

        # ── STEP 5: Push new news keywords to the collector's filter ──────────
        await self._update_news_keywords(brief)
        
        # ── STEP 5.5: Provide Semantic Feedback to Anomaly Scorer ─────────────
        await self._update_semantic_sentiment(brief)

        # ── STEP 5.6: Write Episodic Memory for High Severity Events ──────────
        if brief.severity >= 3:
            impacted = [e.name for e in brief.entities[:3]]
            mem_text = f"Intel Brief (Severity {brief.severity}): {brief.headline_summary}. Impacted: {impacted}."
            asyncio.create_task(self.write_agent_memory(mem_text))

        # ── STEP 6: Emit structured Intel Brief ───────────────────────────────
        return {
            "agent":            self.name,
            "agent_run_id":     f"intel_{url_hash}",
            "source_event_id":  message.get("event_id"),
            "trace_id":         message.get("trace_id"),
            "created_at":       datetime.now(timezone.utc).isoformat(),
            "brief":            brief.model_dump(),
            "original_anomaly": anomaly_score,
            "computed_severity": brief.severity,
        }

    async def _update_semantic_sentiment(self, brief: IntelBrief):
        """Pushes semantic sentiment from the LLM back to Redis for the AnomalyScorer."""
        try:
            if not self.redis: return
            pipe = self.redis.raw.pipeline()
            # We decay semantic sentiment after 12 hours
            for entity in brief.entities:
                if entity.sentiment in ("positive", "negative", "critical"):
                    score = 1.0 if entity.sentiment == "positive" else -1.0 if entity.sentiment == "negative" else -2.0
                    pipe.set(f"sentinel:semantic_sentiment:{entity.name.lower()}", score, ex=43200)
            await pipe.execute()
        except Exception as e:
            logger.error(f"Failed to update semantic sentiment: {e}")

    async def _fetch_recent_context(self) -> List[Dict]:
        """
        Fetch recent high-anomaly events from TimescaleDB for context injection.
        Runs in executor to avoid blocking the event loop.
        """
        # psycopg2 (PostgreSQL) is a "blocking" library—it stops the entire Python program
        # while waiting for the database. But we use asyncpg now, so we can await it directly.
        try:
            rows = await self.db.query("""
                SELECT type, headline, anomaly_score, region, tags, occurred_at
                FROM events
                WHERE occurred_at > NOW() - INTERVAL '4 hours'
                  AND anomaly_score >= 0.5
                  AND type != 'headline'
                ORDER BY anomaly_score DESC
                LIMIT 10
            """)
            return [
                {
                    "type":    r.get("type"),
                    "summary": r.get("headline") or f"Event in {r.get('region')}",
                    "score":   r.get("anomaly_score"),
                    "tags":    r.get("tags", [])[:5],
                }
                for r in rows
            ]
        except Exception as e:
            logger.debug(f"Context fetch error: {e}")
            return []

    async def _extract_and_write_graph(
        self, brief: IntelBrief, trace_id: Optional[str]
    ):
        """
        Write pre-extracted entity relationships to Neo4j.
        Failures here are non-fatal — the intel brief still gets published.
        """
        written = 0
        for triple in brief.graph_triples:
            if triple.predicate not in VALID_PREDICATES:
                logger.debug(f"Skipping invalid predicate: {triple.predicate}")
                continue
            if triple.confidence < 0.6:
                continue  # Only high-confidence relationships enter the graph

            try:
                await self._write_neo4j_triple(triple, trace_id)
                written += 1
            except Exception as e:
                logger.warning(f"Neo4j write failed for {triple.subject}→{triple.object}: {e}")

        if written:
            logger.info(f"  Neo4j: +{written} relationships written")

    async def _fetch_existing_graph_entities_by_names(self, names: List[str]) -> List[str]:
        """Pull existing entity names from Neo4j for the given names list."""
        if not names:
            return []
        try:
            rows = await self.neo4j.query(
                "MATCH (n) WHERE n.name IN $names RETURN n.name as name, labels(n) as types LIMIT 20",
                {"names": names},
            )
            return [f"{r['name']} ({r['types']})" for r in rows]
        except Exception:
            return []

    async def _write_neo4j_triple(self, triple: GraphTriple, trace_id: Optional[str]):
        """
        MERGE the triple into Neo4j. Uses dynamic labels.
        MERGE on (name) ensures idempotency — re-running on the same article
        does not create duplicate nodes or relationships.
        """

        # Sanitize: Neo4j labels cannot contain spaces
        subj_label = triple.subject_type.title().replace(" ", "")
        obj_label  = triple.object_type.title().replace(" ", "")

        proposal = {
            "entity_id": triple.subject,
            "trace_id": trace_id,
            "action": "LINK_ENTITY",
            "data": {
                "target_id": triple.object,
                "source_label": subj_label,
                "target_label": obj_label,
                "relation_type": triple.predicate,
                "weight": triple.confidence
            }
        }
        await self._producer.send("sentinel.ontology.proposals", proposal, key=triple.subject)

    async def _update_instrument_watchlist(self, brief: IntelBrief, title_lower: str):
        """
        Auto-add geopolitically relevant financial instruments to the Finnhub watchlist.
        This closes the loop: a news event about Iran triggers USO/BNO tracking immediately.
        """
        # We use a Python `set` to automatically remove duplicate tickers.
        candidates = set(brief.financial_instruments_affected)

        for keyword, tickers in GEO_KEYWORD_INSTRUMENT_MAP.items():
            if keyword in title_lower:
                candidates.update(tickers)

        instruments_to_add = {
            t.upper().strip() for t in candidates 
            if t and isinstance(t, str) and is_valid_primary_equity(t.strip().upper())
        }

        if instruments_to_add:
            try:
                async with self.redis.raw.pipeline(transaction=True) as pipe:
                    pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time() for ticker in instruments_to_add})
                    pipe.zremrangebyrank("sentinel:watched:equities", 0, -51)
                    await pipe.execute()
                logger.info(
                    f"  Watchlist: added {instruments_to_add} from intel trigger"
                )
            except Exception as e:
                logger.warning(f"Watchlist update failed: {e}")

    async def _update_news_keywords(self, brief: IntelBrief):
        """
        Add new entity names and monitoring keywords to the news collector's
        Redis keyword set so future articles get detected faster.
        """
        keywords = set()
        for entity in brief.entities:
            if len(entity.name) > 3:
                keywords.add(entity.name.lower())
        for kw in brief.recommended_monitoring:
            if len(kw) > 3:
                keywords.add(kw.lower()[:30])

        if keywords:
            try:
                await self.redis.raw.sadd("sentinel:news:keywords", *keywords)
            except Exception as e:
                logger.debug(f"Keyword update error: {e}")

    async def _handle_scenario(self, scenario: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        headline = scenario.get("headline", "")
        if not headline:
            return None
            
        logger.info(f"NewsIntelAgent digesting AI-generated scenario: {headline}")
        
        user_prompt = f"""
        A high-level reasoning scenario has emerged from the Sentinel Reasoning Engine:
        HEADLINE: {headline}
        SIGNIFICANCE: {scenario.get('significance', '')}
        HYPOTHESES: {scenario.get('hypotheses', [])}
        
        Write a concise, high-priority intelligence brief summarizing this scenario.
        Focus on geopolitical risk, global supply chain impact, and narrative momentum.
        """
        
        try:
            brief = await self._execute_with_telemetry(
                message=scenario,
                system_prompt=NEWS_INTEL_SYSTEM,
                user_prompt=user_prompt,
                schema=IntelBrief,
                temperature=0.3
            )
            
            if brief:
                run_id = f"scenario_intel_{int(time.time())}"
                payload = {
                    "agent": self.name,
                    "agent_run_id": run_id,
                    "event_id": scenario.get("scenario_id"),
                    "brief": brief.model_dump(),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                if self._producer:
                    await self._producer.send(
                        self.output_topic,
                        payload,
                        key=scenario.get("scenario_id")
                    )
                    logger.info(f"Published Scenario Intel Brief: {brief.headline_summary}")
                    
                return payload
                
        except Exception as e:
            logger.error(f"Failed to synthesize brief for scenario: {e}")
            
        return None
