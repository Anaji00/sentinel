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

import hashlib
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import SentinelAgent, SchemaViolationError
from .prompts import (
    NEWS_INTEL_SYSTEM,
    NEWS_INTEL_USER_TEMPLATE,
    GRAPH_EXTRACTION_SYSTEM,
    GRAPH_EXTRACTION_USER_TEMPLATE,
)

logger = logging.getLogger("agent.news_intel")


# ── OUTPUT SCHEMAS ────────────────────────────────────────────────────────────

# CONCEPT: Structured AI Output
# We use Pydantic models to define exactly what the AI should return.
# By passing these schemas into our LLM infer method, we force the AI to return
# a predictable JSON object rather than a conversational text blob. 
# If the AI hallucinates the wrong format, Pydantic catches it immediately.
class IntelEntity(BaseModel):
    name: str
    type: str
    role: str
    sentiment: str = "neutral"
    is_threat_actor: bool = False


class IntelRelationship(BaseModel):
    source: str
    relation: str
    target: str
    # BEST PRACTICE: Confidence Scores
    # Always require the AI to assign a confidence score to its extractions. 
    # This allows downstream systems to ignore "guesses" and only act on sure things.
    confidence: float = 0.5


class IntelBrief(BaseModel):
    headline_summary: str
    catalyst_type: str
    severity: int
    entities: List[IntelEntity] = Field(default_factory=list)
    relationships: List[IntelRelationship] = Field(default_factory=list)
    geographic_hotspots: List[str] = Field(default_factory=list)
    financial_instruments_affected: List[str] = Field(default_factory=list)
    intelligence_gaps: Optional[str] = None
    recommended_monitoring: List[str] = Field(default_factory=list)
    time_sensitivity: str = "days"
    geopolitical_theater: str = "unknown"


class GraphTriple(BaseModel):
    subject: str
    subject_type: str
    predicate: str
    object: str
    object_type: str
    confidence: float = 0.5
    temporal: str = "current"
    source_quote: Optional[str] = None


class GraphTriples(BaseModel):
    triples: List[GraphTriple] = Field(default_factory=list)
    new_entities: List[Dict[str, str]] = Field(default_factory=list)


# ── VALID RELATIONSHIP TYPES (for Neo4j MERGE safety) ──────────────────────────
VALID_PREDICATES = {
    "OWNS", "OPERATES", "SUPPLIES", "PURCHASES_FROM", "ALLIED_WITH",
    "SANCTIONS_TARGET", "FLAGGED_BY", "CONTROLS", "SUBSIDIARY_OF",
    "COMPETES_WITH", "ADJACENT_TO", "ATTACKED", "TARGETED_BY",
    "REGISTERED_IN", "EMPLOYS",
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
        # BEST PRACTICE: Guard Clauses (Early Returns)
        # Instead of wrapping the whole function in a giant `if` statement, we check
        # conditions and immediately `return None` to save compute power and indentation.
        agent_source = message.get("agent")
        if not agent_source:
            raise ValueError("Missing 'agent' field in message")

        if agent_source == "quant_researcher":
            discovery = message.get("discovery", {})
            new_keywords = [p.get("ticker") for p in discovery.get("peer_tickers", [])]
            if new_keywords:
                # Dynamically start looking for news about the newly discovered correlated peers
                await asyncio.to_thread(self.redis.raw.sadd, "sentinel:news:keywords", *new_keywords)
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
        # UNDER THE HOOD: Hashing for Caching
        # URLs can be extremely long and contain weird characters. We convert the URL
        # into a short, consistent 16-character string using SHA-256. This makes it
        # a perfect, fast lookup key for Redis to check if we've seen this news before.
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
        if self.is_recently_processed(url_hash, window_seconds=3600):
            logger.debug(f"Skipping duplicate: {url_hash}")
            return None
        self.mark_processed(url_hash)

        title   = message.get("headline", "")
        summary = message.get("summary", "")
        source  = message.get("source", "unknown")

        if not title:
            return None

        logger.info(f"Analyzing: {title[:80]}... (anomaly={anomaly_score:.2f})")

        # ── STEP 1: Fetch recent context from TimescaleDB ──────────────────────
        recent_context = await self._fetch_recent_context()

        # ── STEP 2: Primary LLM call — Intel Brief extraction ─────────────────
        user_prompt = NEWS_INTEL_USER_TEMPLATE.format(
            source=source,
            reliability=message.get("source_reliability", 0.8),
            published_at=message.get("occurred_at", "unknown"),
            named_entities=json.dumps(message.get("named_entities", [])[:15]),
            tags=json.dumps(message.get("tags", [])[:10]),
            title=title,
            body=(summary or title)[:1500],
            recent_context=json.dumps(recent_context[:5], default=str),
        )

        try:
            # CONCEPT: Deterministic AI
            # temperature=0.1 means the AI will be highly factual and logical. 
            # It will pick the most statistically likely words rather than being "creative".
            brief: IntelBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt=NEWS_INTEL_SYSTEM,
                user_prompt=user_prompt,
                schema=IntelBrief,
                temperature=0.1,
            )
        except SchemaViolationError as e:
            logger.error(f"Intel brief extraction failed: {e}")
            return None

        for entity in brief.entities:
            await self._producer.send(
                "agents.ontology.unknown_entities",
                {
                    "name": entity.name,
                    "context": (summary or title)[:500],
                    "source_domain": source,
                    "frequency": 1,
                },
                key = entity.name
            )

        # ── STEP 3: Secondary LLM call — Graph relationship extraction ─────────
        if brief.entities and len(brief.entities) >= 2:
            await self._extract_and_write_graph(title, summary, brief)

        # ── STEP 4: Auto-inject financial instruments into watched list ─────────
        self._update_instrument_watchlist(brief, title.lower())

        # ── STEP 5: Push new news keywords to the collector's filter ──────────
        self._update_news_keywords(brief)

        # ── STEP 6: Emit structured Intel Brief ───────────────────────────────
        return {
            "agent":            self.name,
            "agent_run_id":     f"intel_{url_hash}",
            "source_event_id":  message.get("event_id"),
            "created_at":       datetime.now(timezone.utc).isoformat(),
            "brief":            brief.dict(),
            "original_anomaly": anomaly_score,
            "computed_severity": brief.severity,
        }

    async def _fetch_recent_context(self) -> List[Dict]:
        """
        Fetch recent high-anomaly events from TimescaleDB for context injection.
        Runs in executor to avoid blocking the event loop.
        """
        import asyncio
        loop = asyncio.get_running_loop()
        # UNDER THE HOOD: Thread Executors for Blocking IO
        # psycopg2 (PostgreSQL) is a "blocking" library—it stops the entire Python program
        # while waiting for the database. By using `run_in_executor`, we push this database
        # query into a background thread, allowing our async agent to keep processing other things!
        try:
            rows = await loop.run_in_executor(
                None,
                lambda: self.db.query("""
                    SELECT type, headline, anomaly_score, region, tags, occurred_at
                    FROM events
                    WHERE occurred_at > NOW() - INTERVAL '4 hours'
                      AND anomaly_score >= 0.5
                      AND type != 'headline'
                    ORDER BY anomaly_score DESC
                    LIMIT 10
                """),
            )
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
        self, title: str, summary: str, brief: IntelBrief
    ):
        """
        Extract entity relationships via Llama3 and write them to Neo4j.
        Failures here are non-fatal — the intel brief still gets published.
        """
        # Fetch existing entities from Neo4j to improve consistency
        existing = await self._fetch_existing_graph_entities(brief)

        text = f"{title}\n\n{summary or ''}"
        user_prompt = GRAPH_EXTRACTION_USER_TEMPLATE.format(
            text=text[:2000],
            existing_entities=json.dumps(existing[:20]),
        )

        try:
            triples: GraphTriples = await self._execute_with_telemetry(
                message={"event_id": f"graph_{title[:10]}"}, # Mock ID for sub-task telemetry
                system_prompt=GRAPH_EXTRACTION_SYSTEM,
                user_prompt=user_prompt,
                schema=GraphTriples,
                temperature=0.05,
            )
        except SchemaViolationError as e:
            logger.warning(f"Graph extraction skipped: {e}")
            return

        # Write validated triples to Neo4j
        written = 0
        for triple in triples.triples:
            if triple.predicate not in VALID_PREDICATES:
                logger.debug(f"Skipping invalid predicate: {triple.predicate}")
                continue
            if triple.confidence < 0.6:
                continue  # Only high-confidence relationships enter the graph

            try:
                await self._write_neo4j_triple(triple)
                written += 1
            except Exception as e:
                logger.warning(f"Neo4j write failed for {triple.subject}→{triple.object}: {e}")

        if written:
            logger.info(f"  Neo4j: +{written} relationships written")

    async def _fetch_existing_graph_entities(self, brief: IntelBrief) -> List[str]:
        """Pull existing entity names from Neo4j for the entities in this brief."""
        import asyncio
        loop = asyncio.get_running_loop()
        names = [e.name for e in brief.entities[:5]]
        if not names:
            return []
        try:
            rows = await loop.run_in_executor(
                None,
                lambda: self.neo4j.query(
                    "MATCH (n) WHERE n.name IN $names RETURN n.name as name, labels(n) as types LIMIT 20",
                    {"names": names},
                ),
            )
            return [f"{r['name']} ({r['types']})" for r in rows]
        except Exception:
            return []

    async def _write_neo4j_triple(self, triple: GraphTriple):
        """
        MERGE the triple into Neo4j. Uses dynamic labels.
        MERGE on (name) ensures idempotency — re-running on the same article
        does not create duplicate nodes or relationships.
        """

        # Sanitize: Neo4j labels cannot contain spaces
        subj_label = triple.subject_type.title().replace(" ", "")
        obj_label  = triple.object_type.title().replace(" ", "")

        proposal = {
            "subject": triple.subject,
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

    def _update_instrument_watchlist(self, brief: IntelBrief, title_lower: str):
        """
        Auto-add geopolitically relevant financial instruments to the Finnhub watchlist.
        This closes the loop: a news event about Iran triggers USO/BNO tracking immediately.
        """
        # We use a Python `set` to automatically remove duplicate tickers.
        instruments_to_add = set(brief.financial_instruments_affected)

        for keyword, tickers in GEO_KEYWORD_INSTRUMENT_MAP.items():
            if keyword in title_lower:
                instruments_to_add.update(tickers)

        if instruments_to_add:
            try:
                # UNDER THE HOOD: Redis Sets (SADD)
                # `sadd` adds items to a Redis Set. Just like Python sets, Redis Sets
                # enforce uniqueness. If "USO" is already being watched, this command
                # does nothing safely. `*instruments_to_add` unpacks the python set 
                # so they are all added in one single network command.
                self.redis.sadd("sentinel:watched:equities", *instruments_to_add)
                logger.info(
                    f"  Watchlist: added {instruments_to_add} from intel trigger"
                )
            except Exception as e:
                logger.warning(f"Watchlist update failed: {e}")

    def _update_news_keywords(self, brief: IntelBrief):
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
                self.redis.sadd("sentinel:news:keywords", *keywords)
            except Exception as e:
                logger.debug(f"Keyword update error: {e}")
