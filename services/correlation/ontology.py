tology master · PY
Copy

"""
services/agents/ontology_master.py
 
ONTOLOGY MASTER AGENT
=====================
Continuously prunes, validates, and expands the Redis/Neo4j ontology layers.
 
Two operating modes:
 
  EVENT-DRIVEN (primary):
    Consumes from agents.ontology.unknown_entities — a topic that other agents
    publish to when they encounter an unclassified entity. Runs full Llama3
    classification and writes results to Redis + Neo4j immediately.
 
  SCHEDULED (background):
    Every PRUNE_INTERVAL_SECONDS, scans the Redis ontology keys and:
      - Removes stale entries (not seen in > STALENESS_DAYS days)
      - Identifies high-frequency entities that should be promoted to Neo4j nodes
      - Detects concept drift (e.g., "ASTS" was aerospace, now also "telecom")
      - Rebuilds the concept-to-entity inverted index
 
This is SENTINEL's "immune system" — it keeps the knowledge base from accumulating
stale, contradictory, or overly-generic mappings that degrade correlation quality.
 
Why a separate agent for this?
  The ontology is a shared resource. Writes from the Quant agent and Intel agent
  could conflict without a dedicated curator. The Ontology Master owns all writes
  to sentinel:ontology:* keys and validates consistency before writing.
"""
 
import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set
 
from pydantic import BaseModel, Field
 
from .base import SentinelAgent, SchemaViolationError
from .prompts import (
    ONTOLOGY_CATEGORIZE_SYSTEM,
    ONTOLOGY_CATEGORIZE_USER_TEMPLATE,
)
 
logger = logging.getLogger("agent.ontology_master")
 
 
# ── CONFIGURATION ─────────────────────────────────────────────────────────────
PRUNE_INTERVAL_SECONDS = 3600       # Prune every hour
STALENESS_DAYS         = 14         # Remove concepts not seen in 14 days
PROMOTE_FREQUENCY      = 50         # Promote to Neo4j if seen > 50 times
MIN_CLASSIFICATION_FREQ = 3         # Don't classify entities seen < 3 times (noise)
 
# The full SENTINEL macro concept taxonomy
CORE_CONCEPTS = [
    "energy_oil", "energy_gas", "precious_metals", "agriculture",
    "theater_middle_east", "theater_apac", "theater_eeur", "theater_latam",
    "sector_defense", "sector_semis", "sector_shipping",
    "cyber_warfare", "crypto_ecosystem", "aviation_distress",
    "macro_rates", "macro_volatility", "macro_sentiment",
    "data_center", "ai", "semiconductor", "sanctions",
    "supply_chain", "geopolitics", "emerging_tech",
]
 
 
# ── SCHEMA ────────────────────────────────────────────────────────────────────
 
class EntityClassification(BaseModel):
    entity_name: str
    entity_type: str
    primary_domain: str
    macro_concepts: List[str] = Field(default_factory=list)
    geographic_exposure: List[str] = Field(default_factory=list)
    sector_tags: List[str] = Field(default_factory=list)
    sanctions_risk: bool = False
    should_watch_equities: List[str] = Field(default_factory=list)
    should_watch_maritime: bool = False
    should_watch_news_keywords: List[str] = Field(default_factory=list)
    confidence: float = 0.5
    reasoning: str = ""
 
 
class OntologyMasterAgent(SentinelAgent):
    """
    Knowledge graph curator and ontology maintenance agent.
    """
 
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_prune = 0.0
        self._classified_this_cycle = 0
 
    @property
    def output_topic(self) -> str:
        return "agents.ontology.updates"
 
    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle an incoming unknown entity classification request.
        Also runs scheduled pruning when the timer fires.
        """
        # ── SCHEDULED PRUNING ─────────────────────────────────────────────────
        now = time.time()
        if now - self._last_prune > PRUNE_INTERVAL_SECONDS:
            self._last_prune = now
            asyncio.create_task(self._run_maintenance_cycle())
 
        # ── EVENT-DRIVEN CLASSIFICATION ────────────────────────────────────────
        entity_name  = message.get("entity_name", "").strip()
        context      = message.get("context", "")
        source_domain = message.get("source_domain", "unknown")
        frequency    = int(message.get("frequency", 1))
 
        if not entity_name:
            return None
 
        # Skip low-frequency noise — wait until we've seen it a few times
        if frequency < MIN_CLASSIFICATION_FREQ:
            # Increment frequency counter and wait
            self._increment_entity_frequency(entity_name)
            return None
 
        # Dedup: don't reclassify within 6 hours unless forced
        dedup_key = f"classify:{entity_name}"
        if self.is_recently_processed(dedup_key, window_seconds=21600):
            return None
        self.mark_processed(dedup_key, window_seconds=21600)
 
        logger.info(f"Classifying entity: '{entity_name}' (freq={frequency})")
 
        # ── LLM CLASSIFICATION ─────────────────────────────────────────────────
        user_prompt = ONTOLOGY_CATEGORIZE_USER_TEMPLATE.format(
            entity_name=entity_name,
            context=context[:500],
            source_domain=source_domain,
            frequency=frequency,
        )
 
        try:
            classification: EntityClassification = await self._llm.infer(
                system_prompt=ONTOLOGY_CATEGORIZE_SYSTEM,
                user_prompt=user_prompt,
                schema=EntityClassification,
                temperature=0.05,
            )
        except SchemaViolationError as e:
            logger.error(f"Classification failed for '{entity_name}': {e}")
            return None
 
        # Validate concepts against known taxonomy
        classification.macro_concepts = [
            c for c in classification.macro_concepts
            if c in CORE_CONCEPTS
        ]
 
        if classification.confidence < 0.5:
            logger.info(
                f"  Low confidence ({classification.confidence:.2f}) for '{entity_name}' — skipping write"
            )
            return None
 
        # ── WRITE TO REDIS ONTOLOGY ────────────────────────────────────────────
        self._write_to_redis_ontology(entity_name, classification)
 
        # ── CONDITIONALLY WRITE TO NEO4J ──────────────────────────────────────
        if frequency >= PROMOTE_FREQUENCY or classification.confidence >= 0.85:
            asyncio.create_task(
                self._promote_to_neo4j(entity_name, classification)
            )
 
        # ── TRIGGER DOWNSTREAM WATCHLISTS ─────────────────────────────────────
        self._trigger_watchlists(classification)
 
        self._classified_this_cycle += 1
 
        return {
            "agent":           self.name,
            "agent_run_id":    f"ontology_{entity_name}",
            "entity_name":     entity_name,
            "classification":  classification.dict(),
            "created_at":      datetime.now(timezone.utc).isoformat(),
        }
 
    # ── REDIS ONTOLOGY WRITES ─────────────────────────────────────────────────
 
    def _write_to_redis_ontology(
        self,
        entity_name:    str,
        classification: EntityClassification,
    ):
        """
        Write classification to the Redis ontology layer.
 
        Redis structure:
          sentinel:ontology:{concept} → SET of entity names
          sentinel:ontology:entity:{name} → JSON classification record
          sentinel:ontology:freq:{name} → integer frequency counter
        """
        try:
            normalized = entity_name.lower().strip()
 
            # 1. Add entity to each of its concept buckets
            for concept in classification.macro_concepts:
                self.redis.sadd(f"sentinel:ontology:{concept}", normalized)
 
            # 2. Store full classification record (for fast lookup by name)
            record = classification.dict()
            record["classified_at"] = datetime.now(timezone.utc).isoformat()
            self.redis.set(
                f"sentinel:ontology:entity:{normalized}",
                json.dumps(record),
                ttl=STALENESS_DAYS * 86400,
            )
 
            # 3. Update the domain index
            self.redis.sadd(
                f"sentinel:ontology:domain:{classification.primary_domain}",
                normalized,
            )
 
            logger.info(
                f"  Ontology: '{normalized}' → "
                f"{classification.primary_domain} / {classification.macro_concepts}"
            )
 
        except Exception as e:
            logger.error(f"Redis ontology write failed for '{entity_name}': {e}")
 
    def _increment_entity_frequency(self, entity_name: str):
        """Increment the seen-frequency counter for an entity."""
        key = f"sentinel:ontology:freq:{entity_name.lower()}"
        try:
            self.redis.raw.incr(key)
            self.redis.expire(key, 7 * 86400)
            return count
        except Exception as e:
            return 0

    # ── NEO4J PROMOTION ───────────────────────────────────────────────────────
 
    async def _promote_to_neo4j(
        self,
        entity_name:    str,
        classification: EntityClassification,
    ):
        """
        Promote a high-confidence or high-frequency entity to a Neo4j node.
        This makes it queryable in graph traversals and path finding.
        """
        loop = asyncio.get_running_loop()
        label = classification.entity_type.title().replace(" ", "").replace("_", "")
 
        try:
            await loop.run_in_executor(
                None,
                lambda: self.neo4j.execute(f"""
                    MERGE (e:{label} {{name: $name}})
                    SET e.primary_domain    = $domain,
                        e.macro_concepts    = $concepts,
                        e.sanctions_risk    = $sanctions,
                        e.confidence        = $confidence,
                        e.classified_at     = datetime(),
                        e.classifier        = 'ontology_agent'
                """, {
                    "name":       entity_name,
                    "domain":     classification.primary_domain,
                    "concepts":   classification.macro_concepts,
                    "sanctions":  classification.sanctions_risk,
                    "confidence": classification.confidence,
                }),
            )
            for country in classification.geographic_exposure[:5]:
                if len(country) == 2:
                    await loop.run_in_executor(
                        None,
                        lambda c=country: self.neo4j.execute("""
                            MERGE (e {name: $name})
                            MERGE (country:Country {code: $code})
                            MERGE (e)-[:HAS_EXPOSURE_IN]->(country)
                        """, {"name": entity_name, "code": c}),
                    )
 
            logger.info(f"  Neo4j: promoted '{entity_name}' as {label}")
 
        except Exception as e:
            logger.warning(f"Neo4j promotion failed for '{entity_name}': {e}")
            
            # ── DOWNSTREAM WATCHLIST TRIGGERS ─────────────────────────────────────────
    def _trigger_watchlists(self, classification: EntityClassification):
        """
        Based on the classification, trigger relevant data collection.
        """
        try:
            if classification.should_watch_equities:
                valid_tickers = [
                    t for t in classification.should_watch_equities if t.isalpha() and t.isupper() and 1 <= len(t) <= 5
                ]
                if valid_tickers:
                    self.redis.sadd(
                        "sentinel:watched:equities", *valid_tickers
                        )
                    logger.info(f"  Watchlist: added equities {valid_tickers}")
                
            if classification.should_watch_news_keywords:
                self.redis.sadd(
                    "sentinel:news:keywords",
                    *[k.lower()[:30] for k in classification.should_watch_news_keywords],
                )