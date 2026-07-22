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
import re
from pydantic import BaseModel, Field

from .base import SentinelAgent, SchemaViolationError, InferenceError
from .prompts import (
    ONTOLOGY_CATEGORIZE_SYSTEM,
    build_ontology_prompt,
    ONTOLOGY_CATEGORIZE_USER_TEMPLATE,
)

logger = logging.getLogger("agent.ontology_master")


# ── CONFIGURATION ─────────────────────────────────────────────────────────────
PRUNE_INTERVAL_SECONDS = 3600       # Prune every hour
STALENESS_DAYS         = 14         # Remove concepts not seen in 14 days
PROMOTE_FREQUENCY      = 50         # Promote to Neo4j if seen > 50 times
MIN_CLASSIFICATION_FREQ = 3         # Don't classify entities seen < 3 times (noise)

# Seed concepts used ONLY for initial bootstrapping if the system starts completely empty
BOOTSTRAP_CONCEPTS = [
    "energy_oil", "energy_gas", "precious_metals", "agriculture",
    "sector_defense", "cyber_warfare", "macro_rates", "geopolitics"
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
        self._soft_correlator = kwargs.pop("soft_correlator", None)
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
        entity_name   = message.get("entity_name", "").strip()
        context       = message.get("context", "")
        source_domain = message.get("source_domain", "unknown")
        frequency     = int(message.get("frequency", 1))

        if not entity_name:
            pe = message.get("primary_entity", {})
            if isinstance(pe, dict) and pe.get("name"):
                entity_name = pe["name"].strip()
            elif isinstance(pe, dict) and pe.get("id"):
                entity_name = pe["id"].strip()
            elif message.get("named_entities"):
                entity_name = message["named_entities"][0].strip()

        if not entity_name:
            return None

        # Skip low-frequency noise — wait until we've seen it a few times
        if frequency < MIN_CLASSIFICATION_FREQ:
            # PATCH: Properly await the asynchronous incrementation to prevent blocking
            await self._increment_entity_frequency(entity_name)
            return None

        dedup_key = f"classify:{entity_name}"
        if await self.is_recently_processed(dedup_key, window_seconds=21600):
            return None
        await self.mark_processed(dedup_key, window_seconds=21600)

        logger.info(f"Classifying entity: '{entity_name}' (freq={frequency})")

        # ── LLM CLASSIFICATION ─────────────────────────────────────────────────
        entity_context = await self.fetch_entity_context(entity_name)
        
        user_prompt = ONTOLOGY_CATEGORIZE_USER_TEMPLATE.format(
            entity_name=entity_name,
            context=f"{context[:500]}\nAdditional Context: {entity_context}",
            source_domain=source_domain,
            frequency=frequency,
        )

        try:
            dynamic_sys = build_ontology_prompt(source_domain=source_domain)
            classification: EntityClassification = await self._execute_with_telemetry(
                message=message,
                system_prompt=dynamic_sys,
                user_prompt=user_prompt,
                schema=EntityClassification,
                temperature=0.05,
            )
        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Classification failed for '{entity_name}': {e}")
            return None
        
        if classification.confidence < 0.5:
            logger.info(
                f"  Low confidence ({classification.confidence:.2f}) for '{entity_name}' — skipping write"
            )
            return None

        valid_concepts = []
        if self._soft_correlator:
            for proposed_concept in classification.macro_concepts:
                try:
                    concept_embedding = await self._soft_correlator.embed_text(proposed_concept)
                    similar = await self._soft_correlator.find_similar_concepts(concept_embedding, limit=1)
                    if similar and similar[0]["score"] > 0.85:
                        valid_concepts.append(similar[0]["concept_name"])
                        logger.info(f"Ontology Merge: Mapped '{proposed_concept}' to '{similar[0]['concept_name']}'")
                    else:
                        valid_concepts.append(proposed_concept)
                        await self._register_new_concept(proposed_concept, concept_embedding)
                        logger.warning(f"🌐 AUTONOMOUS EVOLUTION: New macro concept created: '{proposed_concept}'")
                except Exception as e:
                    logger.warning(f"Soft correlator embedding error for '{proposed_concept}': {e}")
                    valid_concepts.append(proposed_concept)
        else:
            valid_concepts = classification.macro_concepts
        
        classification.macro_concepts = list(set(valid_concepts))

        # ── WRITE TO REDIS ONTOLOGY ────────────────────────────────────────────
        # PATCH: Now properly awaited as a background task
        await self._write_to_redis_ontology(entity_name, classification)

        # ── CONDITIONALLY WRITE TO NEO4J ──────────────────────────────────────
        if frequency >= PROMOTE_FREQUENCY or classification.confidence >= 0.85:
            asyncio.create_task(
                self._promote_to_neo4j(entity_name, classification)
            )

        # ── TRIGGER DOWNSTREAM WATCHLISTS ─────────────────────────────────────
        # PATCH: Now properly awaited as a background task
        await self._trigger_watchlists(classification)

        self._classified_this_cycle += 1

        return {
            "agent":           self.name,
            "agent_run_id":    f"ontology_{entity_name}",
            "entity_name":     entity_name,
            "classification":  classification.dict(),
            "created_at":      datetime.now(timezone.utc).isoformat(),
        }
    

    async def _register_new_concept(self, concept_name: str, embedding: List[float]):
        """
        Registers a newly discovered concept in Qdrant and tracks it in Redis
        so background maintenance loops are aware of the expanded taxonomy.
        """
        if hasattr(self._soft_correlator, "register_concept"):
            await self._soft_correlator.register_concept(concept_name, embedding)
        
        await self.redis.raw.sadd("sentinel:ontology:active_concepts", concept_name)
    
    async def _get_active_concepts(self) -> List[str]:
        """Fetches the dynamically expanding list of taxonomy concepts."""
        raw_concepts = await self.redis.raw.smembers("sentinel:ontology:active_concepts")
        concepts = [c.decode("utf-8") if isinstance(c, bytes) else c for c in (raw_concepts or [])]
        
        # If Redis is completely empty, initialize with bootstrap list
        if not concepts:
            concepts = BOOTSTRAP_CONCEPTS
            await self.redis.raw.sadd("sentinel:ontology:active_concepts", *BOOTSTRAP_CONCEPTS)
        return concepts
    # ── REDIS ONTOLOGY WRITES ─────────────────────────────────────────────────

    async def _write_to_redis_ontology(
        self,
        entity_name:    str,
        classification: EntityClassification,
    ):
        """
        Write classification to the Redis ontology layer.
        """
        loop = asyncio.get_running_loop()
        normalized = entity_name.lower().strip()
        record = classification.model_dump() if hasattr(classification, "model_dump") else classification.dict()
        record["classified_at"] = datetime.now(timezone.utc).isoformat()

        try:
            pipeline = self.redis.raw.pipeline()
            for concept in classification.macro_concepts:
                pipeline.sadd(f"sentinel:ontology:{concept}", normalized)
            
            # Using 'ex' parameter to set TTL on the string key
            pipeline.set(
                f"sentinel:ontology:entity:{normalized}", 
                json.dumps(record), 
                ex=STALENESS_DAYS * 86400
            )
            pipeline.sadd(f"sentinel:ontology:domain:{classification.primary_domain}", normalized)
            await pipeline.execute()
            
            logger.info(
                f"  Ontology: '{normalized}' → "
                f"{classification.primary_domain} / {classification.macro_concepts}"
            )
        except Exception as e:
            logger.error(f"Redis ontology write failed for '{entity_name}': {e}")

    async def _increment_entity_frequency(self, entity_name: str):
        """Increment the seen-frequency counter for an entity non-blockingly."""
        loop = asyncio.get_running_loop()
        key = f"sentinel:ontology:freq:{entity_name.lower()}"
        
        try:
            count = await self.redis.raw.incr(key)
            await self.redis.raw.expire(key, 7 * 86400)  # Reset weekly
            return count
        except Exception:
            return 0

    # ── NEO4J PROMOTION ───────────────────────────────────────────────────────

    async def _promote_to_neo4j(
        self,
        entity_name:    str,
        classification: EntityClassification,
    ):
        """
        The Agent no longer writes to Neo4j directly.
        It generates a structured proposal and pushes it to Kafka for the Supervisor.
        """        
        # Cypher Injection Validation (Already applied properly)
        raw_label = classification.entity_type.title().replace(" ", "").replace("_", "")
        if not re.match(r"^[A-Za-z0-9]+$", raw_label):
            logger.warning(f"Cypher Injection attempt or hallucination detected in label: '{raw_label}'. Defaulting to 'UnknownEntity'.")
            label = "UnknownEntity"
        else:
            label = raw_label

        proposal = {
            "entity_id": entity_name,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {
                "label": label,
                "primary_domain": classification.primary_domain,
                "macro_concepts": classification.macro_concepts,
                "sanctions_risk": classification.sanctions_risk,
                "confidence": classification.confidence

            }
        }
        await self._producer.send("sentinel.ontology.proposals", proposal, key=entity_name)

        # Geographic exposures are sent as a separate relational proposal
        for country in classification.geographic_exposure[:5]:
            if len(country) == 2:  
                rel_proposal = {
                    "entity_id": entity_name,
                    "action": "LINK_ENTITY",
                    "data": {
                        "target_id": country,
                        "target_label": "Country",
                        "source_label": label,
                        "relation_type": "HAS_EXPOSURE_IN"
                    }
                }
                await self._producer.send("sentinel.ontology.proposals", rel_proposal, key=entity_name)

        logger.info(f"Proposed Neo4j promotion for '{entity_name}' with label '{label}' and domain '{classification.primary_domain}'")
    # ── DOWNSTREAM WATCHLIST TRIGGERS ─────────────────────────────────────────

    async def _trigger_watchlists(self, classification: EntityClassification):
        """
        Based on the classification, trigger relevant data collection non-blockingly.
        """
        try:
            pipeline = self.redis.raw.pipeline()
            
            # Equity watchlist
            if classification.should_watch_equities:
                valid_tickers = [
                    t for t in classification.should_watch_equities
                    if t.isalpha() and t.isupper() and 1 <= len(t) <= 5
                ]
                if valid_tickers:
                    import time
                    pipeline.zadd("sentinel:watched:equities", mapping={ticker: time.time() for ticker in valid_tickers})
                    pipeline.zremrangebyrank("sentinel:watched:equities", 0, -51)
                    

            # Maritime watchlist flag
            if classification.should_watch_maritime:
                pipeline.sadd(
                    "sentinel:watched:entities",
                    classification.entity_name.lower(),
                )

            # News keywords
            if classification.should_watch_news_keywords:
                pipeline.sadd(
                    "sentinel:news:keywords",
                    *[k.lower()[:30] for k in classification.should_watch_news_keywords],
                )

            # Sanctions alert
            if classification.sanctions_risk:
                pipeline.sadd(
                    "sentinel:sanctions:watchlist",
                    classification.entity_name.lower(),
                )
                logger.warning(
                    f"  🔴 Sanctions risk flagged: {classification.entity_name}"
                )
                
            await pipeline.execute()
            logger.info(
                f"  Watchlist triggers: equities={classification.should_watch_equities} "
                f"maritime={classification.should_watch_maritime} "
                f"news_keywords={classification.should_watch_news_keywords} "
                f"sanctions={classification.sanctions_risk}"
            )
        except Exception as e:
            logger.warning(f"Watchlist trigger error: {e}")

    # ── SCHEDULED MAINTENANCE ─────────────────────────────────────────────────

    async def _run_maintenance_cycle(self):
        """
        Scheduled ontology maintenance.
        Runs as a background task so it doesn't block the consume loop.
        """
        logger.info("Running ontology maintenance cycle...")
        t0 = time.monotonic()

        total_pruned   = 0
        total_promoted = 0

        try:
            total_pruned = await self._prune_stale_entities()
            total_promoted = await self._promote_high_frequency_entities()
            await self._rebuild_cooccurrence_index()

            elapsed = time.monotonic() - t0
            logger.info(
                f"Ontology maintenance complete: "
                f"pruned={total_pruned} promoted={total_promoted} "
                f"elapsed={elapsed:.1f}s classified_this_cycle={self._classified_this_cycle}"
            )
            self._classified_this_cycle = 0

        except Exception as e:
            logger.error(f"Maintenance cycle error: {e}", exc_info=True)

    async def _prune_stale_entities(self) -> int:
        total_pruned = 0

        active_concepts = await self._get_active_concepts()

        for concept in active_concepts:
            set_key = f"sentinel:ontology:{concept}"
            try:
                members_raw = await self.redis.raw.smembers(set_key)
                if not members_raw: continue
                members = [m.decode("utf-8") if isinstance(m, bytes) else m for m in members_raw]
                to_remove = []

                for entity in members:
                    still_alive = await self.redis.raw.exists(f"sentinel:ontology:entity:{entity}")
                    if not still_alive:
                        to_remove.append(entity)
                
                if to_remove:
                    await self.redis.raw.srem(set_key, *to_remove)
                    total_pruned += len(to_remove)
            except Exception as e:
                logger.debug(f"Prune error for concept '{concept}': {e}")

        return total_pruned

    async def _promote_high_frequency_entities(self) -> int:
        promoted = 0

        try:
            freq_keys = [k async for k in self.redis.raw.scan_iter("sentinel:ontology:freq:*")]

            for key in freq_keys[:100]:
                try:
                    raw_count = await self.redis.raw.get(key)
                    count = int(raw_count) if raw_count else 0
                    if count < PROMOTE_FREQUENCY:
                        continue

                    key_str   = key.decode("utf-8") if isinstance(key, bytes) else key
                    entity    = key_str.replace("sentinel:ontology:freq:", "")

                    classified = await self.redis.raw.exists(f"sentinel:ontology:entity:{entity}")
                    if not classified:
                        await self.enqueue_task(
                            "classify_entity",
                            {"entity_name": entity, "frequency": count, "context": ""},
                            priority="low",
                        )
                        promoted += 1
                except Exception:
                    pass

        except Exception as e:
            logger.debug(f"Promote scan error: {e}")

        return promoted

    async def _rebuild_cooccurrence_index(self):
        cooccurrence: Dict[str, float] = {}
        active_concepts = await self._get_active_concepts()
        try:
            for concept in active_concepts:
                members_raw = await self.redis.raw.smembers(f"sentinel:ontology:{concept}")
                members = [m.decode("utf-8") if isinstance(m, bytes) else m for m in (members_raw or [])]
                for entity in members[:20]:
                    record_raw = await self.redis.raw.get(f"sentinel:ontology:entity:{entity}")
                    if not record_raw: continue
                    try:
                        record = json.loads(record_raw)
                        other_concepts = record.get("macro_concepts", [])
                        for other in other_concepts:
                            if other == concept: continue
                            pair = ":".join(sorted([concept, other]))
                            cooccurrence[pair] = cooccurrence.get(pair, 0) + 1
                    except Exception:
                        pass


            if cooccurrence:
                top_pairs = sorted(
                    cooccurrence.items(), key=lambda x: x[1], reverse=True
                )[:200]
                
                pipeline = self.redis.raw.pipeline()
                pipeline.delete("sentinel:ontology:cooccurrence")
                for pair, score in top_pairs:
                    pipeline.zadd("sentinel:ontology:cooccurrence", {pair: score})
                pipeline.expire("sentinel:ontology:cooccurrence", 7 * 86400)
                await pipeline.execute()
                logger.debug(f"  Co-occurrence index: {len(top_pairs)} pairs written")

        except Exception as e:
            logger.debug(f"Co-occurrence rebuild error: {e}")