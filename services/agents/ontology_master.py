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

# CONCEPT: Pydantic Data Models
# We use Pydantic to define the exact shape of the data we want from the LLM.
# Why? LLMs (like Llama3) naturally output conversational text. By providing this
# schema to the LLM via our base agent, we force it to reply with a JSON object
# that perfectly matches these fields and types. If it hallucinates or misses a 
# field, Pydantic will raise a validation error so we can retry or drop the bad data.
class EntityClassification(BaseModel):
    entity_name: str
    entity_type: str
    primary_domain: str
    # BEST PRACTICE: Mutable Defaults in Python
    # We use `Field(default_factory=list)` instead of `macro_concepts: List[str] = []`.
    # In Python, setting a default to `[]` shares the SAME list across all instances
    # of the class, causing terrible bugs. `default_factory` creates a fresh list every time.
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
        # UNDER THE HOOD: Non-blocking Background Tasks
        # time.time() is a lightweight way to check if an hour has passed.
        # By using `asyncio.create_task`, we tell Python to run the maintenance cycle
        # in the background. This ensures the agent can immediately go back to 
        # reading the next Kafka message without waiting for the database cleanup to finish.
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
        # BEST PRACTICE: Debouncing/Noise Reduction
        # LLM inference is expensive (it takes GPU power and time). We don't want to
        # classify every random typo. We wait until an entity is seen 3 times.
        if frequency < MIN_CLASSIFICATION_FREQ:
            # Increment frequency counter and wait
            self._increment_entity_frequency(entity_name)
            return None

        # Dedup: don't reclassify within 6 hours unless forced
        # CONCEPT: Idempotency. If 10 systems report "Apple" in a 5 minute window,
        # this lock ensures we only ask the LLM to classify it once.
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
            # CONCEPT: Low Temperature LLM Inference
            # temperature=0.05 makes the LLM highly deterministic. Instead of being
            # "creative", it will pick the most statistically probable answer.
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
            # UNDER THE HOOD: Redis Sets
            # `sadd` adds to a mathematical "Set". Sets guarantee uniqueness.
            # If "apple" is already in the "technology" bucket, this does nothing (which is good!).
            for concept in classification.macro_concepts:
                self.redis.sadd(f"sentinel:ontology:{concept}", normalized)

            # 2. Store full classification record (for fast lookup by name)
            # BEST PRACTICE: TTL (Time To Live)
            # We set `ttl=STALENESS_DAYS * 86400`. Redis will automatically delete
            # this record after 14 days. This is how the "immune system" naturally cleans itself!
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
            count = self.redis.raw.incr(key)
            self.redis.raw.expire(key, 7 * 86400)  # Reset weekly
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
        Promote a high-confidence or high-frequency entity to a Neo4j node.
        This makes it queryable in graph traversals and path finding.
        """
        loop = asyncio.get_running_loop()
        label = classification.entity_type.title().replace(" ", "").replace("_", "")

        try:
            await loop.run_in_executor(
                None,
                # CONCEPT: Cypher Query Language & Neo4j
                # MERGE means "Create this node if it doesn't exist, otherwise update it".
                # e:Label creates a node where Label might be "Company" or "Vessel".
                # Graph databases excel at finding hidden paths (e.g. Company A -> 
                # owned by Person B -> who is sanctioned).
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

            # Write geographic exposure relationships
            for country in classification.geographic_exposure[:5]:
                if len(country) == 2:  # ISO code
                    # CONCEPT: Python Lambda Late-Binding (The Loop Gotcha)
                    # Why `lambda c=country:` instead of just `lambda: ... country`?
                    # In Python loops, variables inside a lambda are evaluated when the lambda RUNS, 
                    # not when it is created. Since this runs asynchronously in the background, 
                    # the loop might finish before the first lambda executes, making ALL lambdas 
                    # use the last country in the list! 
                    # Passing `c=country` forces Python to capture the *current* value immediately.
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
        
        CONCEPT: Event-Driven Triggers
        By simply adding these text strings to Redis sets (e.g., 'sentinel:watched:equities'),
        we instantly instruct other microservices (like the News Collector or AIS Collector)
        to start paying attention to these specific entities without needing direct API calls.
        """
        try:
            # Equity watchlist
            if classification.should_watch_equities:
                valid_tickers = [
                    t for t in classification.should_watch_equities
                    if t.isalpha() and t.isupper() and 1 <= len(t) <= 5
                ]
                if valid_tickers:
                    self.redis.sadd("sentinel:watched:equities", *valid_tickers)
                    logger.info(f"  Watchlist: added equities {valid_tickers}")

            # Maritime watchlist flag
            if classification.should_watch_maritime:
                self.redis.sadd(
                    "sentinel:watched:entities",
                    classification.entity_name.lower(),
                )

            # News keywords
            if classification.should_watch_news_keywords:
                self.redis.sadd(
                    "sentinel:news:keywords",
                    *[k.lower()[:30] for k in classification.should_watch_news_keywords],
                )

            # Sanctions alert
            if classification.sanctions_risk:
                self.redis.sadd(
                    "sentinel:sanctions:watchlist",
                    classification.entity_name.lower(),
                )
                logger.warning(
                    f"  🔴 Sanctions risk flagged: {classification.entity_name}"
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
            # 1. Prune stale entities from concept buckets
            total_pruned = await self._prune_stale_entities()

            # 2. Find high-frequency entities not yet in Neo4j → promote them
            total_promoted = await self._promote_high_frequency_entities()

            # 3. Rebuild the concept co-occurrence index (for soft correlation)
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
        """
        Remove entities from concept buckets if their Redis classification record
        has expired (TTL elapsed = not seen in STALENESS_DAYS days).
        """
        loop = asyncio.get_running_loop()
        total_pruned = 0

        for concept in CORE_CONCEPTS:
            set_key = f"sentinel:ontology:{concept}"
            try:
                members_raw = await loop.run_in_executor(
                    None, lambda k=set_key: self.redis.raw.smembers(k)
                )
                if not members_raw:
                    continue

                members = [
                    m.decode("utf-8") if isinstance(m, bytes) else m
                    for m in members_raw
                ]

                to_remove = []
                for entity in members:
                    # Check if the classification record still exists
                    # CONCEPT: Concept Drift Management
                    # If the main record was deleted by Redis (because its TTL expired),
                    # we manually prune it out of the concept buckets here.
                    
                    # UNDER THE HOOD: run_in_executor
                    # Redis operations are fast, but they are still "blocking" network calls.
                    # By passing the lambda to `run_in_executor`, we hand the blocking network
                    # call to a background thread pool, keeping our main asyncio event loop completely free.
                    still_alive = await loop.run_in_executor(
                        None,
                        lambda e=entity: self.redis.exists(f"sentinel:ontology:entity:{e}"),
                    )
                    if not still_alive:
                        to_remove.append(entity)

                if to_remove:
                    # UNDER THE HOOD: Redis SREM (Set Remove)
                    # We use the splat operator `*r` to unpack the list. This deletes
                    # all stale entities from the concept bucket in a single network trip,
                    # which is much faster than looping and deleting them one by one.
                    await loop.run_in_executor(
                        None,
                        lambda k=set_key, r=to_remove: self.redis.raw.srem(k, *r),
                    )
                    total_pruned += len(to_remove)
                    logger.debug(f"  Pruned {len(to_remove)} from {concept}: {to_remove[:5]}")

            except Exception as e:
                logger.debug(f"Prune error for {concept}: {e}")

        return total_pruned

    async def _promote_high_frequency_entities(self) -> int:
        """
        Find entities with high frequency counters that haven't been promoted
        to Neo4j yet and promote them.
        """
        loop = asyncio.get_running_loop()
        promoted = 0

        try:
            # Scan freq keys
            # BEST PRACTICE: `scan_iter` vs `keys()`
            # Never use `keys("sentinel:*")` in production Redis! It blocks the entire
            # database while searching. `scan_iter` uses a cursor to safely paginate 
            # through keys a few at a time, keeping Redis fast and responsive.
            freq_keys = await loop.run_in_executor(
                None,
                lambda: list(self.redis.raw.scan_iter("sentinel:ontology:freq:*")),
            )

            for key in freq_keys[:100]:  # Process max 100 per cycle
                try:
                    raw_count = await loop.run_in_executor(
                        None, lambda k=key: self.redis.raw.get(k)
                    )
                    count = int(raw_count) if raw_count else 0
                    if count < PROMOTE_FREQUENCY:
                        continue

                    key_str   = key.decode("utf-8") if isinstance(key, bytes) else key
                    entity    = key_str.replace("sentinel:ontology:freq:", "")

                    # Check if already classified
                    classified = await loop.run_in_executor(
                        None,
                        lambda e=entity: self.redis.exists(f"sentinel:ontology:entity:{e}"),
                    )
                    if not classified:
                        # Push to the classification topic for processing
                        self.enqueue_task(
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
        """
        Build a Redis sorted set of concept pairs that frequently co-occur.
        Used by the soft correlator to find semantically related events
        without embedding lookups.

        CONCEPT: Co-occurrence Matrices (Recommendation Engine Math)
        If we frequently see "crypto_ecosystem" and "cyber_warfare" together,
        we track that overlap here. Later, if a new event is flagged as "crypto",
        our system knows to be on high alert for cyber attacks even if not explicitly mentioned.

        Structure: sentinel:ontology:cooccurrence → ZSET of "conceptA:conceptB" → score
        """
        loop = asyncio.get_running_loop()
        cooccurrence: Dict[str, float] = {}

        try:
            # Sample entities from each concept and find concept overlaps
            for concept in CORE_CONCEPTS:
                members_raw = await loop.run_in_executor(
                    None,
                    lambda c=concept: self.redis.raw.smembers(f"sentinel:ontology:{concept}"),
                )
                members = [
                    m.decode("utf-8") if isinstance(m, bytes) else m
                    for m in (members_raw or [])
                ]

                for entity in members[:20]:
                    record_raw = await loop.run_in_executor(
                        None,
                        lambda e=entity: self.redis.raw.get(f"sentinel:ontology:entity:{e}"),
                    )
                    if not record_raw:
                        continue
                    try:
                        record = json.loads(record_raw)
                        other_concepts = record.get("macro_concepts", [])
                        for other in other_concepts:
                            if other == concept:
                                continue
                            pair = ":".join(sorted([concept, other]))
                            cooccurrence[pair] = cooccurrence.get(pair, 0) + 1
                    except Exception:
                        pass

            if cooccurrence:
                # Write top 200 co-occurrence pairs to Redis
                top_pairs = sorted(
                    cooccurrence.items(), key=lambda x: x[1], reverse=True
                )[:200]
                # UNDER THE HOOD: Redis Pipelines & Sorted Sets (ZSET)
                # A Pipeline bundles multiple commands (delete, zadd, expire) into a single 
                # TCP payload. This minimizes network latency. 
                # A ZSET automatically keeps the pairs sorted by their overlap score!
                pipeline = self.redis.raw.pipeline()
                pipeline.delete("sentinel:ontology:cooccurrence")
                for pair, score in top_pairs:
                    pipeline.zadd("sentinel:ontology:cooccurrence", {pair: score})
                pipeline.expire("sentinel:ontology:cooccurrence", 7 * 86400)
                await loop.run_in_executor(None, pipeline.execute)
                logger.debug(f"  Co-occurrence index: {len(top_pairs)} pairs written")

        except Exception as e:
            logger.debug(f"Co-occurrence rebuild error: {e}")
