"""
services/agents/knowledge_graph_engine.py

MASTER QUANT & SWE CONSOLIDATED KNOWLEDGE GRAPH ENGINE
======================================================
Consolidates 2 intelligence agents into a single high-performance engine:
  - NewsIntelAgent (Headline parsing, structured IntelBrief extraction, severe event alerts)
  - OntologyMasterAgent (Entity classification, graph triples, direct Neo4j MERGE)

Preserves 100% of existing Kafka topics, Redis keys, and output schemas.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from services.agents.base import SentinelAgent, SchemaViolationError, InferenceError
from shared.kafka import Topics
from shared.db import get_neo4j
from services.agents.news_intel import IntelBrief, GraphTriple, IntelEntity, VALID_PREDICATES

logger = logging.getLogger("agent.knowledge_graph")


class EntityClassification(BaseModel):
    entity_name: str
    primary_domain: str  # "maritime", "aviation", "financial", "cyber", "geopolitical"
    suggested_label: str  # "Company", "Vessel", "Aircraft", "Organization", "Location"
    confidence: float
    aliases: List[str] = Field(default_factory=list)
    macro_concepts: List[str] = Field(default_factory=list)
    reasoning: str


class KnowledgeGraphEngine(SentinelAgent):
    """
    Unified Knowledge Graph Engine.
    Combines news intelligence synthesis, entity classification, relationship triple extraction,
    and single-transaction Neo4j MERGE updates in a single pass.
    """

    @property
    def output_topic(self) -> str:
        return Topics.INTEL_BRIEFS

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source = message.get("source", "")
        event_type = message.get("type", "")
        raw = message.get("raw_payload", message)

        # ── 1. UNKNOWN ENTITY CLASSIFICATION REQUEST HANDLER ──────────────────
        if "unknown_entities" in source or event_type == "CLASSIFY_ENTITY":
            entity_name = message.get("entity_name") or raw.get("entity_name")
            if entity_name:
                return await self._classify_and_merge_entity(entity_name, message)
            return None

        # ── 2. NEWS INTEL & GRAPH TRIPLE EXTRACTION HANDLER ───────────────────
        headline = message.get("headline") or raw.get("headline") or raw.get("title")
        if not headline:
            return None

        anomaly_score = float(message.get("anomaly_score", 0.35))
        if anomaly_score < 0.35:
            return None

        dedup_key = f"news_intel:{hash(headline)}:{int(time.time() // 3600)}"
        if await self.is_recently_processed(dedup_key, window_seconds=3600):
            return None
        await self.mark_processed(dedup_key, window_seconds=3600)

        logger.info(f"📰 Processing News Intel & Graph Triples | Headline: '{headline[:60]}...'")

        global_context = await self.fetch_global_context()

        user_prompt = f"""
        Extract structured intelligence brief and Neo4j relationship triples:
        - Headline: {headline}
        - Summary: {message.get("summary", "")}
        - Anomaly Score: {anomaly_score:.2f}
        - Source: {source}

        GLOBAL CONTEXT:
        {global_context}

        Return raw JSON matching IntelBrief schema exactly.
        """

        try:
            brief: IntelBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Chief Intelligence Officer & Knowledge Architect. Extract structured intelligence brief and entity relationship triples. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=IntelBrief,
                temperature=0.1,
            )

            # Direct single-transaction Neo4j MERGE for extracted triples
            if brief.graph_triples:
                asyncio.create_task(self._merge_graph_triples(brief.graph_triples))

            # Update co-occurrence matrix in Redis
            if len(brief.geographic_hotspots) >= 2:
                async with self.redis.raw.pipeline(transaction=True) as pipe:
                    for i in range(len(brief.geographic_hotspots)):
                        for j in range(i + 1, len(brief.geographic_hotspots)):
                            pair = f"{brief.geographic_hotspots[i]}:{brief.geographic_hotspots[j]}"
                            pipe.zincrby("sentinel:ontology:cooccurrence", 1, pair)
                    await pipe.execute()

            brief_dict = brief.model_dump()
            res_payload = {
                "agent": self.name,
                "agent_run_id": f"intel_{int(time.time())}",
                "trace_id": message.get("trace_id"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief_dict,
                "computed_severity": brief.severity,
            }

            # Cache latest brief for swarm & dashboard
            await self.redis.raw.set("sentinel:intel:briefs:latest", json.dumps(res_payload["brief"]), ex=86400)

            # Emit to agents.ontology.updates for backwards compatibility
            if brief.graph_triples:
                await self._producer.send(Topics.ONTOLOGY_UPDATES, {
                    "triples": [t.model_dump() for t in brief.graph_triples],
                    "source_headline": headline,
                }, key=str(time.time()))

            # Publish structured AgentBulletin
            asyncio.create_task(self.publish_bulletin(
                bulletin_type="alert" if brief.severity >= 4 else "thesis",
                summary=f"Intel ({brief.geopolitical_theater}): {brief.headline_summary[:80]}",
                conviction=min(1.0, brief.severity / 5.0),
                expected_direction="neutral",
                payload={"severity": brief.severity, "hotspots": brief.geographic_hotspots, "theater": brief.geopolitical_theater},
                ttl_seconds=3600,
            ))

            return res_payload

        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"News intel extraction failed: {e}")
            return None

    # ── ENTITY CLASSIFICATION ──────────────────────────────────────────────────

    async def _classify_and_merge_entity(self, entity_name: str, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        cache_key = f"sentinel:ontology:entity:{entity_name.lower()}"
        cached = await self.redis.raw.get(cache_key)
        if cached:
            return json.loads(cached if isinstance(cached, str) else cached.decode("utf-8"))

        user_prompt = f"Classify unknown entity '{entity_name}' for ontology graph. Context: {message.get('context', '')}"

        try:
            classification: EntityClassification = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Ontology Master. Classify unknown entity into ontology types and macro concepts. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=EntityClassification,
                temperature=0.1,
            )

            # Write directly to Neo4j
            neo4j_client = await get_neo4j()
            query = """
            MERGE (e:Entity {id: $id})
            SET e.name = $name,
                e.primary_domain = $domain,
                e.type = $type,
                e.classified_at = timestamp()
            """
            await neo4j_client.query(query, {
                "id": entity_name.upper(),
                "name": entity_name,
                "domain": classification.primary_domain,
                "type": classification.suggested_label.lower(),
            })

            data = classification.model_dump()
            await self.redis.raw.set(cache_key, json.dumps(data), ex=604800)
            return data

        except Exception as e:
            logger.error(f"Entity classification failed for '{entity_name}': {e}")
            return None

    # ── GRAPH TRIPLE MERGING ───────────────────────────────────────────────────

    async def _merge_graph_triples(self, triples: List[GraphTriple]) -> None:
        try:
            neo4j_client = await get_neo4j()
            for t in triples:
                predicate = t.predicate if t.predicate in VALID_PREDICATES else "RELATED_TO"
                query = f"""
                MERGE (a:Entity {{id: $subj}})
                SET a.type = $subj_type
                MERGE (b:Entity {{id: $obj}})
                SET b.type = $obj_type
                MERGE (a)-[r:{predicate}]->(b)
                SET r.confidence = $conf, r.last_updated = timestamp()
                """
                await neo4j_client.query(query, {
                    "subj": t.subject.upper(),
                    "subj_type": t.subject_type,
                    "obj": t.object.upper(),
                    "obj_type": t.object_type,
                    "conf": t.confidence,
                })
        except Exception as e:
            logger.debug(f"Graph triple MERGE warning: {e}")
