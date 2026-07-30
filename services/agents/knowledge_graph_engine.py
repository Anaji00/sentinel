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

from pydantic import BaseModel, Field, model_validator

from services.agents.base import SentinelAgent, SchemaViolationError, InferenceError
from shared.kafka import Topics
VALID_PREDICATES = {
    "OPERATES_IN", "OWNED_BY", "AFFILIATED_WITH", "SANCTIONED_BY",
    "TARGETS", "CONFLICTS_WITH", "SUPPLIES", "LOCATED_IN", "RELATED_TO"
}


class IntelEntity(BaseModel):
    name: str
    entity_type: str  # "Company", "Vessel", "Aircraft", "Organization", "Location", "Person"

    @model_validator(mode="before")
    @classmethod
    def _coerce_entity(cls, data: Any) -> Any:
        if isinstance(data, str):
            return {"name": data, "entity_type": "Organization"}
        if isinstance(data, dict):
            name = data.get("name") or data.get("entity") or data.get("label") or data.get("item") or data.get("title") or "Unknown"
            entity_type = data.get("entity_type") or data.get("type") or data.get("category") or "Organization"
            return {"name": str(name), "entity_type": str(entity_type)}
        return data


class GraphTriple(BaseModel):
    subject: str
    predicate: str
    object: str
    confidence: float = 0.8

    @model_validator(mode="before")
    @classmethod
    def _coerce_triple(cls, data: Any) -> Any:
        if isinstance(data, (list, tuple)):
            if len(data) >= 3:
                return {"subject": str(data[0]), "predicate": str(data[1]), "object": str(data[2])}
            elif len(data) == 2:
                return {"subject": str(data[0]), "predicate": "RELATED_TO", "object": str(data[1])}
        if isinstance(data, str):
            parts = data.split()
            if len(parts) >= 3:
                return {"subject": parts[0], "predicate": parts[1], "object": " ".join(parts[2:])}
            return {"subject": data, "predicate": "RELATED_TO", "object": "Unknown"}
        if isinstance(data, dict):
            subj = data.get("subject") or data.get("head") or data.get("source") or data.get("from") or "Unknown"
            pred = data.get("predicate") or data.get("relation") or data.get("type") or data.get("action") or "RELATED_TO"
            obj = data.get("object") or data.get("tail") or data.get("target") or data.get("to") or "Unknown"
            conf = data.get("confidence", 0.8)
            try:
                conf = float(conf)
            except (ValueError, TypeError):
                conf = 0.8
            return {"subject": str(subj), "predicate": str(pred), "object": str(obj), "confidence": conf}
        return data


class IntelBrief(BaseModel):
    headline: str
    summary: str
    headline_summary: str = ""
    primary_entity: Optional[str] = None
    geopolitical_theater: str = "Global"
    geographic_hotspots: List[str] = Field(default_factory=list)
    entities: List[IntelEntity] = Field(default_factory=list)
    graph_triples: List[GraphTriple] = Field(default_factory=list)
    severity: int = 3
    tags: List[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce_brief(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        raw_headline = data.get("headline") or data.get("title") or data.get("summary")
        raw_summary = data.get("summary") or data.get("description") or raw_headline

        if isinstance(raw_headline, list):
            raw_headline = " ".join(str(x) for x in raw_headline)
        if isinstance(raw_summary, list):
            raw_summary = " ".join(str(x) for x in raw_summary)

        if not raw_headline or not str(raw_headline).strip() or not raw_summary or not str(raw_summary).strip():
            raise ValueError("No usable headline/title/summary present in intel brief data")

        headline = str(raw_headline).strip()
        summary = str(raw_summary).strip()

        data["headline"] = headline
        data["summary"] = summary
        data["headline_summary"] = str(data.get("headline_summary") or headline)

        pe = data.get("primary_entity")
        if isinstance(pe, list):
            data["primary_entity"] = str(pe[0]) if pe else None
        elif pe is not None:
            data["primary_entity"] = str(pe)

        gt = data.get("geopolitical_theater") or "Global"
        if isinstance(gt, list):
            gt = str(gt[0]) if gt else "Global"
        data["geopolitical_theater"] = str(gt)

        gh = data.get("geographic_hotspots")
        if isinstance(gh, str):
            data["geographic_hotspots"] = [x.strip() for x in gh.split(",") if x.strip()]
        elif not isinstance(gh, list):
            data["geographic_hotspots"] = []

        tags = data.get("tags")
        if isinstance(tags, str):
            data["tags"] = [x.strip() for x in tags.split(",") if x.strip()]
        elif not isinstance(tags, list):
            data["tags"] = []

        sev = data.get("severity", 3)
        try:
            sev_int = int(float(sev))
            data["severity"] = max(1, min(5, sev_int))
        except (ValueError, TypeError):
            data["severity"] = 3

        return data


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

        global_context, cross_context = await asyncio.gather(
            self.fetch_global_context(),
            self.get_cross_agent_context(limit=3),
        )
        cross_block = f"\nCROSS-AGENT INTELLIGENCE:\n{cross_context}\n" if cross_context else ""

        allowed_preds_str = ", ".join(sorted(VALID_PREDICATES))
        user_prompt = f"""
        Extract structured intelligence brief and Neo4j relationship triples:
        - Headline: {headline}
        - Summary: {message.get("summary", "")}
        - Anomaly Score: {anomaly_score:.2f}
        - Source: {source}

        GLOBAL CONTEXT:
        {global_context}
        {cross_block}
        PREDICATE CONSTRAINTS & TRIPLE RULES:
        - Allowed predicates for graph_triples: {allowed_preds_str}
        - Omit a triple rather than inventing a predicate if none of the allowed predicates fit.
        - FORBIDDEN: Do NOT use provenance, reporting, or sourcing (e.g., "Source", "SOURCED_FROM", "REPORTED_BY") as relationship predicates.
        - FORBIDDEN: Do NOT create self-loop triples where subject == object.

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

            # Filter graph triples using whitelist, self-loop guard, and length limit
            valid_triples = [
                t for t in brief.graph_triples
                if t.predicate in VALID_PREDICATES
                and t.subject.strip().lower() != t.object.strip().lower()
                and len(t.subject) <= 80
                and len(t.object) <= 80
            ]

            # Direct single-transaction Neo4j MERGE for extracted triples
            if valid_triples:
                asyncio.create_task(self._merge_graph_triples(valid_triples))

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
            if valid_triples:
                await self._producer.send(Topics.ONTOLOGY_UPDATES, {
                    "triples": [t.model_dump() for t in valid_triples],
                    "source_headline": headline,
                }, key=str(time.time()))

            # Publish structured AgentBulletin
            asyncio.create_task(self.publish_bulletin(
                bulletin_type="alert" if brief.severity >= 4 else "thesis",
                summary=f"Intel ({brief.geopolitical_theater}): {(brief.headline_summary or brief.headline or brief.summary)[:80]}",
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
            neo4j_client = self.neo4j
            if neo4j_client is None:
                from shared.db import get_neo4j
                neo4j_client = await get_neo4j()

            for t in triples:
                if t.predicate not in VALID_PREDICATES:
                    continue
                if t.subject.strip().lower() == t.object.strip().lower():
                    continue
                if len(t.subject) > 80 or len(t.object) > 80:
                    continue

                query = f"""
                MERGE (a:Entity {{id: $subj}})
                SET a.type = $subj_type
                MERGE (b:Entity {{id: $obj}})
                SET b.type = $obj_type
                MERGE (a)-[r:{t.predicate}]->(b)
                SET r.confidence = $conf, r.last_updated = timestamp()
                """
                await neo4j_client.query(query, {
                    "subj": t.subject.upper(),
                    "subj_type": getattr(t, 'subject_type', 'entity'),
                    "obj": t.object.upper(),
                    "obj_type": getattr(t, 'object_type', 'entity'),
                    "conf": t.confidence,
                })
        except Exception as e:
            logger.error(f"Failed to merge graph triples into Neo4j: {e}")

    async def get_entity_centrality(self, entity_id: str) -> float:
        """
        Fetches degree centrality for an entity in Neo4j graph.
        Weights anomaly correlation-cluster severity by node centrality.
        """
        try:
            from shared.db import get_neo4j
            import math
            neo4j_client = await get_neo4j()
            query = """
            MATCH (e:Entity {id: $id})
            OPTIONAL MATCH (e)-[r]-(neighbor)
            RETURN count(r) as degree
            """
            res = await neo4j_client.query(query, {"id": entity_id.upper()})
            if res and res[0].get("degree"):
                degree = float(res[0]["degree"])
                return 1.0 + math.log(1.0 + degree)
        except Exception as e:
            logger.debug(f"Centrality query fallback for {entity_id}: {e}")
        return 1.0
