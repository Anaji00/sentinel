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

from services.agents.base import InferenceBatcher, SentinelAgent, SchemaViolationError, InferenceError, DEDUP_WINDOW_SLOW_SEC
from shared.kafka import Topics
from shared.models.ontology import VALID_PREDICATES, is_valid_predicate, normalize_predicate
from shared.models.events import graph_node_id, UNRATED_EDGE_CONFIDENCE
from shared.utils.tasks import safe_create_task
from shared.utils.text import clip


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
    confidence: float = UNRATED_EDGE_CONFIDENCE

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
            conf = data.get("confidence", UNRATED_EDGE_CONFIDENCE)
            try:
                conf = float(conf)
            except (ValueError, TypeError):
                conf = UNRATED_EDGE_CONFIDENCE
            return {"subject": str(subj), "predicate": str(pred), "object": str(obj), "confidence": conf}
        return data


class IntelBrief(BaseModel):
    headline: str
    summary: str
    headline_summary: str = ""
    # Required, not optional.
    #
    # The prompt lists this under "Required fields" and the schema said
    # Optional[str] = None. Ollama builds its decoding grammar from the schema,
    # so the model was free to omit it and did on every brief published -- a
    # bulletin whose own summary read "ANTAY moored in Black Sea" carried
    # ticker=None and primary_entity_id=None, naming the vessel in prose and
    # nowhere a consumer could read it. The consensus engine fuses by entity, so
    # a brief with no entity cannot be corroborated or contradicted by anything.
    #
    # min_length=1 is what reaches the grammar: "" is not an entity.
    primary_entity: str = Field(..., min_length=1)
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

        # The cluster knows its own subject. Where the model left the field out,
        # take it from the entities it did name rather than rejecting a brief
        # that is otherwise complete -- this is the repair of an omission, not a
        # guess at what the brief was about.
        if not str(data.get("primary_entity") or "").strip():
            ents = data.get("entities") or []
            for e in ents:
                name = e.get("name") if isinstance(e, dict) else getattr(e, "name", None)
                if name and str(name).strip():
                    data["primary_entity"] = str(name).strip()
                    break

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


class EntityClassificationBatch(BaseModel):
    """Several classifications from one inference.

    Entity classification is the cheapest question this swarm asks -- a label, a
    domain, a confidence -- and it was costing a whole 600-second budget slot
    per entity, on the agent with the highest message rate in the tier. Twelve
    entities in one prompt is twelve classifications for that same slot.
    """
    classifications: List[EntityClassification]


# Actions the GraphSupervisor consumes off the shared ontology topic. Listed
# here so this engine can tell a command apart from an observation; keep in step
# with the branches in services/agents/supervisor.py.
_SUPERVISOR_GRAPH_ACTIONS = frozenset({
    "MERGE_ONTOLOGY_NODE",
    "LINK_ENTITY",
    "ADD_SYMPATHY_EDGE",
    "ADD_TAGS",
})


class KnowledgeGraphEngine(SentinelAgent):
    """
    Unified Knowledge Graph Engine.
    Combines news intelligence synthesis, entity classification, relationship triple extraction,
    and single-transaction Neo4j MERGE updates in a single pass.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._classification_batcher = InferenceBatcher(
            name="entity_classification",
            flush_fn=self._classify_batch,
            max_items=int(os.getenv("ONTOLOGY_BATCH_SIZE", "12")),
            max_wait_sec=float(os.getenv("ONTOLOGY_BATCH_WAIT_SEC", "20")),
            logger=logger,
            max_waiters=self.dispatch_concurrency,
        )

    async def _classify_batch(self, items: List[tuple]) -> Dict[str, "EntityClassification"]:
        """Classifies every queued entity in one inference.

        Returns {entity_name: classification}. An entity the model does not
        answer for is absent, and the batcher resolves it to None -- so it stays
        unclassified rather than acquiring a label nothing chose for it. An
        unclassified entity is simply retried later; a wrongly labelled one
        propagates into the graph and is much harder to notice.
        """
        if not items:
            return {}

        listed = []
        for _, ctx in items:
            line = f"- {ctx['entity_name']}"
            if ctx.get("context"):
                line += f" (seen in: {clip(str(ctx['context']), 120)})"
            listed.append(line)
        names = [ctx["entity_name"] for _, ctx in items]

        prompt = (
            f"Classify each of these {len(items)} unknown entities for an intelligence "
            f"ontology.\n\nENTITIES\n" + f"\n".join(listed) + f"\n\n"
            "primary_domain must be one of: maritime, aviation, financial, cyber, geopolitical.\n"
            "suggested_label must be one of: Company, Vessel, Aircraft, Organization, "
            "Location, Person, Instrument, Infrastructure.\n"
            "Classify each entity independently -- appearing in the same batch implies "
            "no relationship between them.\n"
            "If an entity cannot be classified from its name and context, omit it "
            "entirely rather than guessing.\n\n"
            "Return ONE classification per entity you can classify, and no others."
        )

        batch = await self._execute_with_telemetry(
            message=items[0][1].get("message", {}),
            system_prompt=(
                "You are SENTINEL Ontology Master. Classify unknown entities into "
                "ontology types and macro concepts. Return ONLY raw JSON."
            ),
            user_prompt=prompt,
            schema=EntityClassificationBatch,
            temperature=0.1,
            num_predict=min(1024, 128 + 64 * len(items)),
        )

        wanted = {n.lower(): n for n in names}
        out: Dict[str, EntityClassification] = {}
        for c in (getattr(batch, "classifications", None) or []):
            key = str(getattr(c, "entity_name", "")).strip().lower()
            # Only entities actually asked about. A classification for a name
            # the model invented must never reach the ontology.
            if key in wanted and wanted[key] not in out:
                out[wanted[key]] = c
        return out

    @property
    def output_topic(self) -> str:
        return Topics.INTEL_BRIEFS

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source = str(message.get("source", ""))
        event_type = str(message.get("type", ""))
        raw = message.get("raw_payload") if isinstance(message.get("raw_payload"), dict) else message

        # ── 1. UNKNOWN ENTITY CLASSIFICATION REQUEST HANDLER ──────────────────
        if "unknown_entities" in source or event_type in ("CLASSIFY_ENTITY", "ONTOLOGY_PROPOSAL"):
            entity_name = (
                message.get("entity_name")
                or raw.get("entity_name")
                or message.get("entity_id")
                or raw.get("entity_id")
                or message.get("primary_entity_id")
            )
            if entity_name and not (message.get("headline") or raw.get("headline") or raw.get("title")):
                return await self._classify_and_merge_entity(str(entity_name), message)

        # ── 1b. GOVERNANCE COMMANDS ARE NOT INTELLIGENCE ──────────────────────
        # Graph mutations bound for the supervisor share this topic. They are
        # commands -- "merge this node", "link these two" -- carrying no prose to
        # extract triples from. Because they have no headline, the fallback below
        # fabricated one ("Intelligence update for entity X") and the missing
        # anomaly_score defaulted to 0.50, above the skip threshold, so every one
        # of them bought a full LLM round trip to reason about a placeholder.
        #
        # Measured: 338 of 335 recently processed messages were that synthesized
        # headline -- effectively the entire inference budget -- while the
        # consumer sat 144,000 messages behind and losing ground.
        if str(message.get("action", "")).upper() in _SUPERVISOR_GRAPH_ACTIONS:
            return None

        # ── 2. NEWS INTEL & GRAPH TRIPLE EXTRACTION HANDLER ───────────────────
        headline = (
            message.get("headline")
            or raw.get("headline")
            or raw.get("title")
            or message.get("summary")
            or raw.get("summary")
            or message.get("description")
            or raw.get("description")
            or message.get("hypothesis")
            or raw.get("hypothesis")
        )
        if not headline:
            # No prose means nothing to extract. Synthesizing a headline from an
            # id gave the model a sentence with no information in it, so anything
            # it returned was invention rather than extraction.
            return None

        raw_anomaly = message.get("anomaly_score") if message.get("anomaly_score") is not None else raw.get("anomaly_score")
        anomaly_score = float(raw_anomaly) if raw_anomaly is not None else 0.50
        if anomaly_score < 0.20:
            return None

        # Cheap peek before any expensive preparation. This topic carries the
        # whole pipeline's output -- tens of thousands of events an hour -- and
        # every one of them was paying a Redis read, a Redis write and several
        # context queries only to be shed at the inference call. Peeking here
        # does not claim the slot; the atomic claim still happens in the base
        # class, so two agents cannot both conclude the model is free.
        #
        # It also has to come before mark_processed: marking a message as seen
        # and then shedding it burns its dedup key for an hour, so the same
        # headline is suppressed later when capacity is actually available.
        if not await self._inference_budget.is_available():
            return None

        dedup_key = f"news_intel:{hash(headline)}:{int(time.time() // 3600)}"
        if await self.is_recently_processed(dedup_key, window_seconds=DEDUP_WINDOW_SLOW_SEC):
            return None

        await self.mark_processed(dedup_key, window_seconds=DEDUP_WINDOW_SLOW_SEC)

        logger.info(f"🌐 KNOWLEDGE GRAPH | Processing Graph Triples & Intel | Headline: '{headline[:60]}...'")

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

            # Filter graph triples using whitelist, self-loop guard, and length limit.
            #
            # Matched through is_valid_predicate, which strips and uppercases,
            # rather than by raw membership in VALID_PREDICATES.
            #
            # The strict test required the model to reproduce the constant's
            # exact spelling, so "supplier_to", "Supplier_To" and " SUPPLIER_TO"
            # were each dropped without a word -- and the extraction published
            # graph_triples: [] on every brief in the deployment. is_valid_
            # predicate was imported at the top of this file for this and never
            # called; the case-sensitive comparison was written beside it.
            #
            # The predicate is normalised onto the kept triple too, because the
            # Neo4j MERGE below interpolates it as a relationship type, and
            # "supplier_to" and "SUPPLIER_TO" are two different edges in a graph
            # that is queried by exact type.
            valid_triples = []
            rejected = 0
            for t in brief.graph_triples:
                if not is_valid_predicate(t.predicate):
                    rejected += 1
                    continue
                if t.subject.strip().lower() == t.object.strip().lower():
                    rejected += 1
                    continue
                if len(t.subject) > 80 or len(t.object) > 80:
                    rejected += 1
                    continue
                t.predicate = normalize_predicate(t.predicate)
                valid_triples.append(t)

            # Said out loud. A filter that silently discards everything looks
            # exactly like a model that extracted nothing.
            if rejected:
                logger.info(
                    "Knowledge graph extraction: kept %d of %d triples, %d rejected "
                    "(predicate not in the allowlist, self-loop, or over length).",
                    len(valid_triples), len(brief.graph_triples), rejected,
                )

            # Direct single-transaction Neo4j MERGE for extracted triples
            if valid_triples:
                task = safe_create_task(self._merge_graph_triples(valid_triples))
                if not hasattr(self, "_background_tasks"):
                    self._background_tasks = set()
                self._background_tasks.add(task)
                def _on_done(t):
                    self._background_tasks.discard(t)
                    if not t.cancelled() and t.exception():
                        logger.error(f"Background _merge_graph_triples failed: {t.exception()}")
                task.add_done_callback(_on_done)

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

            # The subject this bulletin is about, from whichever source knows it.
            #
            # The message is asked first because it carries the resolved entity
            # id the rest of the platform keys on; the brief is the fallback,
            # and it is the one that was missing. A bulletin published from a
            # brief about an aircraft carried primary_entity_id=None and
            # ticker=None, so the only thing that named the aircraft was prose
            # inside the summary -- "an aviation alert with a specific aircraft
            # ID", with the ID itself nowhere in the record.
            subject_id = message.get("primary_entity_id") or brief.primary_entity
            subject_name = message.get("primary_entity_name") or brief.primary_entity
            if not subject_name and brief.entities:
                first = brief.entities[0]
                subject_name = getattr(first, "name", None) or str(first)
                subject_id = subject_id or subject_name

            # Named in the text as well as the metadata. A reader should not
            # have to join against another field to learn what the bulletin is
            # about, and a summary that describes the shape of its evidence
            # rather than its content is the generic-output failure the scenario
            # prompts were rewritten to fix.
            headline_text = clip(brief.headline_summary or brief.headline or brief.summary, 80)
            subject_prefix = f"{subject_name} — " if subject_name else ""

            # Publish structured AgentBulletin
            safe_create_task(self.publish_bulletin(
                bulletin_type="alert" if brief.severity >= 4 else "thesis",
                summary=f"Intel ({brief.geopolitical_theater}): {subject_prefix}{headline_text}",
                # Attributed to the entity it is about.
                #
                # This published with primary_entity_id and ticker both null,
                # so the bulletin named no subject -- and the consensus engine
                # fuses bulletins *by entity*. An unattributed bulletin cannot
                # be corroborated by, or contradicted by, anything: it reaches
                # the swarm and then sits outside every comparison the swarm
                # exists to make. Observed live on the first bulletin this
                # system ever produced end to end.
                primary_entity_id=subject_id,
                primary_entity_name=subject_name,
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

        try:
            # Queued rather than dispatched: one budget slot classifies a dozen
            # entities instead of one. Awaiting a single entity reads the same
            # as the direct call it replaced.
            classification = await self._classification_batcher.submit(
                entity_name,
                {"entity_name": entity_name, "context": message.get("context", ""), "message": message},
            )

            # None means the batch reached no verdict for this entity -- shed,
            # timed out, or omitted by a model told to omit rather than guess.
            # It stays unclassified and is retried when next seen. An
            # unclassified entity costs a later inference; a wrongly labelled
            # one propagates into the graph and is far harder to notice.
            if classification is None:
                return None

            # Route through governed ONTOLOGY_PROPOSALS channel (§3.3)
            if self._producer:
                # Not .upper(). Most of what this classifies carries the
                # generic `Entity` label, which canonicalises to UNKNOWN --
                # preserved exactly as given. So upper-casing here is not undone
                # downstream: it put 139,047 `0X...` nodes in the graph beside
                # 6,366 `0x...` ones, the same addresses split in two.
                proposal = {
                    "entity_id": graph_node_id(entity_name, classification.suggested_label),
                    "action": "MERGE_ONTOLOGY_NODE",
                    "data": {
                        "label": classification.suggested_label,
                        "primary_domain": classification.primary_domain,
                        "macro_concepts": classification.macro_concepts,
                        "confidence": classification.confidence,
                    }
                }
                await self._producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=proposal["entity_id"])

            data = classification.model_dump()
            await self.redis.raw.set(cache_key, json.dumps(data), ex=604800)
            return data

        except Exception as e:
            logger.error(f"Entity classification failed for '{entity_name}': {e}")
            return None

    # ── GRAPH TRIPLE MERGING ───────────────────────────────────────────────────

    async def _merge_graph_triples(self, triples: List[GraphTriple]) -> None:
        """Emits governed relationship proposals to Topics.ONTOLOGY_PROPOSALS (§3.3)."""
        if not self._producer or not triples:
            return

        try:
            for t in triples:
                if not is_valid_predicate(t.predicate):
                    continue
                if t.subject.strip().lower() == t.object.strip().lower():
                    continue
                if len(t.subject) > 80 or len(t.object) > 80:
                    continue

                subject_label = getattr(t, 'subject_type', 'Entity')
                object_label = getattr(t, 'object_type', 'Entity')
                proposal = {
                    "entity_id": graph_node_id(t.subject, subject_label),
                    "action": "LINK_ENTITY",
                    "data": {
                        "target_id": graph_node_id(t.object, object_label),
                        "source_label": getattr(t, 'subject_type', 'Entity'),
                        "target_label": getattr(t, 'object_type', 'Entity'),
                        "relation_type": t.predicate,
                        "weight": 1.0,
                        "confidence": t.confidence,
                    }
                }
                await self._producer.send(Topics.ONTOLOGY_PROPOSALS, proposal, key=proposal["entity_id"])
        except Exception as e:
            logger.error(f"Failed to emit graph triples to ONTOLOGY_PROPOSALS: {e}")

    async def get_entity_centrality(self, entity_id: str, label: str = "Entity") -> float:
        """
        Fetches degree centrality for an entity in Neo4j graph.
        Weights anomaly correlation-cluster severity by node centrality.

        The severity weighting this describes is applied by the correlation
        tiering, which had its own inline copy of the query spelled `.upper()`
        -- so it missed every wallet it was asked about and returned a degree of
        zero, a centrality of exactly 1.0, on the boundary between ALERT and
        INTELLIGENCE. Both now resolve through `graph_node_id`, so the readers
        and the writers agree about how an identifier is spelled.
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
            res = await neo4j_client.query(query, {"id": graph_node_id(entity_id, label)})
            if res and res[0].get("degree"):
                degree = float(res[0]["degree"])
                return 1.0 + math.log(1.0 + degree)
        except Exception as e:
            logger.debug(f"Centrality query fallback for {entity_id}: {e}")
        return 1.0
