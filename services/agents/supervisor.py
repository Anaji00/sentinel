"""
services/agents/supervisor.py

Centralized Governance Supervisor for Knowledge Graph.
Consumes Topics.ONTOLOGY_PROPOSALS and acts as the single authorized gateway to Neo4j.
"""

import asyncio
import json
import logging
import time
import re
from typing import List, Dict, Any, Optional
from services.agents.base import SentinelAgent
from shared.kafka import Topics
from shared.models.events import graph_node_id
from shared.models.ontology import (
    VALID_PREDICATES,
    ALLOWED_NODE_LABELS,
    is_valid_predicate,
    normalize_predicate,
    is_valid_node_label,
)

logger = logging.getLogger("agent.supervisor")


def _as_unit_interval(value, default: float = 1.0) -> float:
    """A confidence or weight on 0-1, whatever scale the producer used.

    SYMPATHY_MOVER edges in the live graph range from -0.1 to 95.0 on both
    properties, because the stock-correlation agent passes a model-generated
    `conviction` straight through and nothing bounded it. Consumers read these
    as probabilities: a 95.0 outranks every genuine edge, and a -0.1 is a
    negative confidence, which is not a quantity.

    Same defect and same remedy as AgentPrediction.conviction -- normalise on
    the shared boundary rather than in the one producer that exposed it, since
    every writer reaching this supervisor has the same freedom to be wrong.
    Percentages are rescaled; anything else is clamped.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number != number:                      # NaN
        return default
    if 1.0 < number <= 100.0:
        number /= 100.0
    return min(1.0, max(0.0, number))


# What an edge says about direction when nobody measured one.
#
# The default was "lead", so every relationship that did not state a direction
# was written to the graph asserting one. PEER_OF is derived from a
# contemporaneous Pearson correlation, which is symmetric and has no lead or
# lag at all -- yet all three live peer edges carried direction="lead", a causal
# claim about instruments whose relationship was only ever measured
# simultaneously. STATISTICALLY_CORRELATED_WITH has the same shape.
#
# A default is not a measurement. Lead and lag are what the Granger path
# establishes, and edges from that path still state it explicitly.
UNDIRECTED = "undirected"


class GraphSupervisor(SentinelAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def output_topic(self) -> str:
        return Topics.ONTOLOGY_UPDATES

    async def handle(self, message: Any) -> Optional[Dict[str, Any]]:
        if isinstance(message, list):
            await self.execute_batch_proposals(message)
            return {"agent": self.name, "action": "batch_commit", "proposals_processed": len(message)}
        elif isinstance(message, dict):
            await self.execute_proposal(message)
            return {"agent": self.name, "action": "single_commit", "entity_id": message.get("entity_id")}
        return None

    async def acquire_lock(self, entity_id: str, timeout: int = 10) -> bool:
        lock_key = f"sentinel:lock:neo4j:{entity_id}"
        end_time = time.time() + timeout
        while time.time() < end_time:
            if await self.redis.raw.set(lock_key, "locked", nx=True, ex=15):
                return True
            await asyncio.sleep(0.05)
        return False

    async def release_lock(self, entity_id: str):
        await self.redis.raw.delete(f"sentinel:lock:neo4j:{entity_id}")

    async def execute_batch_proposals(self, proposals: List[dict]):
        """
        Executes Cypher UNWIND $batch AS row queries to commit graph updates in high-throughput ACID batches.
        Acquires Redis locks for involved entities to prevent race conditions.
        """
        if not proposals:
            return

        # Extract entity IDs for batch locking
        entity_ids = set()
        for p in proposals:
            eid = p.get("entity_id")
            if eid:
                entity_ids.add(eid)
            tid = p.get("data", {}).get("target_id") or p.get("data", {}).get("sympathy_ticker")
            if tid:
                entity_ids.add(tid)

        acquired_locks = []
        for eid in entity_ids:
            if await self.acquire_lock(eid):
                acquired_locks.append(eid)
            else:
                logger.warning(f"Lock timeout for entity {eid} in batch proposal. Continuing with remaining locks.")

        try:
            nodes_by_label: Dict[str, List[dict]] = {}
            links_by_relation: Dict[tuple, List[dict]] = {}

            for p in proposals:
                action = p.get("action")
                entity_id = p.get("entity_id")
                data = p.get("data", {})
                if not entity_id or not action:
                    continue

                if action == "MERGE_ONTOLOGY_NODE":
                    raw_label = data.get("label", "Entity")
                    label = raw_label if is_valid_node_label(raw_label) else "Entity"
                    if label not in nodes_by_label:
                        nodes_by_label[label] = []
                    nodes_by_label[label].append({
                        "name": graph_node_id(entity_id, label),
                        "domain": data.get("primary_domain", "financial"),
                        "concepts": data.get("macro_concepts", []),
                        "sanctions": data.get("sanctions_risk"),
                        "sector": data.get("sector"),
                        "industry": data.get("industry"),
                        "confidence": float(data.get("confidence", 1.0))
                    })

                elif action in ("LINK_ENTITY", "ADD_SYMPATHY_EDGE"):
                    relation = "SYMPATHY_MOVER" if action == "ADD_SYMPATHY_EDGE" else data.get("relation_type", "RELATED_TO").upper()
                    if is_valid_predicate(relation):
                        raw_src = data.get("source_label", "Entity")
                        raw_tgt = data.get("target_label", "Entity")
                        source_label = raw_src if is_valid_node_label(raw_src) else "Entity"
                        target_label = raw_tgt if is_valid_node_label(raw_tgt) else "Entity"
                        
                        target_id = data.get("target_id") or data.get("sympathy_ticker")
                        if not target_id:
                            continue

                        rel_key = (source_label, relation, target_label)
                        if rel_key not in links_by_relation:
                            links_by_relation[rel_key] = []
                        
                        props = data.get("properties", {})
                        links_by_relation[rel_key].append({
                            "id": graph_node_id(entity_id, source_label),
                            "target_id": graph_node_id(target_id, target_label),
                            "weight": _as_unit_interval(data.get("weight", props.get("weight", 1.0))),
                            "confidence": _as_unit_interval(data.get("confidence", props.get("conviction", data.get("conviction", 1.0)))),
                            "relationship": data.get("relationship", props.get("relationship", "")),
                            "direction": data.get("direction", props.get("direction", UNDIRECTED)),
                            "method": props.get("method", ""),
                            "window": props.get("window", ""),
                            "coefficient": float(props.get("coefficient", 0.0)),
                            "p_value": float(props.get("p_value", 0.0)),
                            "lag": int(props.get("lag", 0)),
                            "f_stat": float(props.get("f_stat", 0.0)),
                            "branching_ratio": float(props.get("branching_ratio", 0.0)),
                            "half_life": float(props.get("half_life", 0.0)),
                        })

            # Commit Node batches
            for label, batch in nodes_by_label.items():
                cypher = f"""
                UNWIND $batch AS row
                MERGE (e:{label} {{name: row.name}})
                ON CREATE SET e.id = row.name, e.created_at = timestamp()
                SET e.primary_domain = row.domain,
                    e.macro_concepts = row.concepts,
                    e.sanctions_risk = row.sanctions,
                    e.sector = row.sector,
                    e.industry = row.industry,
                    e.confidence = row.confidence,
                    e.updated_at = timestamp(),
                    e.last_updated = timestamp()
                """
                await self.neo4j.execute(cypher, {"batch": batch})

            # Commit Relationship batches
            for (source_label, relation, target_label), batch in links_by_relation.items():
                cypher = f"""
                UNWIND $batch AS row
                MERGE (a:{source_label} {{name: row.id}})
                ON CREATE SET a.id = row.id, a.created_at = timestamp()
                MERGE (b:{target_label} {{name: row.target_id}})
                ON CREATE SET b.id = row.target_id, b.created_at = timestamp()
                MERGE (a)-[r:{relation}]->(b)
                ON CREATE SET r.created_at = timestamp()
                SET r.weight = row.weight,
                    r.confidence = row.confidence,
                    r.relationship = row.relationship,
                    r.direction = row.direction,
                    r.method = row.method,
                    r.window = row.window,
                    r.coefficient = row.coefficient,
                    r.p_value = row.p_value,
                    r.lag = row.lag,
                    r.f_stat = row.f_stat,
                    r.branching_ratio = row.branching_ratio,
                    r.half_life = row.half_life,
                    r.updated_at = timestamp(),
                    r.last_updated = timestamp()
                """
                await self.neo4j.execute(cypher, {"batch": batch})

            logger.info(f"✅ UNWIND Batch Cypher committed {len(proposals)} graph proposals.")

        except Exception as e:
            logger.error(f"UNWIND batch commit failed: {e}")
            raise
        finally:
            for eid in acquired_locks:
                await self.release_lock(eid)

    async def execute_proposal(self, payload: dict):
        """Maps trusted JSON structs to Cypher queries with centralized validation."""
        entity_id = payload.get("entity_id")
        action = payload.get("action") 
        data = payload.get("data", {})
        
        if not entity_id or not action:
            return

        if not await self.acquire_lock(entity_id):
            logger.warning(f"Lock timeout for entity {entity_id}. Dropping proposal.")
            return

        try:
            if action == "MERGE_ONTOLOGY_NODE":
                raw_label = data.get("label", "Entity")
                label = raw_label if is_valid_node_label(raw_label) else "Entity"

                cypher = f"""
                MERGE (e:{label} {{name: $name}})
                ON CREATE SET e.id = $name, e.created_at = timestamp()
                SET e.primary_domain = $domain,
                    e.macro_concepts = $concepts,
                    e.sanctions_risk = $sanctions,
                    e.sector = $sector,
                    e.industry = $industry,
                    e.confidence = $confidence,
                    e.updated_at = timestamp(),
                    e.last_updated = timestamp()
                """
                await self.neo4j.execute(cypher, {
                    "name": graph_node_id(entity_id, label),
                    "domain": data.get("primary_domain", "financial"),
                    "concepts": data.get("macro_concepts", []),
                    "sanctions": data.get("sanctions_risk"),
                    "sector": data.get("sector"),
                    "industry": data.get("industry"),
                    "confidence": float(data.get("confidence", 1.0))
                })
                logger.debug(f"✅ Created/Updated Node: {entity_id} ({label})")

            elif action in ("LINK_ENTITY", "ADD_SYMPATHY_EDGE"):
                target_id = data.get("target_id") or data.get("sympathy_ticker")
                if not target_id:
                    return

                raw_src = data.get("source_label", "Entity")
                raw_tgt = data.get("target_label", "Entity")
                source_label = raw_src if is_valid_node_label(raw_src) else "Entity"
                target_label = raw_tgt if is_valid_node_label(raw_tgt) else "Entity"

                relation = "SYMPATHY_MOVER" if action == "ADD_SYMPATHY_EDGE" else data.get("relation_type", "RELATED_TO").upper()
                
                if not is_valid_predicate(relation):
                    logger.warning(f"Rejected unauthorized graph predicate: {relation}")
                    return

                props = data.get("properties", {})
                cypher = f"""
                MERGE (a:{source_label} {{name: $id}})
                ON CREATE SET a.id = $id, a.created_at = timestamp()
                MERGE (b:{target_label} {{name: $target_id}})
                ON CREATE SET b.id = $target_id, b.created_at = timestamp()
                MERGE (a)-[r:{relation}]->(b)
                ON CREATE SET r.created_at = timestamp()
                SET r.weight = $weight,
                    r.confidence = $confidence,
                    r.relationship = $relationship,
                    r.direction = $direction,
                    r.method = $method,
                    r.window = $window,
                    r.coefficient = $coefficient,
                    r.p_value = $p_value,
                    r.lag = $lag,
                    r.f_stat = $f_stat,
                    r.branching_ratio = $branching_ratio,
                    r.half_life = $half_life,
                    r.updated_at = timestamp(),
                    r.last_updated = timestamp()
                """
                await self.neo4j.execute(cypher, {
                    "id": graph_node_id(entity_id, source_label),
                    "target_id": graph_node_id(target_id, target_label),
                    "weight": _as_unit_interval(data.get("weight", props.get("weight", 1.0))),
                    "confidence": _as_unit_interval(data.get("confidence", props.get("conviction", data.get("conviction", 1.0)))),
                    "relationship": str(data.get("relationship", props.get("relationship", ""))),
                    "direction": str(data.get("direction", props.get("direction", UNDIRECTED))),
                    "method": str(props.get("method", "")),
                    "window": str(props.get("window", "")),
                    "coefficient": float(props.get("coefficient", 0.0)),
                    "p_value": float(props.get("p_value", 0.0)),
                    "lag": int(props.get("lag", 0)),
                    "f_stat": float(props.get("f_stat", 0.0)),
                    "branching_ratio": float(props.get("branching_ratio", 0.0)),
                    "half_life": float(props.get("half_life", 0.0)),
                })
                logger.debug(f"✅ Created/Updated Edge: {entity_id} -[{relation}]-> {target_id}")

            elif action == "ADD_TAGS":
                tags = data.get("tags", [])
                raw_label = data.get("label", "Entity")
                label = raw_label if is_valid_node_label(raw_label) else "Entity"
                if not tags:
                    return

                cypher = f"""
                MERGE (e:{label} {{name: $id}})
                ON CREATE SET e.id = $id, e.created_at = timestamp()
                WITH e, coalesce(e.tags, []) + $new_tags AS all_tags
                UNWIND all_tags AS tag
                WITH e, collect(distinct tag) AS unique_tags
                SET e.tags = unique_tags,
                    e.updated_at = timestamp(),
                    e.last_updated = timestamp()
                """
                # `label`, not `source_label`. This branch has no source/target
                # pair -- it tags a single node -- and the name it does bind is
                # the one the MERGE above already uses. `source_label` is bound
                # only in the two link-handling branches, so this raised
                # UnboundLocalError every time a tag proposal arrived, inside a
                # handler that reported it as a Neo4j commit failure.
                await self.neo4j.execute(cypher, {"id": graph_node_id(entity_id, label), "new_tags": tags})
                logger.debug(f"✅ Added {len(tags)} tags to {entity_id}")

            else:
                logger.warning(f"Unknown proposal action: {action}")

        except Exception as e:
            logger.error(f"Neo4j commit failed for {entity_id}: {e}")
            raise
        finally:
            await self.release_lock(entity_id)


async def start_supervisor():
    logger.info("🛡️ Graph Supervisor Online. Protecting Neo4j state.")
    from shared.db import get_redis, get_neo4j, get_timescale
    from shared.kafka import SentinelProducer, SentinelConsumer

    redis_client = await get_redis()
    db_client = await get_timescale()
    neo4j_client = await get_neo4j()

    producer = SentinelProducer()
    dlq = SentinelProducer()
    consumer = SentinelConsumer(
        topics=[Topics.ONTOLOGY_PROPOSALS],
        group_id="supervisor-group",
        auto_offset_reset="latest",
    )

    supervisor = GraphSupervisor(
        agent_name="supervisor",
        input_topics=[Topics.ONTOLOGY_PROPOSALS],
        redis_client=redis_client,
        db_client=db_client,
        neo4j_client=neo4j_client,
        producer=producer,
        consumer=consumer,
        dlq=dlq,
    )
    await supervisor.run()


if __name__ == "__main__":
    asyncio.run(start_supervisor())