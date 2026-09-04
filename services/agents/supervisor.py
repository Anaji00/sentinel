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
import uuid
from typing import List, Dict, Any, Optional
from services.agents.base import SentinelAgent
from shared.kafka import Topics
from shared.models.events import graph_node_id
from shared.utils.tasks import safe_create_task
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


# How long a graph write may hold an entity before the lease lapses.
#
# Named rather than inlined because release_lock reports against it: when a
# batch outruns this, the log line needs to say what it outran.
LOCK_TTL_SEC: int = 15


def _describe_proposals(proposals: List[dict]) -> str:
    """What these proposals changed in the graph, in words.

    Names the entities and relationships written rather than counting the write
    itself, so a reader downstream can tell what the supervisor did without
    re-deriving it from the payload.
    """
    if not proposals:
        return "No graph changes committed."

    nodes: List[str] = []
    links: List[str] = []
    tagged: List[str] = []
    for p in proposals:
        if not isinstance(p, dict):
            continue
        action = str(p.get("action") or "")
        entity_id = str(p.get("entity_id") or "?")
        data = p.get("data") if isinstance(p.get("data"), dict) else {}
        target = data.get("target_id") or data.get("sympathy_ticker")
        if action == "MERGE_ONTOLOGY_NODE":
            nodes.append(f"{entity_id} ({data.get('label', 'Entity')})")
        elif target:
            predicate = data.get("predicate") or data.get("relationship") or "RELATED_TO"
            links.append(f"{entity_id} -[{predicate}]-> {target}")
        elif data.get("tags"):
            tagged.append(entity_id)
        else:
            nodes.append(entity_id)

    parts: List[str] = []
    if nodes:
        parts.append(f"Entities merged: {', '.join(nodes[:5])}" + (f" (+{len(nodes) - 5} more)" if len(nodes) > 5 else ""))
    if links:
        parts.append(f"Relationships written: {'; '.join(links[:5])}" + (f" (+{len(links) - 5} more)" if len(links) > 5 else ""))
    if tagged:
        parts.append(f"Entities tagged: {', '.join(tagged[:5])}" + (f" (+{len(tagged) - 5} more)" if len(tagged) > 5 else ""))
    return " | ".join(parts) if parts else "No graph changes committed."


class GraphSupervisor(SentinelAgent):

    async def on_start(self) -> None:
        """One-time repairs this service is responsible for.

        Runs from the base agent's start hook, which is the path GraphSupervisor
        actually takes -- build_agent constructs it like every other agent, and
        start_supervisor() below is only reachable from __main__.
        """
        await backfill_node_types(self.neo4j, self.redis)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def output_topic(self) -> str:
        return Topics.ONTOLOGY_UPDATES

    async def handle(self, message: Any) -> Optional[Dict[str, Any]]:
        """Commits a graph proposal and reports what was committed.

        The return value used to be a receipt -- {"action": "single_commit",
        "entity_id": ...} -- which is bookkeeping about the write rather than a
        statement of what changed in the graph. That shape reached an operator's
        screen labelled "Intelligence Brief Synthesized" with an anomaly score
        beside it, because a reader downstream had no way to tell a commit
        acknowledgement from an analytical result. The live feed filters those
        now, so the symptom is gone; this is the cause.
        """
        if isinstance(message, list):
            await self.execute_batch_proposals(message)
            return {
                "agent": self.name,
                "action": "batch_commit",
                "proposals_processed": len(message),
                "summary": _describe_proposals(message),
            }
        elif isinstance(message, dict):
            await self.execute_proposal(message)
            return {
                "agent": self.name,
                "action": "single_commit",
                "entity_id": message.get("entity_id"),
                "summary": _describe_proposals([message]),
            }
        return None

    async def acquire_lock(self, entity_id: str, timeout: int = 10) -> Optional[str]:
        """Takes the entity's write lock, returning the token that owns it.

        The token is what makes the release safe. Every holder used to write the
        literal string "locked", so a lock was indistinguishable from any other
        lock on the same key -- see release_lock for what that cost.
        """
        lock_key = f"sentinel:lock:neo4j:{entity_id}"
        token = uuid.uuid4().hex
        end_time = time.time() + timeout
        while time.time() < end_time:
            if await self.redis.raw.set(lock_key, token, nx=True, ex=LOCK_TTL_SEC):
                return token
            await asyncio.sleep(0.05)
        return None

    async def release_lock(self, entity_id: str, token: Optional[str] = None) -> bool:
        """Releases the lock only if this caller still holds it.

        The unconditional DELETE this replaces could free a lock the caller no
        longer owned. The sequence is ordinary rather than exotic: a Neo4j batch
        runs past the 15-second TTL, the lock expires, the next waiter acquires
        it, and then the first batch reaches its `finally` and deletes the new
        holder's lock. Two writers are then inside a section built for one, and
        nothing anywhere reports it.

        Compare-and-delete has to be atomic, or it reintroduces the same race in
        the gap between the GET and the DELETE.
        """
        lock_key = f"sentinel:lock:neo4j:{entity_id}"
        if not token:
            # No token means the caller never successfully acquired. Deleting on
            # its way out would be exactly the bug this guards against.
            return False
        try:
            released = await self.redis.raw.eval(
                "if redis.call('get', KEYS[1]) == ARGV[1] then "
                "return redis.call('del', KEYS[1]) else return 0 end",
                1, lock_key, token,
            )
            if not released:
                # The TTL lapsed and someone else holds it now. Worth saying:
                # it means a graph write ran longer than the lease it held.
                logger.warning(
                    "Lock for %s had already been taken over before release; "
                    "the batch outran its %ss lease.", entity_id, LOCK_TTL_SEC,
                )
            return bool(released)
        except Exception as e:
            logger.warning("Lock release failed for %s: %s", entity_id, e)
            return False

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

        acquired_locks: Dict[str, str] = {}
        for eid in entity_ids:
            token = await self.acquire_lock(eid)
            if token:
                acquired_locks[eid] = token
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

            # Every node carries its own type.
            #
            # 236,989 of 237,018 Entity nodes had type = null -- 99.99%. Twenty
            # nine were typed, all EQUITY. The label was being set correctly as
            # a Neo4j label, and the `type` property, which is what every
            # consumer reads and what the ontology model declares, was never
            # written at all.
            #
            # The relationship write below is the larger source: it MERGEs its
            # own endpoints, so every edge created a bare node carrying nothing
            # but an id and a timestamp. A graph of a quarter of a million
            # untyped nodes cannot answer "show me the companies" or "show me
            # the wallets", which is most of what a knowledge graph is for.
            # Commit Node batches
            for label, batch in nodes_by_label.items():
                cypher = f"""
                UNWIND $batch AS row
                MERGE (e:{label} {{name: row.name}})
                ON CREATE SET e.id = row.name, e.created_at = timestamp()
                SET e.type = '{label}',
                    e.primary_domain = row.domain,
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
                ON CREATE SET a.id = row.id, a.created_at = timestamp(), a.type = '{source_label}'
                MERGE (b:{target_label} {{name: row.target_id}})
                ON CREATE SET b.id = row.target_id, b.created_at = timestamp(), b.type = '{target_label}'
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
            for eid, token in acquired_locks.items():
                await self.release_lock(eid, token)

    async def execute_proposal(self, payload: dict):
        """Maps trusted JSON structs to Cypher queries with centralized validation."""
        entity_id = payload.get("entity_id")
        action = payload.get("action") 
        data = payload.get("data", {})
        
        if not entity_id or not action:
            return

        lock_token = await self.acquire_lock(entity_id)
        if not lock_token:
            logger.warning(f"Lock timeout for entity {entity_id}. Dropping proposal.")
            return

        try:
            if action == "MERGE_ONTOLOGY_NODE":
                raw_label = data.get("label", "Entity")
                label = raw_label if is_valid_node_label(raw_label) else "Entity"

                cypher = f"""
                MERGE (e:{label} {{name: $name}})
                ON CREATE SET e.id = $name, e.created_at = timestamp()
                SET e.type = '{label}'
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
                ON CREATE SET a.id = $id, a.created_at = timestamp(), a.type = '{source_label}'
                MERGE (b:{target_label} {{name: $target_id}})
                ON CREATE SET b.id = $target_id, b.created_at = timestamp(), b.type = '{target_label}'
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
                SET e.type = coalesce(e.type, '{label}')
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
            await self.release_lock(entity_id, lock_token)


# One-time backfill of node types, run once per deployment.
#
# Typing was added to every write path in this audit, and it is forward-only:
# 533 of 238,388 nodes carried a `type` after the deploy, because the fix types
# what it writes and nothing had ever revisited what was already there. A graph
# that cannot answer "show me the companies" is not fixed by promising that
# future companies will be answerable.
#
# Two populations, handled differently:
#
#   ~36,000 nodes carry a meaningful label already -- Aircraft, Vessel,
#   AutonomousSystem, Company, Prefix -- and their type is simply that label.
#   This is free and exact.
#
#   ~237,855 carry only the generic `Entity` label, so the label says nothing.
#   Their kind is inferred from the shape of the identifier, which is the same
#   rule graph_node_id already applies when writing: a 0x-prefixed 40-hex string
#   is a wallet, and nothing else is guessed. An identifier that fits no known
#   shape is left untyped rather than labelled with a guess -- an untyped node
#   is a known gap and a wrongly-typed one is a silent error.
BACKFILL_MARKER_KEY = "sentinel:graph:type_backfill_version"
BACKFILL_VERSION = "1"

# Batched so a quarter-million-node update cannot hold a single transaction open
# long enough to stall the writes this service exists to perform.
BACKFILL_BATCH = 5000


async def backfill_node_types(neo4j_client, redis_client) -> None:
    """Types the nodes that predate the typing fix. Idempotent; runs once."""
    if not neo4j_client:
        return
    try:
        if redis_client:
            seen = await redis_client.raw.get(BACKFILL_MARKER_KEY)
            if seen and str(seen.decode() if isinstance(seen, bytes) else seen) == BACKFILL_VERSION:
                return
    except Exception:
        pass

    labelled_total = 0
    try:
        # 1. Nodes whose label already says what they are.
        for label in ("Aircraft", "Vessel", "Company", "AutonomousSystem", "Prefix", "Wallet"):
            while True:
                res = await neo4j_client.query(
                    f"""
                    MATCH (n:{label}) WHERE n.type IS NULL
                    WITH n LIMIT {BACKFILL_BATCH}
                    SET n.type = '{label}'
                    RETURN count(n) AS n
                    """
                )
                done = int((res or [{}])[0].get("n") or 0)
                labelled_total += done
                if done < BACKFILL_BATCH:
                    break

        # 2. Generic Entity nodes whose identifier shape is recognisable.
        #    Only the wallet shape is asserted; everything else stays untyped.
        wallet_total = 0
        while True:
            res = await neo4j_client.query(
                f"""
                MATCH (n:Entity)
                WHERE n.type IS NULL AND n.id =~ '(?i)^0x[0-9a-f]{{40}}$'
                WITH n LIMIT {BACKFILL_BATCH}
                SET n.type = 'Wallet'
                RETURN count(n) AS n
                """
            )
            done = int((res or [{}])[0].get("n") or 0)
            wallet_total += done
            if done < BACKFILL_BATCH:
                break

        logger.info(
            "Graph type backfill complete: %d node(s) typed from their label, "
            "%d wallet(s) typed from their identifier shape. Nodes whose kind "
            "cannot be established are left untyped rather than guessed.",
            labelled_total, wallet_total,
        )
        if redis_client:
            await redis_client.raw.set(BACKFILL_MARKER_KEY, BACKFILL_VERSION)
    except Exception as e:
        # A backfill that fails must not stop the supervisor: it is a repair of
        # history, and history keeps.
        logger.warning("Graph type backfill did not complete: %s", e)


# NOT THE LIVE PATH. No compose service runs this file.
#
# GraphSupervisor is constructed through build_agent in services/agents/main.py
# like every other agent; this standalone runner exists for local use. A graph
# type backfill was placed in here during the September audit and never ran --
# it had a caller, and the caller had no runtime. Anything that must execute in
# the deployment belongs on the class, reachable from the agent lifecycle:
# on_start() for one-shot work, or a loop started in run().
#
# scripts/check_reachability.py flags this file for exactly this reason.
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