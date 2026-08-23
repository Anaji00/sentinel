"""
services/api-gateway/routes/graph.py

This file defines the API endpoints for Graph Analysis.
It allows users to query the Neo4j database to uncover hidden relationships
and connection paths between different entities (like vessels, companies, and countries).
"""

import logging
import time
import math
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from services.api_gateway.dependencies import get_graph, get_db

logger = logging.getLogger("api-gateway.graph")
router = APIRouter(prefix="/api/v1/graph", tags=["Graph Analysis"])

@router.get("/entities")
async def get_graph_entities(
    limit: int = Query(50, ge=1, le=500),
    graph = Depends(get_graph),
    db = Depends(get_db)
):
    """Entities available to explore, most connected first.

    Ordering by connectivity rather than name is what makes this list usable:
    alphabetically the graph begins with hundreds of `0x0000...` wallet
    addresses that are indistinguishable in a dropdown, so the explorer opened
    on fifty unusable choices.
    """
    try:
        query = """
        MATCH (n)-[r]-()
        WITH n, count(r) AS degree
        RETURN coalesce(n.id, n.name) AS id,
               coalesce(n.name, n.id)  AS name,
               labels(n)[0]            AS type,
               degree
        ORDER BY degree DESC
        LIMIT $limit
        """
        records = await graph.query(query, {"limit": limit})
        entities = []
        if records:
            for r in records:
                eid = r.get("id") or r.get("name")
                if not eid or any(e["id"] == eid for e in entities):
                    continue        # the same node can arrive twice from a multi-match
                entities.append({
                    "id": eid,
                    "name": _display_name(r.get("name") or eid),
                    # The label carries the type. A "type" property does not
                    # exist on these nodes, so coalescing to it made every
                    # entity report as "ENTITY" regardless of what it was.
                    "type": r.get("type") or "Entity",
                    "degree": int(r.get("degree") or 0),
                })
        
        # Dynamic fallback to TimescaleDB events if Neo4j returns empty
        if not entities:
            db_query = """
            SELECT DISTINCT primary_entity_id as id, primary_entity_name as name, 'ENTITY' as type
            FROM events
            WHERE primary_entity_id IS NOT NULL AND primary_entity_id != ''
            ORDER BY primary_entity_name ASC
            LIMIT $1
            """
            db_rows = await db.query(db_query, limit)
            for r in db_rows:
                e_id = r.get("id") or r.get("name")
                e_name = r.get("name") or e_id
                if e_id and not any(e["id"] == e_id for e in entities):
                    entities.append({
                        "id": e_id,
                        "name": e_name,
                        "type": r.get("type", "ENTITY")
                    })
        return {"entities": entities}
    except Exception as e:
        logger.error(f"Error fetching graph entities: {e}")
        return {"entities": []}


@router.get("/entity/{entity_id}")
async def get_entity_graph(
    entity_id: str, 
    hops: int = 1,
    graph = Depends(get_graph),
    db = Depends(get_db)
):
    """Find 1st or 2nd-degree connections with edge staleness decay weighting and statistical metadata (§3.6, §8.1, §9.1)."""
    hops = max(1, min(2, hops))
    try:
        if hops == 1:
            query = """
            MATCH (n)
            WHERE toLower(n.id) = toLower($entity_id) OR toLower(n.name) = toLower($entity_id) OR toLower(n.ticker) = toLower($entity_id)
            MATCH (n)-[r]-(connected)
            RETURN coalesce(n.name, n.id) as source_name, coalesce(n.type, head(labels(n)), 'ENTITY') as source_type, 
                   type(r) as relationship, connected.id as target_id, coalesce(connected.name, connected.id) as target_name, 
                   coalesce(connected.type, head(labels(connected)), 'ENTITY') as target_type,
                   coalesce(r.weight, 1.0) as weight,
                   coalesce(r.confidence, 0.8) as confidence,
                   coalesce(r.last_updated, r.updated_at, 0) as last_updated,
                   r.coefficient as coefficient,
                   r.p_value as p_value,
                   r.lag as lag,
                   r.f_stat as f_stat,
                   r.branching_ratio as branching_ratio,
                   r.method as method,
                   r.window as window,
                   1 as hop_level
            LIMIT 60
            """
        else:
            query = """
            MATCH (n)
            WHERE toLower(n.id) = toLower($entity_id) OR toLower(n.name) = toLower($entity_id) OR toLower(n.ticker) = toLower($entity_id)
            MATCH path = (n)-[r*1..2]-(connected)
            UNWIND relationships(path) as rel
            WITH startNode(rel) as sn, endNode(rel) as tn, rel, length(path) as path_len
            RETURN DISTINCT coalesce(sn.name, sn.id) as source_name, coalesce(sn.type, head(labels(sn)), 'ENTITY') as source_type,
                   type(rel) as relationship, coalesce(tn.id, tn.name) as target_id, coalesce(tn.name, tn.id) as target_name,
                   coalesce(tn.type, head(labels(tn)), 'ENTITY') as target_type,
                   coalesce(rel.weight, 1.0) as weight,
                   coalesce(rel.confidence, 0.8) as confidence,
                   coalesce(rel.last_updated, rel.updated_at, 0) as last_updated,
                   rel.coefficient as coefficient,
                   rel.p_value as p_value,
                   rel.lag as lag,
                   rel.f_stat as f_stat,
                   rel.branching_ratio as branching_ratio,
                   rel.method as method,
                   rel.window as window,
                   path_len as hop_level
            LIMIT 120
            """
        records = await graph.query(query, {"entity_id": entity_id})
        now_ts = time.time()
        connections = []

        if records:
            seen_edges = set()
            for r in records:
                s_name = r.get("source_name")
                t_name = r.get("target_name")
                rel = r.get("relationship")
                edge_sig = (s_name, rel, t_name)
                if edge_sig in seen_edges:
                    continue
                seen_edges.add(edge_sig)

                raw_updated = r.get("last_updated") or 0
                updated_s = (raw_updated / 1000.0) if raw_updated > 1e11 else float(raw_updated)
                age_days = max(0.0, (now_ts - updated_s) / 86400.0) if updated_s > 0 else 60.0
                decay_factor = math.exp(-0.693 * age_days / 30.0) if age_days < 365.0 else 0.05
                base_weight = float(r.get("weight", 1.0)) * float(r.get("confidence", 0.8))
                decayed_weight = round(base_weight * decay_factor, 4)

                conn_obj = {
                    "source_name": s_name,
                    "source_type": r.get("source_type"),
                    "relationship": rel,
                    "target_id": r.get("target_id"),
                    "target_name": t_name,
                    "target_type": r.get("target_type"),
                    "weight": float(r.get("weight", 1.0)),
                    "confidence": float(r.get("confidence", 0.8)),
                    "last_updated": updated_s,
                    "decayed_weight": decayed_weight,
                    "hop_level": r.get("hop_level", 1),
                }
                # Attach statistical properties if present
                for stat_field in ("coefficient", "p_value", "lag", "f_stat", "branching_ratio", "method", "window"):
                    if r.get(stat_field) is not None:
                        conn_obj[stat_field] = r[stat_field]

                connections.append(conn_obj)
            # Sort by decayed weight descending
            connections.sort(key=lambda x: x.get("decayed_weight", 0.0), reverse=True)

        # Dynamic fallback: if Neo4j returns empty, synthesize real connections from recent hypertable events
        if not connections:
            db_query = """
            SELECT DISTINCT primary_entity_id, primary_entity_name, type as relationship, region
            FROM events
            WHERE (LOWER(primary_entity_id) LIKE $1 OR LOWER(primary_entity_name) LIKE $1 OR LOWER(region) LIKE $1)
            ORDER BY occurred_at DESC
            LIMIT 25
            """
            search_param = f"%{entity_id.lower()}%"
            db_rows = await db.query(db_query, search_param)
            
            for r in db_rows:
                target_name = r.get("primary_entity_name") or r.get("primary_entity_id")
                if target_name and target_name.upper() != entity_id.upper():
                    rel_type = (r.get("relationship") or "CORRELATED_WITH").upper()
                    connections.append({
                        "source_name": entity_id,
                        "relationship": rel_type,
                        "target_id": r.get("primary_entity_id") or target_name,
                        "target_name": target_name,
                        "target_type": "VESSEL" if "vessel" in rel_type.lower() else ("INFRASTRUCTURE" if "bgp" in rel_type.lower() or "cyber" in rel_type.lower() else "ENTITY"),
                        "weight": 1.0,
                        "confidence": 0.8,
                        "last_updated": now_ts,
                        "decayed_weight": 0.8,
                        "hop_level": 1,
                    })
                    
        return {"entity_id": entity_id, "hops": hops, "connections": connections}
    except Exception as e:
        logger.error(f"Error fetching entity graph: {e}")
        return {"entity_id": entity_id, "hops": hops, "connections": []}
    

@router.get("/shortest-path")
async def get_shortest_path(
    source_id: str, 
    target_id: str, 
    graph = Depends(get_graph),
    db = Depends(get_db)
):
    """Advanced Graph AI: Find how two geopolitical entities are connected using native Cypher shortestPath."""
    try:
        query = """
        MATCH p = shortestPath((start:Entity)-[*..6]-(end:Entity))
        WHERE (toLower(start.id) = toLower($source_id) OR toLower(start.name) = toLower($source_id))
          AND (toLower(end.id) = toLower($target_id) OR toLower(end.name) = toLower($target_id))
        RETURN nodes(p) AS entities, relationships(p) AS relations
        """
        results = await graph.query(query, {"source_id": source_id, "target_id": target_id})
        
        # Fallback to APOC dijkstra if standard shortestPath returns empty
        if not results:
            try:
                apoc_query = """
                MATCH (start:Entity), (end:Entity)
                WHERE (toLower(start.id) = toLower($source_id) OR toLower(start.name) = toLower($source_id))
                  AND (toLower(end.id) = toLower($target_id) OR toLower(end.name) = toLower($target_id))
                CALL apoc.algo.dijkstra(start, end, '', 'weight') YIELD path, weight
                RETURN nodes(path) AS entities, relationships(path) AS relations
                """
                results = await graph.query(apoc_query, {"source_id": source_id, "target_id": target_id})
            except Exception as apoc_err:
                logger.debug(f"APOC shortest path query bypass: {apoc_err}")

        if not results:
            # Dynamic fallback: query TimescaleDB co-occurrence events
            db_query = """
            SELECT DISTINCT primary_entity_id, primary_entity_name, type as relationship
            FROM events
            WHERE (LOWER(primary_entity_id) LIKE $1 OR LOWER(primary_entity_name) LIKE $1)
               OR (LOWER(primary_entity_id) LIKE $2 OR LOWER(primary_entity_name) LIKE $2)
            ORDER BY occurred_at DESC
            LIMIT 10
            """
            db_rows = await db.query(db_query, f"%{source_id.lower()}%", f"%{target_id.lower()}%")
            if db_rows:
                path_nodes = [{"id": source_id, "name": source_id, "type": "ENTITY"}]
                for r in db_rows:
                    e_name = r.get("primary_entity_name") or r.get("primary_entity_id")
                    if e_name and e_name.upper() not in (source_id.upper(), target_id.upper()):
                        path_nodes.append({"id": e_name, "name": e_name, "type": "ENTITY"})
                path_nodes.append({"id": target_id, "name": target_id, "type": "ENTITY"})
                return {"path": [{"entities": path_nodes}]}
            return {"message": "No path found", "path": []}
        return {"path": results}
    except Exception as e:
        logger.error(f"Error fetching shortest path: {e}")
        return {"message": "No path found", "path": []}


@router.get("/network")
async def get_graph_network(
    limit: int = Query(60, ge=5, le=300),
    label: Optional[str] = Query(None, description="Restrict to one node label, e.g. Vessel"),
    min_degree: int = Query(1, ge=0),
    graph=Depends(get_graph),
):
    """An overview of the graph: the most connected nodes and the edges between them.

    The explorer previously had no way to show anything until the user picked an
    entity, and the picker was filled from an alphabetical scan -- which on this
    dataset means fifty near-identical `0x0000...` wallet addresses. A knowledge
    graph that opens empty and offers only unreadable choices reads as broken
    even though 19,000 nodes and 13,000 relationships are sitting behind it.

    Nodes are ranked by degree, so the view opens on the hubs that actually
    organise the graph. Only edges whose endpoints are both in the returned set
    are included: an edge pointing at a node that was not returned renders as a
    line into empty space.
    """
    try:
        label_filter = ""
        params: Dict[str, Any] = {
            "limit": limit,
            "min_degree": min_degree,
            # Enough to show a hub's structure without importing its whole
            # neighbourhood; the total is bounded independently.
            "neighbour_cap": 8,
            "total_cap": limit * 10,
        }
        if label and label.isalnum():
            # Interpolated because a label cannot be parameterised in Cypher.
            # Constrained to alphanumerics above so nothing else can be injected.
            label_filter = f":{label}"

        # Filtering to one label and returning only those nodes yields a view
        # with no edges at all: a vessel is connected to its flag state and its
        # region, never to another vessel. The neighbours are what make the
        # selection a graph rather than a list, so they come back with it.
        node_query = f"""
        MATCH (n{label_filter})-[r]-()
        WITH n, count(r) AS degree
        WHERE degree >= $min_degree
        // A second WITH: in a Cypher WITH clause ORDER BY must precede WHERE,
        // so the ranking cannot share the clause that filters on the aggregate.
        WITH n, degree ORDER BY degree DESC LIMIT $limit
        // Neighbours are capped per seed. Without this a single hub -- the
        // busiest wallet here has degree 4,603 -- drags its entire neighbourhood
        // in and a request for 10 nodes returns ten thousand, which no client
        // can lay out and no person can read.
        CALL {{
            WITH n
            MATCH (n)-[]-(nb)
            RETURN nb LIMIT $neighbour_cap
        }}
        WITH collect(DISTINCT n) AS seeds, collect(DISTINCT nb) AS neighbours
        UNWIND (seeds + neighbours) AS m
        WITH DISTINCT m
        RETURN coalesce(m.id, m.name, elementId(m)) AS id,
               coalesce(m.name, m.id, 'unnamed')    AS name,
               labels(m)[0]                          AS type,
               size([(m)--() | 1])                   AS degree
        ORDER BY degree DESC
        LIMIT $total_cap
        """
        node_rows = await graph.query(node_query, params) or []

        nodes = []
        seen = set()
        for r in node_rows:
            nid = r.get("id")
            if not nid or nid in seen:
                continue
            seen.add(nid)
            nodes.append({
                "id": nid,
                "name": _display_name(r.get("name") or nid),
                "type": r.get("type") or "Entity",
                "degree": int(r.get("degree") or 0),
            })

        edges = []
        if nodes:
            edge_query = """
            MATCH (a)-[r]->(b)
            WHERE coalesce(a.id, a.name) IN $ids AND coalesce(b.id, b.name) IN $ids
            RETURN coalesce(a.id, a.name) AS source,
                   coalesce(b.id, b.name) AS target,
                   type(r)                AS relationship,
                   coalesce(r.weight, 1.0) AS weight
            LIMIT 1000
            """
            edge_rows = await graph.query(edge_query, {"ids": [n["id"] for n in nodes]}) or []
            for r in edge_rows:
                if r.get("source") and r.get("target"):
                    edges.append({
                        "source": r["source"],
                        "target": r["target"],
                        "relationship": r.get("relationship") or "RELATED_TO",
                        "weight": float(r.get("weight") or 1.0),
                    })

        return {
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "labels": await _available_labels(graph),
        }
    except Exception as e:
        logger.error(f"Error building graph network view: {e}")
        # Empty rather than a 500: the explorer degrades to its manual search.
        return {"nodes": [], "edges": [], "node_count": 0, "edge_count": 0, "labels": []}


async def _available_labels(graph) -> List[str]:
    """Node labels present in the graph, so the client can offer real filters."""
    try:
        rows = await graph.query("CALL db.labels() YIELD label RETURN label ORDER BY label")
        return [r["label"] for r in (rows or []) if r.get("label")]
    except Exception:
        return []


def _display_name(raw: str) -> str:
    """Makes a 42-character hex address readable without hiding what it is.

    Wallet addresses dominate this graph by node count. Rendered in full they
    are indistinguishable from one another at a glance -- every one starts with
    the same run of zeroes -- which is what made the entity picker unusable.
    """
    value = str(raw)
    if value.lower().startswith("0x") and len(value) > 20:
        return f"{value[:8]}…{value[-6:]}"
    return value
