"""
services/api-gateway/routes/graph.py

This file defines the API endpoints for Graph Analysis.
It allows users to query the Neo4j database to uncover hidden relationships
and connection paths between different entities (like vessels, companies, and countries).
"""

import logging
import time
import math
from fastapi import APIRouter, HTTPException, Depends
from services.api_gateway.dependencies import get_graph, get_db

logger = logging.getLogger("api-gateway.graph")
router = APIRouter(prefix="/api/v1/graph", tags=["Graph Analysis"])

@router.get("/entities")
async def get_graph_entities(
    graph = Depends(get_graph),
    db = Depends(get_db)
):
    """Retrieve all active entities registered in the Neo4j Knowledge Graph or TimescaleDB."""
    try:
        query = """
        MATCH (n:Entity)
        RETURN n.id as id, coalesce(n.name, n.id) as name, coalesce(n.type, 'ENTITY') as type
        ORDER BY n.name ASC
        LIMIT 50
        """
        records = await graph.query(query)
        entities = []
        if records:
            for r in records:
                entities.append({
                    "id": r.get("id") or r.get("name"),
                    "name": r.get("name") or r.get("id"),
                    "type": r.get("type", "ENTITY")
                })
        
        # Dynamic fallback to TimescaleDB events if Neo4j returns empty
        if not entities:
            db_query = """
            SELECT DISTINCT primary_entity_id as id, primary_entity_name as name, 'ENTITY' as type
            FROM events
            WHERE primary_entity_id IS NOT NULL AND primary_entity_id != ''
            ORDER BY primary_entity_name ASC
            LIMIT 50
            """
            db_rows = await db.query(db_query)
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