"""
services/api-gateway/routes/graph.py

This file defines the API endpoints for Graph Analysis.
It allows users to query the Neo4j database to uncover hidden relationships
and connection paths between different entities (like vessels, companies, and countries).
"""

import logging
from fastapi import APIRouter, HTTPException, Depends
from services.api_gateway.dependencies import get_graph, get_db

logger = logging.getLogger("api-gateway.graph")
router = APIRouter(prefix="/api/v1/graph", tags=["Graph Analysis"])

@router.get("/entity/{entity_id}")
async def get_entity_graph(
    entity_id: str, 
    graph = Depends(get_graph),
    db = Depends(get_db)
):
    """Find 1st-degree connections for a specific entity in Neo4j or dynamic TimescaleDB event graph."""
    try:
        query = """
        MATCH (n:Entity)
        WHERE toLower(n.id) = toLower($entity_id) OR toLower(n.name) = toLower($entity_id)
        MATCH (n)-[r]-(connected)
        RETURN n.name as source_name, n.type as source_type, type(r) as relationship, connected.id as target_id, connected.name as target_name, connected.type as target_type
        LIMIT 50
        """
        connections = await graph.query(query, {"entity_id": entity_id})

        # Dynamic fallback: if Neo4j returns empty, synthesize real connections from recent hypertable events
        if not connections:
            db_query = """
            SELECT DISTINCT primary_entity_id, primary_entity_name, type as relationship, region
            FROM events
            WHERE (toLower(primary_entity_id) LIKE $1 OR toLower(primary_entity_name) LIKE $1 OR toLower(region) LIKE $1)
            ORDER BY occurred_at DESC
            LIMIT 12
            """
            search_param = f"%{entity_id.lower()}%"
            db_rows = await db.query(db_query, search_param)
            
            connections = []
            for r in db_rows:
                target_name = r.get("primary_entity_name") or r.get("primary_entity_id")
                if target_name and target_name.upper() != entity_id.upper():
                    rel_type = (r.get("relationship") or "CORRELATED_WITH").upper()
                    connections.append({
                        "source_name": entity_id,
                        "relationship": rel_type,
                        "target_id": r.get("primary_entity_id") or target_name,
                        "target_name": target_name,
                        "target_type": "VESSEL" if "vessel" in rel_type.lower() else ("INFRASTRUCTURE" if "bgp" in rel_type.lower() or "cyber" in rel_type.lower() else "ENTITY")
                    })
                    
        return {"entity_id": entity_id, "connections": connections}
    except Exception as e:
        logger.error(f"Error fetching entity graph: {e}")
        return {"entity_id": entity_id, "connections": []}
    

@router.get("/shortest-path")
async def get_shortest_path(
    source_id: str, 
    target_id: str, 
    graph = Depends(get_graph)
):
    """Advanced Graph AI: Find how two geopolitical entities are connected."""
    try:
        query = """
        MATCH (start:Entity {id: $source_id}), (end:Entity {id: $target_id})
        CALL apoc.algo.dijkstra(start, end, '', 'weight') YIELD path, weight
        RETURN nodes(path) AS entities, relationships(path) AS relations
        """
        results = await graph.query(query, {"source_id": source_id, "target_id": target_id})
        if not results:
            return {"message": "No path found"}
        return {"path": results}
    except Exception as e:
        logger.error(f"Error fetching shortest path: {e}")
        raise HTTPException(status_code=500, detail="Neo4j query failed")
    

@router.get("/shortest-path")
async def get_shortest_path(
    source_id: str, 
    target_id: str, 
    graph = Depends(get_graph)
):
    """Advanced Graph AI: Find how two geopolitical entities are connected."""
    try:
        query = """
        MATCH (start:Entity {id: $source_id}), (end:Entity {id: $target_id})
        CALL apoc.algo.dijkstra(start, end, '', 'weight') YIELD path, weight
        RETURN nodes(path) AS entities, relationships(path) AS relations
        """
        results = await graph.query(query, {"source_id": source_id, "target_id": target_id})
        return results
        if not results:
            return {"message": "No path found"}
        return {"path": results}
    except Exception as e:
        logger.error(f"Error fetching shortest path: {e}")
        raise HTTPException(status_code=500, detail="Neo4j algorithmic query failed")