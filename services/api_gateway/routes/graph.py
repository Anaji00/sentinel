"""
services/api-gateway/routes/graph.py

This file defines the API endpoints for Graph Analysis.
It allows users to query the Neo4j database to uncover hidden relationships
and connection paths between different entities (like vessels, companies, and countries).
"""

import logging
from fastapi import APIRouter, HTTPException, Depends
from services.api_gateway.dependencies import get_graph

logger = logging.getLogger("api-gateway.graph")
router = APIRouter(prefix="/api/v1/graph", tags=["Graph Analysis"])

@router.get("/entity/{entity_id}")
async def get_entity_graph(
    # PATH PARAMETER: FastAPI extracts `entity_id` directly from the URL path (e.g., /entity/12345)
    entity_id: str, 
    # DEPENDENCY INJECTION: Automatically grabs the Neo4j connection pool
    graph = Depends(get_graph)
):
    """Find 1st-degree connections for a specific entity in Neo4j."""
    try:
        # CYPHER QUERY LANGUAGE & NAMED PARAMETERS:
        # In Neo4j, we use Cypher instead of SQL. The `$entity_id` syntax is a 
        # Named Parameter (the equivalent of `%s` in Postgres). 
        # `(n)-[r]-(connected)` translates to: "Find Node 'n', connected by Relationship 'r', to Node 'connected'".
        query = """
        MATCH (n:Entity {id: $entity_id})-[r]-(connected)
        RETURN type(r) as relationship, connected.name as target_name, connected.type as target_type
        LIMIT 50
        """
        connections = await graph.query(query, {"entity_id": entity_id})
        return {"entity_id": entity_id, "connections": connections}
    except Exception as e:
        logger.error(f"Error fetching entity graph: {e}")
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