"""
This file centrally manages API security and 
passes database connections from the main app state 
down into isolated routes"""

import os
import logging
from fastapi import Request, HTTPException, Security
from fastapi.security import APIKeyHeader

logger = logging.getLogger("api-gateway.auth")

API_KEY = os.getenv("API_GATEWAY_KEY", "sentinel-dev-key-2026")
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=True)

async def verify_api_key(request: Request, api_key: str = Security(api_key_header)):
    """Global dependency to lock down all routes."""
    # HOW INJECTION WORKS: FastAPI sees `Security(api_key_header)` and automatically 
    # extracts the 'X-API-KEY' header from the incoming HTTP request. It injects that 
    # string into the `api_key` parameter before this function even runs.
    if api_key != API_KEY:
        logger.warning(f"Failed authentication attempt with key: {api_key[:5]}...")
        raise HTTPException(status_code=403, detail="Could not validate API Key")
    return api_key

def get_db(request: Request):
    """Retrieves TimescaleDB connection from the global app state."""
    # HOW INJECTION WORKS: By requiring `request: Request`, FastAPI automatically 
    # injects the current HTTP request object. We then peek into `request.app.state`, 
    # which acts as a global storage box where we placed our DB connection during startup.
    if not hasattr(request.app.state, "db") or not request.app.state.db:
        raise HTTPException(status_code=503, detail="TimescaleDB not initialized")
    return request.app.state.db

def get_graph(request: Request):
    """Retrieves Neo4j connection from the global app state."""
    # Using `Depends(get_graph)` in a route definition will automatically call this function,
    # verify the connection exists, and hand the Neo4j client directly to the route.
    if not hasattr(request.app.state, "neo4j") or not request.app.state.neo4j:
        raise HTTPException(status_code=503, detail="Neo4j not initialized")
    return request.app.state.neo4j

def get_redis_client(request: Request):
    """Retrieves Redis connection from the global app state."""
    # This pattern prevents routes from creating their own database connections,
    # ensuring the entire application shares one efficient connection pool.
    if not hasattr(request.app.state, "redis") or not request.app.state.redis:
        raise HTTPException(status_code=503, detail="Redis not initialized")
    return request.app.state.redis