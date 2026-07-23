"""
This file centrally manages API security and 
passes database connections from the main app state 
down into isolated routes"""

import hmac
import os
import logging
from typing import Optional
from fastapi import Request, WebSocket, HTTPException, Security
from fastapi.security import APIKeyHeader

logger = logging.getLogger("api-gateway.auth")

API_KEY = os.getenv("API_GATEWAY_KEY")
if not API_KEY:
    env_name = os.getenv("SENTINEL_ENV", "dev").lower()
    if env_name in ("prod", "production", "staging"):
        raise RuntimeError("CRITICAL SECURITY FAILURE: API_GATEWAY_KEY is not set in environment.")
    logger.warning("SECURITY WARNING: API_GATEWAY_KEY not set. Falling back to default dev key.")
    API_KEY = "sentinel-dev-key-2026"

async def verify_api_key(request: Request = None):
    """Global dependency to lock down all HTTP routes while permitting WebSocket handshakes."""
    if request is None:
        return None
    if hasattr(request, "scope") and request.scope.get("type") == "websocket":
        return None
    if hasattr(request, "method") and request.method == "OPTIONS":
        return None
    api_key = (request.headers.get("X-API-KEY") if hasattr(request, "headers") else None) or (request.query_params.get("api_key") if hasattr(request, "query_params") else None)
    if not api_key:
        raise HTTPException(status_code=403, detail="X-API-KEY header missing")
    if not hmac.compare_digest(api_key.encode("utf-8"), API_KEY.encode("utf-8")):
        logger.warning("Failed authentication attempt: Invalid X-API-KEY header.")
        raise HTTPException(status_code=403, detail="Could not validate API Key")
    return api_key

def get_db(request: Request = None):
    """Retrieves TimescaleDB connection from the global app state."""
    if request is None or not hasattr(request.app.state, "db") or not request.app.state.db:
        raise HTTPException(status_code=503, detail="TimescaleDB not initialized")
    return request.app.state.db

def get_graph(request: Request = None):
    """Retrieves Neo4j connection from the global app state."""
    if request is None or not hasattr(request.app.state, "neo4j") or not request.app.state.neo4j:
        raise HTTPException(status_code=503, detail="Neo4j not initialized")
    return request.app.state.neo4j

def get_redis_client(request: Request = None):
    """Retrieves Redis connection from the global app state."""
    if request is None or not hasattr(request.app.state, "redis") or not request.app.state.redis:
        raise HTTPException(status_code=503, detail="Redis not initialized")
    return request.app.state.redis