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
from shared.utils.env_guard import resolve_env_var

logger = logging.getLogger("api-gateway.auth")

API_KEY = resolve_env_var("API_GATEWAY_KEY", "sentinel-dev-key-2026", warn_on_fallback=True)

async def verify_api_key(request: Request = None):
    """Global dependency to lock down all HTTP routes.
    
    WebSocket connections are NOT validated here — they must use
    verify_websocket_api_key() before calling websocket.accept().
    """
    if request is None:
        return None
    if hasattr(request, "scope") and request.scope.get("type") == "websocket":
        return None
    if hasattr(request, "method") and request.method == "OPTIONS":
        return None
    path = getattr(getattr(request, "url", None), "path", "")
    if path in ("/metrics", "/metrics/json", "/health") or path.startswith("/api/v1/health"):
        return None
    api_key = (request.headers.get("X-API-KEY") if hasattr(request, "headers") else None) or (request.query_params.get("api_key") if hasattr(request, "query_params") else None)
    if not api_key:
        raise HTTPException(status_code=403, detail="X-API-KEY header missing")
    if not hmac.compare_digest(api_key.encode("utf-8"), API_KEY.encode("utf-8")):
        logger.warning("Failed authentication attempt: Invalid X-API-KEY header.")
        raise HTTPException(status_code=403, detail="Could not validate API Key")
    return api_key


async def verify_websocket_api_key(websocket: WebSocket) -> bool:
    """Validate API key on a WebSocket handshake BEFORE calling accept().

    Checks the ``X-API-KEY`` header first, then falls back to the
    ``api_key`` query parameter.  Returns True on success.  On failure,
    closes the socket with status 4003 and returns False — the caller
    must ``return`` immediately.
    """
    api_key = (
        websocket.headers.get("X-API-KEY")
        or websocket.query_params.get("api_key")
    )
    if not api_key:
        await websocket.close(code=4003, reason="X-API-KEY header or api_key query param required")
        logger.warning("WebSocket rejected: no API key provided.")
        return False
    if not hmac.compare_digest(api_key.encode("utf-8"), API_KEY.encode("utf-8")):
        await websocket.close(code=4003, reason="Invalid API key")
        logger.warning("WebSocket rejected: invalid API key.")
        return False
    return True


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