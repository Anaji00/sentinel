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

import time

API_KEY = resolve_env_var("API_GATEWAY_KEY", "sentinel-dev-key-2026", warn_on_fallback=True)

async def check_rate_limit(redis_client, identity_key: str, max_tokens: int = 120, refill_rate_per_sec: float = 10.0) -> bool:
    """Redis-backed token bucket rate limiter per key/session/IP."""
    if not redis_client:
        return True
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        now = time.time()
        bucket_key = f"sentinel:ratelimit:{identity_key}"

        pipe = raw_redis.pipeline()
        pipe.get(f"{bucket_key}:tokens")
        pipe.get(f"{bucket_key}:last_refill")
        res = await pipe.execute()

        curr_tokens = float(res[0]) if res[0] is not None else float(max_tokens)
        last_refill = float(res[1]) if res[1] is not None else now

        delta = max(0.0, now - last_refill)
        refilled_tokens = min(float(max_tokens), curr_tokens + delta * refill_rate_per_sec)

        if refilled_tokens >= 1.0:
            new_tokens = refilled_tokens - 1.0
            p = raw_redis.pipeline()
            p.set(f"{bucket_key}:tokens", str(new_tokens), ex=3600)
            p.set(f"{bucket_key}:last_refill", str(now), ex=3600)
            await p.execute()
            return True
        else:
            return False
    except Exception as e:
        logger.debug(f"Rate limit check warning: {e}")
        return True

async def verify_api_key(request: Request = None):
    """Global dependency to lock down HTTP routes via API Key or httpOnly session cookie.
    
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

    session_cookie = request.cookies.get("sentinel_session") if hasattr(request, "cookies") else None
    api_key = (request.headers.get("X-API-KEY") if hasattr(request, "headers") else None) or (request.query_params.get("api_key") if hasattr(request, "query_params") else None)

    is_valid = False
    identity = "anonymous"

    if api_key:
        if hmac.compare_digest(api_key.encode("utf-8"), API_KEY.encode("utf-8")):
            is_valid = True
            identity = f"apikey:{api_key[:8]}"
    elif session_cookie:
        # Validated server-side via Next.js proxy or cookie presence
        is_valid = True
        identity = f"cookie:{session_cookie[:16]}"
    elif os.getenv("ENVIRONMENT") == "development" or os.getenv("NODE_ENV") == "development":
        # Dev fallback for local developer ergonomics
        is_valid = True
        identity = "dev-client"

    if not is_valid:
        logger.warning(f"Failed authentication attempt for path {path}: Invalid or missing credentials.")
        raise HTTPException(status_code=403, detail="Could not validate API Key or Session Cookie")

    # Rate limiting check
    redis = getattr(request.app.state, "redis", None) if (hasattr(request, "app") and hasattr(request.app, "state")) else None
    if redis:
        allowed = await check_rate_limit(redis, identity)
        if not allowed:
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Please slow down requests.")

    return identity


async def verify_websocket_api_key(websocket: WebSocket) -> bool:
    """Validate API key or session cookie on a WebSocket handshake BEFORE calling accept()."""
    cookies_map = getattr(websocket, "cookies", None)
    session_cookie = cookies_map.get("sentinel_session") if isinstance(cookies_map, dict) else None

    headers_map = getattr(websocket, "headers", None)
    query_params_map = getattr(websocket, "query_params", None)

    api_key_header = headers_map.get("X-API-KEY") if isinstance(headers_map, dict) else None
    api_key_query = query_params_map.get("api_key") if isinstance(query_params_map, dict) else None

    api_key = api_key_header or api_key_query

    is_valid = False
    if api_key and isinstance(api_key, str):
        if hmac.compare_digest(api_key.encode("utf-8"), API_KEY.encode("utf-8")):
            is_valid = True
    elif session_cookie and isinstance(session_cookie, str):
        is_valid = True
    elif os.getenv("ENVIRONMENT") in ("development", "dev") or os.getenv("NODE_ENV") in ("development", "dev"):
        is_valid = True

    if not is_valid:
        await websocket.close(code=4003, reason="Invalid API key or session cookie")
        logger.warning("WebSocket rejected: invalid credentials.")
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