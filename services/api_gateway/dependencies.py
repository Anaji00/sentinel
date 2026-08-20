"""
This file centrally manages API security and 
passes database connections from the main app state 
down into isolated routes"""

import hmac
import hashlib
import base64
import json
import os
import logging
import time
from typing import Optional, Tuple
from fastapi import Request, WebSocket, HTTPException, Security
from fastapi.security import APIKeyHeader
from shared.utils.env_guard import resolve_env_var

from shared.utils.secrets import get_secret
from shared.utils.rbac import Role, parse_role

logger = logging.getLogger("api-gateway.auth")

API_KEY = get_secret("API_GATEWAY_KEY", default=os.getenv("API_KEY") or os.getenv("SENTINEL_API_KEY") or "")
SESSION_SECRET = get_secret("SESSION_SECRET", default=os.getenv("JWT_SECRET") or os.getenv("API_GATEWAY_KEY") or "sentinel-secure-session-secret-key-2026")


def create_jwt_token(payload: dict, secret: Optional[str] = None, expires_in_seconds: int = 86400, role: str = "ANALYST") -> str:
    """
    Creates an RFC 7519 compliant HS256 JWT string.
    """
    secret_key = secret or get_secret("SESSION_SECRET", default=SESSION_SECRET)
    secret_bytes = secret_key.encode("utf-8")

    now = time.time()
    jwt_payload = dict(payload)
    if "iat" not in jwt_payload:
        jwt_payload["iat"] = int(now)
    if "exp" not in jwt_payload:
        jwt_payload["exp"] = int(now + expires_in_seconds)
    if "role" not in jwt_payload:
        jwt_payload["role"] = role

    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = base64.urlsafe_b64encode(json.dumps(header, separators=(',', ':')).encode("utf-8")).rstrip(b"=").decode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(json.dumps(jwt_payload, separators=(',', ':')).encode("utf-8")).rstrip(b"=").decode("utf-8")

    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = base64.urlsafe_b64encode(
        hmac.new(secret_bytes, signing_input, hashlib.sha256).digest()
    ).rstrip(b"=").decode("utf-8")

    return f"{header_b64}.{payload_b64}.{signature}"


def verify_session_token(token: str, secret: Optional[str] = None) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Cryptographically verifies a session token independently of upstream proxies.
    Supports both:
      1. Standard 3-part HS256 JWTs (header.payload.signature)
      2. 2-part HMAC-signed session tokens (base64url(payload).signature) from Next.js auth
    
    Performs constant-time signature verification and validates expiration timestamp.
    Returns: (is_valid, user_identifier, role_name)
    """
    if not token or not isinstance(token, str):
        return False, None, None

    secret_key = secret or get_secret("SESSION_SECRET", default=SESSION_SECRET)
    secret_bytes = secret_key.encode("utf-8")
    parts = token.strip().split(".")

    # 1. Standard 3-part JWT (header.payload.signature)
    if len(parts) == 3:
        header_b64, payload_b64, signature = parts
        signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")

        expected_sig_bytes = hmac.new(secret_bytes, signing_input, hashlib.sha256).digest()
        expected_sig_b64 = base64.urlsafe_b64encode(expected_sig_bytes).rstrip(b"=").decode("utf-8")
        expected_sig_hex = hmac.new(secret_bytes, signing_input, hashlib.sha256).hexdigest()

        if not (hmac.compare_digest(signature, expected_sig_b64) or hmac.compare_digest(signature, expected_sig_hex)):
            return False, None, None

        try:
            rem = len(payload_b64) % 4
            padded = payload_b64 + ("=" * (4 - rem) if rem else "")
            payload_json = json.loads(base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8"))

            exp = payload_json.get("exp")
            if exp is not None:
                exp_ts = float(exp)
                if exp_ts > 1e11:  # Epoch milliseconds
                    exp_ts /= 1000.0
                if time.time() > exp_ts:
                    return False, None, None

            sub = payload_json.get("sub") or payload_json.get("email") or payload_json.get("user") or "authenticated_user"
            role = payload_json.get("role") or payload_json.get("roles") or "ANALYST"
            if isinstance(role, list) and role:
                role = role[0]
            return True, str(sub), str(role)
        except Exception:
            return False, None, None

    # 2. 2-part HMAC-signed session (payload.signature)
    elif len(parts) == 2:
        encoded_payload, signature = parts
        try:
            rem = len(encoded_payload) % 4
            padded = encoded_payload + ("=" * (4 - rem) if rem else "")
            payload_str = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")

            role = "ANALYST"
            if ":" in payload_str:
                colon_parts = payload_str.split(":")
                if len(colon_parts) >= 3:
                    email, role, expires_str = colon_parts[0], colon_parts[1], colon_parts[2]
                else:
                    email, expires_str = colon_parts[0], colon_parts[1]
                expires_at = float(expires_str)
                if expires_at > 1e11:
                    expires_at /= 1000.0
                if time.time() > expires_at:
                    return False, None, None
            else:
                email = payload_str

            expected_sig_hex = hmac.new(secret_bytes, payload_str.encode("utf-8"), hashlib.sha256).hexdigest()
            expected_sig_b64 = base64.urlsafe_b64encode(
                hmac.new(secret_bytes, payload_str.encode("utf-8"), hashlib.sha256).digest()
            ).rstrip(b"=").decode("utf-8")

            if not (hmac.compare_digest(signature, expected_sig_hex) or hmac.compare_digest(signature, expected_sig_b64)):
                return False, None, None

            return True, email or "session_user", role
        except Exception:
            return False, None, None

    return False, None, None


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
    """Global dependency to lock down HTTP routes via API Key or cryptographically signed session cookie.
    
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
    has_qs = hasattr(request, "scope") and isinstance(request.scope, dict) and "query_string" in request.scope
    api_key_query = request.query_params.get("api_key") if has_qs else None
    api_key = (request.headers.get("X-API-KEY") if hasattr(request, "headers") else None) or api_key_query

    is_valid = False
    identity = "anonymous"
    user_role = Role.VIEWER

    if api_key and API_KEY:
        if hmac.compare_digest(api_key.encode("utf-8"), API_KEY.encode("utf-8")):
            is_valid = True
            identity = f"apikey:{api_key[:8]}"
            user_role = Role.ADMIN
    elif session_cookie:
        res = verify_session_token(session_cookie)
        is_token_valid = res[0]
        user_ident = res[1]
        token_role = res[2] if len(res) > 2 else "ANALYST"
        if is_token_valid:
            is_valid = True
            identity = f"session:{user_ident}"
            user_role = parse_role(token_role, default=Role.ANALYST)
        else:
            logger.warning(f"Rejected unverified/expired session cookie for path {path}")
    elif (
        (os.getenv("ENVIRONMENT") in ("development", "dev") or 
         os.getenv("NODE_ENV") in ("development", "dev") or 
         os.getenv("SENTINEL_ENV") in ("development", "dev", "local")) and not API_KEY
    ):
        # Dev fallback for local developer ergonomics when no key is set
        is_valid = True
        identity = "dev-client"
        user_role = Role.ADMIN

    if not is_valid:
        logger.warning(f"Failed authentication attempt for path {path}: Invalid or missing credentials.")
        raise HTTPException(status_code=403, detail="Could not validate API Key or Session Cookie")

    # Attach verified identity and role to request state
    if hasattr(request, "state"):
        request.state.identity = identity
        request.state.role = user_role

    # Rate limiting check
    app = request.scope.get("app") if (hasattr(request, "scope") and isinstance(request.scope, dict)) else None
    redis = getattr(app.state, "redis", None) if (app and hasattr(app, "state")) else None
    if redis:
        allowed = await check_rate_limit(redis, identity)
        if not allowed:
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Please slow down requests.")

    return identity


async def verify_websocket_api_key(websocket: WebSocket) -> bool:
    """Validate API key or cryptographically signed session cookie on a WebSocket handshake BEFORE calling accept()."""
    cookies_map = getattr(websocket, "cookies", {})
    session_cookie = cookies_map.get("sentinel_session") if hasattr(cookies_map, "get") else None

    headers_map = getattr(websocket, "headers", {})
    query_params_map = getattr(websocket, "query_params", {})

    api_key_header = (headers_map.get("X-API-KEY") or headers_map.get("x-api-key")) if hasattr(headers_map, "get") else None
    api_key_query = query_params_map.get("api_key") if hasattr(query_params_map, "get") else None

    api_key = api_key_header or api_key_query

    is_valid = False
    if api_key and isinstance(api_key, str) and API_KEY:
        if hmac.compare_digest(api_key.encode("utf-8"), API_KEY.encode("utf-8")):
            is_valid = True
    elif session_cookie and isinstance(session_cookie, str):
        is_token_valid, *rest = verify_session_token(session_cookie)
        if is_token_valid:
            is_valid = True
        else:
            logger.warning("WebSocket rejected: invalid or expired session cookie.")
    elif (
        (os.getenv("ENVIRONMENT") in ("development", "dev") or 
         os.getenv("NODE_ENV") in ("development", "dev") or 
         os.getenv("SENTINEL_ENV") in ("development", "dev", "local")) and not API_KEY
    ):
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

def get_db_optional(request: Request = None):
    """Retrieves TimescaleDB connection from the global app state if available, or None."""
    if request is None or not hasattr(request.app.state, "db") or not request.app.state.db:
        return None
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

def get_redis_optional(request: Request = None):
    """Retrieves Redis connection from the global app state if available, or None."""
    if request is None or not hasattr(request.app.state, "redis") or not request.app.state.redis:
        return None
    return request.app.state.redis