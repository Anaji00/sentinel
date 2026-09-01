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

# API key: gated dev fallback (resolve_env_var only returns the fallback when
# SENTINEL_ENV is in the safe-dev whitelist; production raises). The literal is a
# non-production placeholder, never a usable admin key in a real deployment.
API_KEY = resolve_env_var("API_GATEWAY_KEY", "dev-only-key-replace-in-prod", warn_on_fallback=True)

# Session signing secret: MUST be its own configured value. No string default and
# no fallback to API_GATEWAY_KEY (reusing the API key to sign JWTs would let anyone
# holding the API key forge sessions). Fails closed at startup if unset — the
# frontend BFF signs cookies with the same SESSION_SECRET, so the two must match.
SESSION_SECRET = get_secret("SESSION_SECRET", required=True)


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


def client_address(request) -> str:
    """Best-effort source address, honouring the ingress proxy headers.

    Behind nginx every request appears to originate from the proxy, so
    throttling on the socket address would put all callers in one bucket.
    """
    headers = getattr(request, "headers", None)
    if headers is not None:
        fwd = headers.get("X-Forwarded-For") or headers.get("x-forwarded-for")
        if fwd:
            return fwd.split(",")[0].strip()
        real = headers.get("X-Real-IP") or headers.get("x-real-ip")
        if real:
            return real.strip()
    client = getattr(request, "client", None)
    return getattr(client, "host", None) or "unknown"


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
    # Login must be reachable without credentials -- it is where credentials are
    # presented. It carries its own per-source throttling rather than relying on
    # the check below, which only runs for callers who are already authenticated.
    # Note this exempts the login path only; /api/v1/auth/account stays protected.
    # Every endpoint a person reaches before they have credentials. Each carries
    # its own per-source throttling, because the check further down only runs for
    # callers who are already authenticated. /api/v1/auth/account is deliberately
    # NOT here -- reading an account requires being signed in.
    _PUBLIC_AUTH_PATHS = (
        "/api/v1/auth/login",
        "/api/v1/auth/signup",
        "/api/v1/auth/verify",
        "/api/v1/auth/resend-verification",
        "/api/v1/auth/forgot-password",
        "/api/v1/auth/reset-password",
        # Single sign-on, all three steps. Every one of them happens before the
        # caller has any credential of ours -- that is the entire point of the
        # flow -- so gating them behind the API key made SSO unusable in a way
        # that looked like it simply did not exist: /status answered 403, the
        # BFF read any non-200 as "no provider configured", and the button never
        # rendered even with an issuer set.
        #
        # None of the three leaks anything. /status reports only whether SSO is
        # enabled and its button label; the issuer, client id and secret stay
        # server-side. /start and /callback are throttled per source and are
        # already bound by state, nonce and PKCE, which is a stronger check
        # than a shared key that every browser session would have to carry.
        "/api/v1/auth/oidc/status",
        "/api/v1/auth/oidc/start",
        "/api/v1/auth/oidc/callback",
        "/api/v1/billing/waitlist",
    )
    if path.rstrip("/") in _PUBLIC_AUTH_PATHS:
        return None
    # Stripe calls the webhook directly and cannot present our API key or a
    # session cookie. It is authenticated by HMAC signature over the raw body
    # inside the handler instead, which is strictly stronger than a shared key.
    if path in ("/api/v1/billing/webhook", "/api/v1/billing/webhook/"):
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

    # Resolved once: needed to throttle failed authentication as well as
    # successful callers.
    app = request.scope.get("app") if (hasattr(request, "scope") and isinstance(request.scope, dict)) else None
    redis = getattr(app.state, "redis", None) if (app and hasattr(app, "state")) else None

    if not is_valid:
        # Unauthenticated callers were never throttled: the rate-limit check sat
        # below this raise, so a stranger could guess keys as fast as the network
        # allowed. Failed attempts are now bucketed by source address, with a far
        # tighter budget than authenticated traffic -- roughly 12 per minute
        # sustained after a burst of 20.
        if redis:
            allowed = await check_rate_limit(
                redis,
                f"authfail:{client_address(request)}",
                max_tokens=20,
                refill_rate_per_sec=0.2,
            )
            if not allowed:
                logger.warning(
                    "Rate-limited repeated failed authentication from %s on %s",
                    client_address(request), path,
                )
                raise HTTPException(
                    status_code=429,
                    detail="Too many failed authentication attempts. Try again shortly.",
                )
        logger.warning(f"Failed authentication attempt for path {path}: Invalid or missing credentials.")
        raise HTTPException(status_code=403, detail="Could not validate API Key or Session Cookie")

    # Attach verified identity and role to request state
    if hasattr(request, "state"):
        request.state.identity = identity
        request.state.role = user_role

    # Rate limiting check
    if redis:
        allowed = await check_rate_limit(redis, identity)
        if not allowed:
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Please slow down requests.")

    return identity



def require_pro(feature: str):
    """Dependency factory gating a paid feature behind an active subscription.

    Entitlement is read from the account row rather than the session cookie, so
    a cancellation takes effect on the next request instead of whenever the
    cookie happens to expire. Named features rather than inline tier checks, so
    every gate in the system is enumerable in one place (accounts.PRO_FEATURES).
    """
    from fastapi import Request as _Request

    async def _gate(request: _Request):
        from shared.utils.accounts import PRO_FEATURES, account_from_row
        from shared.utils.stripe_client import billing_enabled

        if feature not in PRO_FEATURES:
            # A typo in a gate name must not silently grant access.
            raise HTTPException(status_code=500, detail=f"Unknown gated feature {feature!r}.")

        # While payments are switched off the whole platform is free, including
        # the reasoning tier. Charging is the only thing a paywall is for, so
        # gating without it would deny people features nobody can pay for.
        # Tied to the same switch as billing so the two can never disagree:
        # turning payments on restores every gate in one move.
        if not billing_enabled():
            return True

        identity = getattr(getattr(request, "state", None), "identity", "") or ""
        # An API key is the operator's own credential, not a subscriber session;
        # it is already restricted to ADMIN and is not paywalled.
        if identity.startswith("apikey:") or identity == "dev-client":
            return True

        email = identity.split("session:", 1)[1] if identity.startswith("session:") else None
        app = request.scope.get("app") if isinstance(getattr(request, "scope", None), dict) else None
        db = getattr(app.state, "db", None) if (app and hasattr(app, "state")) else None
        if not email or db is None:
            raise HTTPException(status_code=403, detail="Sign in to use this feature.")

        row = await db.query_one(
            """
            SELECT id, email, password_hash, display_name, role, is_active,
                   subscription_tier, subscription_status, subscription_ends_at,
                   stripe_customer_id
            FROM users WHERE email = $1
            """,
            email.strip().lower(),
        )
        if not row or not account_from_row(row).can_use(feature):
            raise HTTPException(
                status_code=402,   # Payment Required: the client renders an upgrade prompt.
                detail={
                    "error": "subscription_required",
                    "feature": feature,
                    "message": "This feature is part of Sentinel Pro.",
                },
            )
        return True

    return _gate


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