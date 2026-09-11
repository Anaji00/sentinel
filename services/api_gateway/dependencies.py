"""
This file centrally manages API security and 
passes database connections from the main app state 
down into isolated routes"""

import hmac
import ipaddress
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

# Shared secret for metric scrapers. Unset means the metrics endpoints require
# the ordinary API key instead -- never that they are public.
METRICS_TOKEN = os.getenv("METRICS_TOKEN", "").strip()

# Session signing secret: MUST be its own configured value. No string default and
# no fallback to API_GATEWAY_KEY (reusing the API key to sign JWTs would let anyone
# holding the API key forge sessions). Fails closed at startup if unset — the
# frontend BFF signs cookies with the same SESSION_SECRET, so the two must match.
SESSION_SECRET = get_secret("SESSION_SECRET", required=True)


# The health paths an orchestrator may reach without credentials.
#
# This was the inverse -- a denylist naming /sources and /data, with everything
# else under /api/v1/health open -- and a denylist only ever protects the paths
# somebody remembered. /api/v1/health/secrets was not on it, so the credential
# audit, including masked previews that reveal a password's prefix and suffix,
# was served to any unauthenticated caller. The same shape had already been
# repaired once here, for /sources, by adding one more name to the list.
#
# Stated as an allowlist so a health endpoint added tomorrow is closed until
# someone decides otherwise, rather than open until someone notices.
_OPEN_HEALTH_PATHS = frozenset({
    "/api/v1/health/liveness",
    "/api/v1/health/readiness",
})


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
                # No expiry segment, so no expiry check ever ran.
                #
                # The signature is verified below, so such a token cannot be
                # forged -- but a legitimately issued one of this shape stays
                # valid forever, which is what a session expiry exists to
                # prevent. A token that cannot expire is refused rather than
                # accepted indefinitely.
                logger.warning(
                    "Rejected a session token carrying no expiry. Tokens must "
                    "encode one; a token that cannot expire cannot be revoked "
                    "by time."
                )
                return False, None, None

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


# Token bucket, evaluated inside Redis.
#
# The Python version read the token count and the refill time, computed the new
# count, and wrote it back -- three round trips with no lock. Two requests
# arriving together both read the same count, both decided a token was
# available, and both wrote the same decremented value, so N concurrent
# requests consumed one token between them. The limiter held under sequential
# load and dissolved under exactly the concurrent load it exists for.
#
# Lua runs atomically in Redis, so the read-compute-write is one operation.
_RATE_LIMIT_LUA = """
local tokens_key = KEYS[1]
local ts_key     = KEYS[2]
local max_tokens = tonumber(ARGV[1])
local refill     = tonumber(ARGV[2])
local now        = tonumber(ARGV[3])
local ttl        = tonumber(ARGV[4])

local tokens = tonumber(redis.call('GET', tokens_key))
local last   = tonumber(redis.call('GET', ts_key))
if tokens == nil then tokens = max_tokens end
if last == nil then last = now end

local delta = now - last
if delta < 0 then delta = 0 end
tokens = math.min(max_tokens, tokens + delta * refill)

local allowed = 0
if tokens >= 1.0 then
    tokens = tokens - 1.0
    allowed = 1
end

redis.call('SET', tokens_key, tostring(tokens), 'EX', ttl)
redis.call('SET', ts_key, tostring(now), 'EX', ttl)
return allowed
"""


async def check_rate_limit(
    redis_client,
    identity_key: str,
    max_tokens: int = 120,
    refill_rate_per_sec: float = 10.0,
    fail_open: bool = True,
) -> bool:
    """Redis-backed token bucket rate limiter per key/session/IP.

    `fail_open` decides what happens when Redis cannot answer. The default is
    open, which is right for ordinary read traffic: a cache outage should not
    take the product down. Callers guarding anything sensitive pass
    `fail_open=False`, because an attacker who can degrade Redis should not
    thereby remove the limiter -- the previous behaviour was to return True
    unconditionally from a debug-level except, so a Redis drop silently
    disabled rate limiting everywhere at once.
    """
    if not redis_client:
        return True if fail_open else False
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        now = time.time()
        bucket_key = f"sentinel:ratelimit:{identity_key}"
        allowed = await raw_redis.eval(
            _RATE_LIMIT_LUA,
            2,
            f"{bucket_key}:tokens",
            f"{bucket_key}:last_refill",
            str(float(max_tokens)),
            str(float(refill_rate_per_sec)),
            str(now),
            "3600",
        )
        return bool(int(allowed))
    except Exception as e:
        if fail_open:
            logger.warning(
                "Rate limiting unavailable (%s); allowing the request. "
                "Sensitive callers should pass fail_open=False.", e,
            )
            return True
        logger.error("Rate limiting unavailable (%s); refusing the request.", e)
        return False


# Networks whose forwarding headers are believed. Empty means believe nobody,
# which is the safe default for a service exposed directly.
TRUSTED_PROXY_CIDRS = [
    c.strip() for c in os.getenv("TRUSTED_PROXY_CIDRS", "").split(",") if c.strip()
]


def _is_ip_literal(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except (ValueError, TypeError):
        return False


def _peer_is_trusted_proxy(peer: str) -> bool:
    """Whether the immediate peer is one of our own reverse proxies."""
    if not TRUSTED_PROXY_CIDRS or not peer or peer == "unknown":
        return False
    try:
        addr = ipaddress.ip_address(peer)
    except (ValueError, TypeError):
        return False
    for cidr in TRUSTED_PROXY_CIDRS:
        try:
            if addr in ipaddress.ip_network(cidr, strict=False):
                return True
        except ValueError:
            continue
    return False


def client_address(request) -> str:
    """Best-effort source address, honouring the ingress proxy headers.

    Behind nginx every request appears to originate from the proxy, so
    throttling on the socket address would put all callers in one bucket.
    """
    client = getattr(request, "client", None)
    peer = getattr(client, "host", None) or "unknown"

    # Forwarding headers are only believed from a trusted proxy.
    #
    # These were read from every request. They are caller-supplied strings, so
    # anyone could set X-Forwarded-For to a fresh value per request and get a
    # fresh rate-limit bucket each time -- the limiter was bypassable by adding
    # a header. Worse, it is also the identity used in throttling keys and
    # audit lines, so an attacker could attribute their traffic to somebody
    # else's address.
    #
    # TRUSTED_PROXY_CIDRS lists the networks the ingress actually runs on. When
    # it is unset the headers are ignored entirely and the socket address is
    # used, which is correct for a direct-exposure deployment.
    if _peer_is_trusted_proxy(peer):
        headers = getattr(request, "headers", None)
        if headers is not None:
            fwd = headers.get("X-Forwarded-For") or headers.get("x-forwarded-for")
            if fwd:
                # Right-most entry the trusted hop appended is the one it saw;
                # the left-most is whatever the client claimed. Take the first
                # from the left only because nginx here rewrites the chain, and
                # validate it parses as an address before trusting it.
                candidate = fwd.split(",")[0].strip()
                if _is_ip_literal(candidate):
                    return candidate
            real = headers.get("X-Real-IP") or headers.get("x-real-ip")
            if real and _is_ip_literal(real.strip()):
                return real.strip()
    return peer


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
    # The health prefix is exempt, but not everything under it.
    #
    # `/api/v1/health/sources`, added this session, inherited this blanket
    # exemption and served the platform's entire feed inventory unauthenticated:
    # 46 sources, their learned polling cadences, and which are currently down.
    # For an intelligence platform the list of what it watches and how often is
    # among the more sensitive things it holds, and a liveness probe is a poor
    # reason to publish it.
    #
    # Liveness and readiness stay open because an orchestrator has to reach them
    # before it has credentials. Anything that describes the platform's sources
    # or data does not.
    # /health stays open: a liveness probe that needs a credential is a
    # liveness probe that fails during a credential outage.
    if path == "/health":
        return None

    # Metrics are not public.
    #
    # These were exempted outright. They expose per-service throughput, queue
    # depths, error counts, model latencies and the platform's own detection
    # rates -- an operational map of what is running, what is failing and what
    # is being noticed, served to anyone who asks.
    #
    # A scrape token keeps Prometheus working, because a scraper cannot present
    # a session cookie. When METRICS_TOKEN is unset the endpoints fall through
    # to the normal API-key check rather than opening up, so an operator who
    # never configures one is not silently exposed.
    # `/api/v1/health/metrics` serves the same content and was open, because it
    # lives under the health prefix and the prefix was exempt by default. That is
    # the denylist failure in one line: the operational map was closed at one
    # door and left open at the other, and the test covering it read the second
    # door as a liveness probe.
    if path in (
        "/metrics",
        "/metrics/json",
        "/api/v1/health/metrics",
        "/api/v1/health/metrics/json",
    ):
        if METRICS_TOKEN:
            supplied = None
            if hasattr(request, "headers"):
                supplied = request.headers.get("X-Metrics-Token")
                if not supplied:
                    auth = request.headers.get("Authorization") or ""
                    if auth.lower().startswith("bearer "):
                        supplied = auth[7:].strip()
            if supplied and hmac.compare_digest(
                supplied.encode("utf-8"), METRICS_TOKEN.encode("utf-8")
            ):
                return None
            raise HTTPException(status_code=401, detail="Metrics require a scrape token.")
    if path in _OPEN_HEALTH_PATHS:
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
    # Headers only. An API key in a query string is written to the nginx access
    # log, kept in browser history, and sent onward in the Referer header of
    # any link the page loads -- and this key grants Role.ADMIN. A credential
    # that ends up in three logs by default is not a credential.
    #
    # Rejected loudly rather than ignored, so a caller still passing ?api_key=
    # finds out immediately instead of silently becoming anonymous.
    api_key = request.headers.get("X-API-KEY") if hasattr(request, "headers") else None
    if not api_key and hasattr(request, "headers"):
        auth_header = request.headers.get("Authorization") or ""
        if auth_header.lower().startswith("bearer "):
            api_key = auth_header[7:].strip()
    if not api_key and hasattr(request, "scope") and isinstance(request.scope, dict)             and "query_string" in request.scope and request.query_params.get("api_key"):
        raise HTTPException(
            status_code=400,
            detail=(
                "API keys must be sent in the X-API-KEY header or as a Bearer "
                "token. A key in the query string is logged by the proxy and "
                "retained in browser history."
            ),
        )

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
    # Deliberately still accepted here, unlike on the HTTP path.
    #
    # A browser cannot set custom headers on a WebSocket handshake, so the query
    # string is the only mechanism available to one that is not relying on the
    # session cookie checked above. The exposure is the same in kind -- the URL
    # reaches the proxy access log -- which is why the HTTP handler refuses it
    # outright and this one does not: there, headers are always available and
    # the query string is a convenience; here it is sometimes the only option.
    #
    # Prefer the cookie or a header where the client can send one.
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