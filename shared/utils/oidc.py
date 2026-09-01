"""
shared/utils/oidc.py

Generic OpenID Connect sign-in, as an option.

One authorization-code flow with PKCE, configured by issuer URL, covers Okta,
Entra ID, Google Workspace and Auth0 without a line of per-vendor code. Nothing
here activates unless OIDC_ISSUER and OIDC_CLIENT_ID are set: an unconfigured
deployment behaves exactly as it did before this module existed, which is what
makes SSO an option rather than a migration.

Identity comes from the UserInfo endpoint, not from the ID token
─────────────────────────────────────────────────────────────────
This deployment has no JWT library and no `cryptography` in the gateway image,
so ID token signatures cannot be verified here, and adding a dependency for it
is what CLAUDE.md tells us not to do when the standard library will serve.

OIDC Core §3.1.3.7 item 6 permits exactly that for this flow: when the ID token
arrives through direct TLS-protected communication between a confidential client
and the token endpoint, TLS server validation MAY stand in for signature
validation. That is the situation here -- the code exchange is a server-to-server
POST to the issuer's own token endpoint, authenticated with the client secret.

We go further than the minimum rather than relying on that clause alone:

  * The ID token payload is never trusted for identity. It is decoded only to
    check `nonce`, `iss`, `aud` and `exp` -- to prove this token answers *our*
    request. The subject and email that become an account are read back from the
    UserInfo endpoint over a second TLS call, presenting the access token.
  * A forged ID token therefore buys nothing: the attacker would also have to
    make the issuer's own UserInfo endpoint return their claims.
  * PKCE (S256) binds the authorization code to this browser session, so an
    intercepted code cannot be redeemed elsewhere.
  * `state` and `nonce` are generated per attempt and held server-side in Redis,
    single-use, so neither CSRF nor replay of an old response is possible.

If a JWT verifier is ever added to the image, `_decode_jwt_payload` is the one
place that changes: verify the signature there and everything above still holds.

Nothing in this module writes to the database. Account creation and linking
live in the route, because those are policy decisions and this is a protocol.
"""

import base64
import hashlib
import json
import logging
import os
import secrets
import time
from typing import Any, Dict, Optional, Tuple

import aiohttp

logger = logging.getLogger("shared.oidc")

# How long a pending authorization attempt stays redeemable. Long enough to
# type a password and clear MFA, short enough that an abandoned attempt is not
# left lying around.
LOGIN_ATTEMPT_TTL_SEC = int(os.getenv("OIDC_LOGIN_TTL_SEC", "600"))

# Discovery and JWKS documents change rarely; refetching them on every sign-in
# would put a third-party round trip on the login path.
_DISCOVERY_TTL_SEC = int(os.getenv("OIDC_DISCOVERY_TTL_SEC", "3600"))

# Tolerance for clock skew between us and the issuer when checking `exp`.
_CLOCK_SKEW_SEC = 120

_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)

# Cached discovery document: {issuer: (fetched_at, document)}
_discovery_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


class OidcError(Exception):
    """A sign-in attempt that cannot be completed.

    Carries a message safe to show a person. Anything diagnostic goes to the
    log instead: telling a caller *why* their token failed validation is a
    probing oracle.
    """


# ── Configuration ─────────────────────────────────────────────────────────────

def issuer() -> str:
    return (os.getenv("OIDC_ISSUER") or "").strip().rstrip("/")


def client_id() -> str:
    return (os.getenv("OIDC_CLIENT_ID") or "").strip()


def _client_secret() -> str:
    return (os.getenv("OIDC_CLIENT_SECRET") or "").strip()


def redirect_uri() -> str:
    return (os.getenv("OIDC_REDIRECT_URI") or "").strip()


def scopes() -> str:
    """`openid` is mandatory; email lets us link to an existing account."""
    return (os.getenv("OIDC_SCOPES") or "openid email profile").strip()


def provider_label() -> str:
    """What the sign-in button should say. Cosmetic, so it has a default."""
    return (os.getenv("OIDC_PROVIDER_LABEL") or "Single sign-on").strip()


def is_configured() -> bool:
    """True only when every value needed to complete a flow is present.

    Deliberately strict. A half-configured provider that renders a button and
    then fails at the callback is worse than no button, because the person
    cannot tell whether they typed something wrong.
    """
    return bool(issuer() and client_id() and _client_secret() and redirect_uri())


def _role_map() -> Dict[str, str]:
    """IdP group name -> Sentinel role, from OIDC_ROLE_MAP.

    Format: "platform-admins:ADMIN,analysts:ANALYST". Absent means every SSO
    account lands on the default role below.
    """
    raw = (os.getenv("OIDC_ROLE_MAP") or "").strip()
    mapping: Dict[str, str] = {}
    for pair in raw.split(","):
        if ":" not in pair:
            continue
        group, role = pair.split(":", 1)
        group, role = group.strip().lower(), role.strip().upper()
        if group and role:
            mapping[group] = role
    return mapping


def default_role() -> str:
    """Role for an SSO account whose groups match nothing.

    VIEWER, and the environment cannot raise it above ANALYST. An SSO provider
    is an assertion about *identity*; it is not an assertion that this person
    should administer the platform. A misconfigured OIDC_DEFAULT_ROLE=ADMIN
    would hand ownership to anyone with an account at the issuer, which for a
    Google Workspace tenant is a large group of people.
    """
    requested = (os.getenv("OIDC_DEFAULT_ROLE") or "VIEWER").strip().upper()
    if requested not in ("VIEWER", "ANALYST"):
        logger.warning(
            "Ignoring OIDC_DEFAULT_ROLE=%s: an SSO default above ANALYST is not "
            "permitted. Map a specific IdP group to a higher role instead.",
            requested,
        )
        return "VIEWER"
    return requested


def role_for_groups(claims: Dict[str, Any]) -> str:
    """The strongest role any of this person's IdP groups maps to."""
    claim_name = (os.getenv("OIDC_GROUPS_CLAIM") or "groups").strip()
    raw = claims.get(claim_name)
    if isinstance(raw, str):
        groups = [raw]
    elif isinstance(raw, (list, tuple)):
        groups = [str(g) for g in raw]
    else:
        groups = []

    mapping = _role_map()
    # Ordered strongest first, so the best match wins rather than the last one.
    precedence = ("ADMIN", "ANALYST", "VIEWER")
    matched = {mapping.get(str(g).strip().lower()) for g in groups}
    for role in precedence:
        if role in matched:
            return role
    return default_role()


# ── Discovery ─────────────────────────────────────────────────────────────────

async def discover(session: aiohttp.ClientSession) -> Dict[str, Any]:
    """The issuer's OpenID configuration, cached.

    Fetched rather than configured endpoint by endpoint: the discovery document
    is the issuer's own statement of where its endpoints are, so it stays
    correct when the provider moves them.
    """
    iss = issuer()
    if not iss:
        raise OidcError("Single sign-on is not configured.")

    cached = _discovery_cache.get(iss)
    if cached and (time.time() - cached[0]) < _DISCOVERY_TTL_SEC:
        return cached[1]

    url = f"{iss}/.well-known/openid-configuration"
    try:
        async with session.get(url, timeout=_HTTP_TIMEOUT) as resp:
            if resp.status != 200:
                raise OidcError("The sign-on provider is not responding correctly.")
            doc = await resp.json()
    except aiohttp.ClientError as e:
        logger.warning("OIDC discovery failed for %s: %s", iss, e)
        raise OidcError("The sign-on provider could not be reached.")

    # The issuer in the document must match the one we asked, or we are talking
    # to something that is not the issuer we configured.
    if str(doc.get("issuer", "")).rstrip("/") != iss:
        logger.error(
            "OIDC discovery issuer mismatch: configured %s, document says %s",
            iss, doc.get("issuer"),
        )
        raise OidcError("The sign-on provider's identity could not be confirmed.")

    for required in ("authorization_endpoint", "token_endpoint", "userinfo_endpoint"):
        if not doc.get(required):
            raise OidcError("The sign-on provider is missing a required endpoint.")

    _discovery_cache[iss] = (time.time(), doc)
    return doc


# ── PKCE and the authorization request ────────────────────────────────────────

def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def new_pkce_pair() -> Tuple[str, str]:
    """(verifier, S256 challenge).

    PKCE is not optional here even though this is a confidential client. It
    binds the authorization code to the browser that started the flow, so a code
    leaked through a redirect, a proxy log or the Referer header cannot be
    redeemed by whoever picked it up.
    """
    verifier = _b64url(secrets.token_bytes(48))
    challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


def new_state() -> str:
    return _b64url(secrets.token_bytes(24))


def new_nonce() -> str:
    return _b64url(secrets.token_bytes(24))


def authorization_url(discovery: Dict[str, Any], state: str, nonce: str, challenge: str) -> str:
    """Where to send the browser to authenticate."""
    from urllib.parse import urlencode

    params = {
        "response_type": "code",
        "client_id": client_id(),
        "redirect_uri": redirect_uri(),
        "scope": scopes(),
        "state": state,
        "nonce": nonce,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    return f"{discovery['authorization_endpoint']}?{urlencode(params)}"


# ── Token exchange and identity ───────────────────────────────────────────────

def _decode_jwt_payload(token: str) -> Dict[str, Any]:
    """The claims of a JWT, without verifying its signature.

    The only place in this module that touches the ID token's contents, and the
    single place to change if a verifier is ever added to the image. Everything
    read from here is used to confirm the token answers *our* request -- nonce,
    issuer, audience, expiry -- and never to establish who someone is. Identity
    comes from UserInfo. See this module's docstring.
    """
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("not a three-part JWT")
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)          # restore base64url padding
        return json.loads(base64.urlsafe_b64decode(payload))
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        logger.warning("Could not decode ID token payload: %s", e)
        raise OidcError("The sign-on provider returned a response we could not read.")


def _check_id_token_claims(claims: Dict[str, Any], expected_nonce: str) -> None:
    """Confirms this ID token was minted for this request.

    Raises rather than returning a reason: the caller has nothing useful to do
    with the distinction, and reporting it to the browser would say which check
    failed.
    """
    if str(claims.get("iss", "")).rstrip("/") != issuer():
        raise OidcError("The sign-on response came from an unexpected issuer.")

    audience = claims.get("aud")
    audiences = audience if isinstance(audience, list) else [audience]
    if client_id() not in [str(a) for a in audiences]:
        raise OidcError("The sign-on response was issued for a different application.")

    # A token with no nonce fails: this flow always sends one, so its absence
    # means the response is not an answer to a request we made.
    if str(claims.get("nonce") or "") != expected_nonce:
        raise OidcError("The sign-on response did not match this sign-in attempt.")

    expires = claims.get("exp")
    try:
        if float(expires) + _CLOCK_SKEW_SEC < time.time():
            raise OidcError("The sign-on response has expired. Please try again.")
    except (TypeError, ValueError):
        raise OidcError("The sign-on response was missing an expiry.")


async def exchange_code(
    session: aiohttp.ClientSession,
    discovery: Dict[str, Any],
    code: str,
    verifier: str,
) -> Dict[str, Any]:
    """Redeems an authorization code for tokens, server to server."""
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri(),
        "client_id": client_id(),
        "client_secret": _client_secret(),
        "code_verifier": verifier,
    }
    try:
        async with session.post(
            discovery["token_endpoint"], data=data, timeout=_HTTP_TIMEOUT,
            headers={"Accept": "application/json"},
        ) as resp:
            body = await resp.json(content_type=None)
            if resp.status != 200:
                # The provider's error text can name the client secret or the
                # redirect URI, so it is logged and not returned.
                logger.warning(
                    "OIDC token exchange failed (%s): %s", resp.status, body,
                )
                raise OidcError("Sign-on could not be completed. Please try again.")
            return body
    except aiohttp.ClientError as e:
        logger.warning("OIDC token endpoint unreachable: %s", e)
        raise OidcError("The sign-on provider could not be reached.")


async def fetch_userinfo(
    session: aiohttp.ClientSession,
    discovery: Dict[str, Any],
    access_token: str,
) -> Dict[str, Any]:
    """The authoritative account claims, read back from the issuer.

    This is where identity actually comes from -- see the module docstring. The
    call is TLS-protected and presents the access token we just received, so the
    answer is the issuer's own statement about the person who authenticated.
    """
    try:
        async with session.get(
            discovery["userinfo_endpoint"],
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=_HTTP_TIMEOUT,
        ) as resp:
            if resp.status != 200:
                logger.warning("OIDC userinfo returned %s", resp.status)
                raise OidcError("Sign-on could not be completed. Please try again.")
            return await resp.json(content_type=None)
    except aiohttp.ClientError as e:
        logger.warning("OIDC userinfo endpoint unreachable: %s", e)
        raise OidcError("The sign-on provider could not be reached.")


async def complete_login(
    session: aiohttp.ClientSession,
    code: str,
    verifier: str,
    expected_nonce: str,
) -> Dict[str, Any]:
    """Runs the whole exchange and returns validated identity claims.

    Returns `{subject, issuer, email, email_verified, display_name, role}`.
    Raises OidcError with a message safe to show a person.
    """
    discovery = await discover(session)
    tokens = await exchange_code(session, discovery, code, verifier)

    id_token = tokens.get("id_token")
    access_token = tokens.get("access_token")
    if not id_token or not access_token:
        raise OidcError("The sign-on provider returned an incomplete response.")

    _check_id_token_claims(_decode_jwt_payload(id_token), expected_nonce)

    claims = await fetch_userinfo(session, discovery, access_token)

    subject = str(claims.get("sub") or "").strip()
    if not subject:
        # Without a stable subject there is nothing to key an account on, and
        # falling back to email would let an address change at the IdP silently
        # become a different account.
        raise OidcError("The sign-on provider did not identify the account.")

    email = str(claims.get("email") or "").strip().lower()

    # `email_verified` decides whether this identity may be linked to an
    # existing password account. Absent means no: an unverified address from an
    # IdP that lets anyone claim one is an account-takeover primitive, and the
    # link is the exact step where it would be exercised.
    email_verified = claims.get("email_verified")
    email_verified = email_verified is True or str(email_verified).lower() == "true"

    return {
        "subject": subject,
        "issuer": issuer(),
        "email": email,
        "email_verified": email_verified,
        "display_name": (
            str(claims.get("name") or claims.get("preferred_username") or "").strip() or None
        ),
        "role": role_for_groups(claims),
    }
