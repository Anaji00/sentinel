"""
services/api_gateway/routes/oidc.py

Single sign-on endpoints. Dormant unless OIDC is configured.

Three routes, mirroring the three moments of an authorization-code flow:

  GET  /auth/oidc/status    is SSO available, and what should the button say
  POST /auth/oidc/start     begin an attempt; returns where to send the browser
  POST /auth/oidc/callback  finish it; returns the same account shape as /login

The protocol lives in shared/utils/oidc.py. What lives here is policy: when a
federated identity may be attached to an existing account, what happens when it
matches nobody, and what role it gets. Those are decisions about this product,
not about OIDC.
"""

import logging
from typing import Optional

import aiohttp
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from services.api_gateway.dependencies import (
    check_rate_limit,
    client_address,
    get_db_optional,
    get_redis_optional,
)
from shared.utils import oidc
from shared.utils.accounts import account_from_row, normalize_email

logger = logging.getLogger("api-gateway.oidc")
router = APIRouter(prefix="/api/v1/auth/oidc", tags=["Authentication"])

# Pending attempts are keyed by state and hold the verifier and nonce. Redis and
# not a cookie: the verifier must never reach the browser, or PKCE stops proving
# that the code was redeemed by whoever requested it.
_ATTEMPT_KEY = "sentinel:oidc:attempt:{state}"


class CallbackRequest(BaseModel):
    code: str = Field(min_length=1, max_length=4096)
    state: str = Field(min_length=8, max_length=512)


@router.get("/status")
async def status():
    """Whether to offer an SSO button, and what to label it.

    Unauthenticated on purpose -- it is read by the sign-in page, before anyone
    has signed in. It reports only whether the option exists and its label; the
    issuer URL, client id and secret stay server-side, because together they
    describe the deployment's identity provider to anyone who asks.
    """
    return {"enabled": oidc.is_configured(), "label": oidc.provider_label()}


@router.post("/start")
async def start(request: Request, redis=Depends(get_redis_optional)):
    """Begins a sign-in attempt and says where to send the browser."""
    if not oidc.is_configured():
        raise HTTPException(status_code=404, detail="Single sign-on is not enabled.")
    if redis is None:
        # The attempt has nowhere to live, so state and nonce could not be
        # checked when the browser comes back. Refuse rather than run the flow
        # with its two anti-forgery controls silently disabled.
        logger.error("OIDC start attempted with no Redis; cannot hold attempt state.")
        raise HTTPException(status_code=503, detail="Single sign-on is temporarily unavailable.")

    allowed = await check_rate_limit(
        redis, f"oidc_start:{client_address(request)}",
        max_tokens=10, refill_rate_per_sec=0.1,
    )
    if not allowed:
        raise HTTPException(status_code=429, detail="Too many attempts. Try again shortly.")

    try:
        async with aiohttp.ClientSession() as session:
            discovery = await oidc.discover(session)
    except oidc.OidcError as e:
        raise HTTPException(status_code=503, detail=str(e))

    state, nonce = oidc.new_state(), oidc.new_nonce()
    verifier, challenge = oidc.new_pkce_pair()

    await redis.raw.set(
        _ATTEMPT_KEY.format(state=state),
        f"{verifier}:{nonce}",
        ex=oidc.LOGIN_ATTEMPT_TTL_SEC,
    )
    return {
        "authorization_url": oidc.authorization_url(discovery, state, nonce, challenge),
        "state": state,
    }


@router.post("/callback")
async def callback(
    body: CallbackRequest,
    request: Request,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """Completes sign-in and returns the account, in /login's shape.

    The response is deliberately identical to a password login so the BFF mints
    its session cookie the same way for both, and every downstream RBAC check
    stays unaware of how someone signed in.
    """
    if not oidc.is_configured():
        raise HTTPException(status_code=404, detail="Single sign-on is not enabled.")
    if db is None or redis is None:
        raise HTTPException(status_code=503, detail="Single sign-on is temporarily unavailable.")

    # Consumed, not read: an authorization code may be redeemed once, and a
    # state that survives its use is a replay waiting to happen. GETDEL is
    # atomic, so two simultaneous callbacks cannot both win.
    key = _ATTEMPT_KEY.format(state=body.state)
    try:
        stored = await redis.raw.getdel(key)
    except AttributeError:
        # Older redis-py has no getdel. Fetch and delete; the race is narrow and
        # the alternative is leaving the state redeemable.
        stored = await redis.raw.get(key)
        await redis.raw.delete(key)

    if not stored:
        raise HTTPException(status_code=400, detail="This sign-in attempt has expired. Please start again.")

    stored = stored if isinstance(stored, str) else stored.decode("utf-8")
    verifier, _, nonce = stored.partition(":")

    try:
        async with aiohttp.ClientSession() as session:
            identity = await oidc.complete_login(session, body.code, verifier, nonce)
    except oidc.OidcError as e:
        logger.warning("OIDC sign-in failed from %s: %s", client_address(request), e)
        raise HTTPException(status_code=401, detail=str(e))

    account = await _resolve_account(db, identity)
    return {"success": True, "user": account.to_public_dict()}


async def _resolve_account(db, identity: dict):
    """Finds, links or creates the account behind a federated identity.

    Three cases, in order of precedence:

      1. The (issuer, subject) pair is already known -- sign that account in.
         Checked first so that an email change at the IdP follows the person to
         their existing account instead of stranding them with a new one.

      2. A password account exists with the same address. Linked only when the
         IdP says the address is verified: linking on an unverified address lets
         anyone who can set an arbitrary email at the IdP take over an account
         here, which is the single sharpest edge in federated sign-in.

      3. Nobody matches -- create a free-tier account.
    """
    issuer, subject = identity["issuer"], identity["subject"]

    existing = await db.query_one(
        """
        SELECT id, email, password_hash, display_name, role, is_active,
               subscription_tier, subscription_status, subscription_ends_at,
               stripe_customer_id
        FROM users WHERE oidc_issuer = $1 AND oidc_subject = $2
        """,
        issuer, subject,
    )
    if existing:
        if not existing.get("is_active", True):
            raise HTTPException(status_code=403, detail="This account has been deactivated.")
        return account_from_row(existing)

    email = normalize_email(identity.get("email") or "")
    if not email:
        raise HTTPException(
            status_code=401,
            detail="Your sign-on provider did not share an email address, which this application needs.",
        )

    by_email = await db.query_one(
        """
        SELECT id, email, password_hash, display_name, role, is_active,
               subscription_tier, subscription_status, subscription_ends_at,
               stripe_customer_id
        FROM users WHERE email = $1
        """,
        email,
    )

    if by_email:
        if not identity["email_verified"]:
            logger.warning(
                "Refusing to link SSO identity %s/%s to existing account %s: "
                "the provider did not verify the address.",
                issuer, subject, email,
            )
            raise HTTPException(
                status_code=403,
                detail=(
                    "An account already exists for this address, and your sign-on "
                    "provider has not confirmed you own it. Sign in with your "
                    "password instead."
                ),
            )
        if not by_email.get("is_active", True):
            raise HTTPException(status_code=403, detail="This account has been deactivated.")

        # Linking only. The existing role and tier are left exactly as they are:
        # an IdP group should never silently re-grade an account that a person
        # already administers here, and demoting one on first SSO sign-in would
        # be just as surprising as promoting it.
        await db.execute(
            """
            UPDATE users SET oidc_issuer = $1, oidc_subject = $2, email_verified = TRUE
            WHERE id = $3
            """,
            issuer, subject, int(by_email["id"]),
        )
        logger.info("Linked SSO identity to existing account %s", by_email["id"])
        return account_from_row(by_email)

    row = await db.query_one(
        """
        INSERT INTO users (email, password_hash, display_name, role, subscription_tier,
                           email_verified, oidc_issuer, oidc_subject)
        VALUES ($1, NULL, $2, $3, 'free', $4, $5, $6)
        ON CONFLICT (email) DO NOTHING
        RETURNING id, email, password_hash, display_name, role, is_active,
                  subscription_tier, subscription_status, subscription_ends_at,
                  stripe_customer_id
        """,
        email,
        identity.get("display_name"),
        identity["role"],
        identity["email_verified"],
        issuer,
        subject,
    )
    if not row:
        # Lost a race with a concurrent sign-in for the same address.
        raise HTTPException(status_code=409, detail="Please try signing in again.")

    logger.info("Created account %s from SSO identity %s/%s", row["id"], issuer, subject)
    return account_from_row(row)
