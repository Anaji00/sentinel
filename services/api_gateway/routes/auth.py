"""
services/api_gateway/routes/auth.py

Account authentication against the `users` table.

Before this the platform had exactly one account, defined by ADMIN_EMAIL and
ADMIN_PASSWORD in the environment and compared with a plaintext `==` in the
Next.js login route. That is defensible for a single self-hosted operator and
unacceptable the moment real people sign up: the secret sits in the environment
of a public-facing web process, cannot be rotated per user, and cannot express
roles or subscription tiers.

Verification now happens here, against scrypt hashes in Postgres, because the
gateway is the only tier that reaches the database. The frontend keeps its role
as a thin BFF: it forwards the credentials and, on success, mints the signed
session cookie both tiers already share via SESSION_SECRET.

Login is deliberately unauthenticated, so it is throttled here rather than by
`verify_api_key` -- an endpoint that checks passwords is exactly the one an
attacker will hammer.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from services.api_gateway.dependencies import (
    check_rate_limit,
    client_address,
    get_db_optional,
    get_redis_optional,
)
from shared.utils.accounts import (
    account_from_row,
    hash_password,
    is_valid_email,
    needs_rehash,
    normalize_email,
    verify_password,
)

logger = logging.getLogger("api-gateway.auth")

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])

# Failed logins get a far tighter budget than ordinary traffic: a burst of 10,
# then roughly 6 per minute. Generous for a mistyped password, useless for
# guessing one. Successful logins do not consume the budget.
_LOGIN_BURST = 10
_LOGIN_REFILL_PER_SEC = 0.1


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=1024)


async def _throttle(redis, scope: str, request: Request) -> None:
    """Raises 429 once a source has burned its failed-attempt budget."""
    if not redis:
        return
    allowed = await check_rate_limit(
        redis,
        f"login:{scope}:{client_address(request)}",
        max_tokens=_LOGIN_BURST,
        refill_rate_per_sec=_LOGIN_REFILL_PER_SEC,
    )
    if not allowed:
        logger.warning("Rate-limited login attempts from %s", client_address(request))
        raise HTTPException(
            status_code=429,
            detail="Too many sign-in attempts. Try again in a few minutes.",
        )


@router.post("/login")
async def login(
    body: LoginRequest,
    request: Request,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """Verifies credentials against the `users` table.

    The response deliberately does not distinguish "no such account" from "wrong
    password": telling an attacker which emails are registered turns a password
    guess into an account enumeration.
    """
    email = normalize_email(body.email)
    await _throttle(redis, "attempt", request)

    if not is_valid_email(email):
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    if db is None:
        # Fail closed. Authenticating without the account store would mean
        # falling back to some weaker check, which is the situation this module
        # exists to remove.
        logger.error("Login attempted while the account database is unavailable.")
        raise HTTPException(status_code=503, detail="Authentication is temporarily unavailable.")

    row = await db.query_one(
        """
        SELECT id, email, password_hash, display_name, role, is_active,
               subscription_tier, subscription_status, subscription_ends_at,
               stripe_customer_id
        FROM users WHERE email = $1
        """,
        email,
    )

    # Verify even when the row is missing, against a throwaway hash, so a
    # non-existent account costs the same time as a wrong password. Skipping the
    # work here is what makes accounts enumerable by response latency.
    stored_hash = (row or {}).get("password_hash") or _DUMMY_HASH
    password_ok = verify_password(body.password, stored_hash)

    if not row or not password_ok or not row.get("is_active", True):
        logger.warning("Failed login for %s from %s", email, client_address(request))
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    account = account_from_row(row)

    # Opportunistic upgrade: a hash made under weaker parameters is replaced once
    # the plaintext is available, so raising the cost factor does not lock anyone
    # out or require a migration.
    if needs_rehash(stored_hash):
        try:
            await db.execute(
                "UPDATE users SET password_hash = $1 WHERE id = $2",
                hash_password(body.password), account.id,
            )
            logger.info("Upgraded stored password hash for account %s", account.id)
        except Exception as e:
            logger.warning("Could not upgrade password hash for %s: %s", account.id, e)

    try:
        await db.execute(
            "UPDATE users SET last_login_at = $1 WHERE id = $2",
            datetime.now(timezone.utc), account.id,
        )
    except Exception as e:
        logger.warning("Could not record last_login_at for %s: %s", account.id, e)

    return {"success": True, "user": account.to_public_dict()}


@router.get("/account")
async def account(
    email: str,
    db=Depends(get_db_optional),
):
    """Current entitlements for an already-authenticated caller.

    Reached through `verify_api_key`, so arriving here means the session cookie
    or API key was already validated. Used by the frontend to decide what to
    show without hardcoding tier rules in the client.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Account store unavailable.")
    row = await db.query_one(
        """
        SELECT id, email, password_hash, display_name, role, is_active,
               subscription_tier, subscription_status, subscription_ends_at,
               stripe_customer_id
        FROM users WHERE email = $1
        """,
        normalize_email(email),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Account not found.")
    return account_from_row(row).to_public_dict()


# A structurally valid hash that no password matches, used to keep the timing of
# a missing account indistinguishable from a wrong password. Built once at import
# so the scrypt cost is not paid on every failed login.
_DUMMY_HASH = hash_password("sentinel-nonexistent-account-placeholder")


async def ensure_admin_account(db) -> None:
    """Seeds the operator account from ADMIN_EMAIL / ADMIN_PASSWORD on first run.

    Migration path, not a permanent mechanism. The single-account environment
    credentials keep working for the existing operator, but they are now stored
    as an scrypt hash in `users` rather than compared in plaintext, and everything
    downstream -- roles, tiers, Stripe linkage -- hangs off a real row.

    Only ever creates a missing row. It never rewrites an existing password, so
    a password changed in the product is not silently reverted on restart.

    The operator is seeded active, not merely tier=pro: `Account.has_pro` checks
    tier *and* subscription status, so a pro tier with status "none" would lock
    the owner of the deployment out of the paid surface of their own instance.
    """
    email = normalize_email(os.getenv("ADMIN_EMAIL", ""))
    password = os.getenv("ADMIN_PASSWORD", "")
    if not email or not password:
        return
    if not is_valid_email(email):
        logger.warning("ADMIN_EMAIL %r is not a valid address; skipping seed.", email)
        return
    if db is None:
        return
    try:
        existing = await db.query_one("SELECT id FROM users WHERE email = $1", email)
        if existing:
            return
        await db.execute(
            """
            INSERT INTO users (email, password_hash, display_name, role,
                               subscription_tier, subscription_status)
            VALUES ($1, $2, $3, 'ADMIN', 'pro', 'active')
            ON CONFLICT (email) DO NOTHING
            """,
            email, hash_password(password), "Operator",
        )
        logger.info("Seeded operator account %s from environment credentials.", email)
    except Exception as e:
        # Never block startup on seeding: the gateway still serves everything
        # that does not require an account.
        logger.error("Could not seed operator account: %s", e)
