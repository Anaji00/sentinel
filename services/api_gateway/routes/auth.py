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
    MIN_PASSWORD_LENGTH,
    account_from_row,
    hash_password,
    is_valid_email,
    needs_rehash,
    normalize_email,
    verify_password,
)
from shared.utils.auth_tokens import (
    TokenPurpose,
    expiry_for,
    generate_token,
    hash_token,
    is_expired,
)
from shared.utils.mailer import (
    is_configured as mailer_configured,
    reset_email,
    send_email,
    verification_email,
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


# ── Open signup ───────────────────────────────────────────────────────────────
#
# Anyone may create an account and gets the free tier immediately: the whole
# analyst platform, every domain, the knowledge graph, all dashboards. The email
# address is a claim until it is confirmed, so verification gates only the two
# things that actually trust the mailbox -- being charged, and resetting a
# password. Blocking sign-in until confirmation would cost real users for no
# security gain, because nothing sensitive is reachable with a fresh account.

_SIGNUP_BURST = 5
_SIGNUP_REFILL_PER_SEC = 0.02      # ~1 per minute sustained, after a burst of 5


class SignupRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=1024)
    display_name: Optional[str] = Field(default=None, max_length=120)


class TokenRequest(BaseModel):
    token: str = Field(min_length=8, max_length=512)


class EmailRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)


class ResetRequest(BaseModel):
    token: str = Field(min_length=8, max_length=512)
    password: str = Field(min_length=1, max_length=1024)


def _public_base_url() -> str:
    return (os.getenv("PUBLIC_BASE_URL") or "https://localhost").rstrip("/")


async def _issue_token(db, user_id: int, purpose: TokenPurpose) -> str:
    """Creates a single-use token and returns the plaintext to email.

    Any outstanding token for the same purpose is consumed first, so requesting
    a new link silently invalidates the old one. Without that, every resend
    leaves another working key to the account lying in an inbox.
    """
    await db.execute(
        """
        UPDATE auth_tokens SET consumed_at = NOW()
         WHERE user_id = $1 AND purpose = $2 AND consumed_at IS NULL
        """,
        user_id, purpose.value,
    )
    token, token_hash = generate_token()
    await db.execute(
        """
        INSERT INTO auth_tokens (user_id, token_hash, purpose, expires_at)
        VALUES ($1, $2, $3, $4)
        """,
        user_id, token_hash, purpose.value, expiry_for(purpose),
    )
    return token


async def _consume_token(db, token: str, purpose: TokenPurpose) -> Optional[int]:
    """Validates and burns a token, returning its user id.

    Looked up by hash, because the plaintext is never stored. Returns None for
    anything unusable -- unknown, wrong purpose, already used, or expired -- so
    callers cannot accidentally distinguish those cases for an attacker.
    """
    row = await db.query_one(
        """
        SELECT id, user_id, expires_at, consumed_at
          FROM auth_tokens WHERE token_hash = $1 AND purpose = $2
        """,
        hash_token(token), purpose.value,
    )
    if not row or row.get("consumed_at") is not None:
        return None
    if is_expired(row.get("expires_at")):
        return None
    # Marked consumed in the same statement that checks it is unconsumed, so two
    # concurrent submissions of the same link cannot both succeed.
    burned = await db.query_one(
        """
        UPDATE auth_tokens SET consumed_at = NOW()
         WHERE id = $1 AND consumed_at IS NULL
         RETURNING user_id
        """,
        row["id"],
    )
    return int(burned["user_id"]) if burned else None


async def _send_verification(db, user_id: int, email: str) -> bool:
    token = await _issue_token(db, user_id, TokenPurpose.VERIFY_EMAIL)
    link = f"{_public_base_url()}/verify?token={token}"
    subject, text, html = verification_email(link)
    return await send_email(email, subject, text, html)


@router.post("/signup", status_code=201)
async def signup(
    body: SignupRequest,
    request: Request,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """Creates a free-tier account and emails a confirmation link.

    The response is identical whether or not the address was already registered.
    Saying "that email is taken" turns signup into a membership oracle: anyone
    could test a list of addresses against the service. An address that already
    has an account instead receives mail saying so.
    """
    email = normalize_email(body.email)

    if redis:
        allowed = await check_rate_limit(
            redis, f"signup:{client_address(request)}",
            max_tokens=_SIGNUP_BURST, refill_rate_per_sec=_SIGNUP_REFILL_PER_SEC,
        )
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail="Too many sign-up attempts from this address. Try again shortly.",
            )

    if not is_valid_email(email):
        raise HTTPException(status_code=400, detail="That does not look like an email address.")
    if len(body.password) < MIN_PASSWORD_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Choose a password of at least {MIN_PASSWORD_LENGTH} characters.",
        )
    if db is None:
        raise HTTPException(status_code=503, detail="Sign-up is temporarily unavailable.")

    # Telling someone to check their inbox when the deployment cannot send mail
    # leaves them waiting for a link that will never arrive. The claim is only
    # made when it is true.
    mail_works = mailer_configured()
    generic = {
        "success": True,
        "email_sent": mail_works,
        "message": (
            "Check your email to confirm your address. Your free account is ready to use."
            if mail_works else
            "Your free account is ready -- sign in now. Email confirmation is not "
            "available on this deployment yet, so there is nothing to confirm."
        ),
    }

    existing = await db.query_one("SELECT id, email FROM users WHERE email = $1", email)
    if existing:
        # Do not create, do not error. Tell the owner of the address instead.
        await send_email(
            email,
            "Someone tried to create a Sentinel account with your email",
            "An account already exists for this address.\n\n"
            f"If that was you, sign in at {_public_base_url()}/login "
            "or use the password reset link there.\n\n"
            "If it was not you, nothing has changed and no action is needed.\n",
        )
        return generic

    try:
        row = await db.query_one(
            """
            INSERT INTO users (email, password_hash, display_name, role, subscription_tier)
            VALUES ($1, $2, $3, 'VIEWER', 'free')
            ON CONFLICT (email) DO NOTHING
            RETURNING id
            """,
            email, hash_password(body.password), (body.display_name or "").strip() or None,
        )
    except Exception as e:
        logger.error("Could not create account for %s: %s", email, e)
        raise HTTPException(status_code=503, detail="Sign-up is temporarily unavailable.")

    if not row:
        # Lost a race with a concurrent signup for the same address.
        return generic

    user_id = int(row["id"])
    logger.info("Created free-tier account %s (%s)", user_id, email)
    await _send_verification(db, user_id, email)
    return generic


@router.post("/verify")
async def verify_email(body: TokenRequest, db=Depends(get_db_optional)):
    """Confirms an address from an emailed link."""
    if db is None:
        raise HTTPException(status_code=503, detail="Verification is temporarily unavailable.")
    user_id = await _consume_token(db, body.token, TokenPurpose.VERIFY_EMAIL)
    if user_id is None:
        raise HTTPException(
            status_code=400,
            detail="That confirmation link is invalid or has expired. Request a new one.",
        )
    await db.execute(
        "UPDATE users SET email_verified = TRUE, email_verified_at = NOW() WHERE id = $1",
        user_id,
    )
    logger.info("Confirmed email for account %s", user_id)
    return {"success": True, "message": "Your email is confirmed."}


@router.post("/resend-verification")
async def resend_verification(
    body: EmailRequest,
    request: Request,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """Issues a fresh confirmation link, invalidating any previous one."""
    email = normalize_email(body.email)
    if redis:
        allowed = await check_rate_limit(
            redis, f"resend:{client_address(request)}",
            max_tokens=3, refill_rate_per_sec=0.02,
        )
        if not allowed:
            raise HTTPException(status_code=429, detail="Try again in a few minutes.")
    generic = {"success": True, "message": "If that address needs confirming, a new link is on its way."}
    if db is None:
        return generic
    row = await db.query_one("SELECT id, email_verified FROM users WHERE email = $1", email)
    if row and not row.get("email_verified"):
        await _send_verification(db, int(row["id"]), email)
    return generic


@router.post("/forgot-password")
async def forgot_password(
    body: EmailRequest,
    request: Request,
    db=Depends(get_db_optional),
    redis=Depends(get_redis_optional),
):
    """Emails a reset link when the address is both known and confirmed.

    Unconfirmed addresses are skipped deliberately: sending a reset link to an
    address nobody has proven they control would let a mistyped signup hand out
    access to whoever actually owns that mailbox.
    """
    email = normalize_email(body.email)
    if redis:
        allowed = await check_rate_limit(
            redis, f"forgot:{client_address(request)}",
            max_tokens=3, refill_rate_per_sec=0.02,
        )
        if not allowed:
            raise HTTPException(status_code=429, detail="Try again in a few minutes.")

    generic = {"success": True, "message": "If that address has an account, a reset link is on its way."}
    if db is None:
        return generic

    row = await db.query_one(
        "SELECT id, email_verified, is_active FROM users WHERE email = $1", email
    )
    if row and row.get("email_verified") and row.get("is_active", True):
        token = await _issue_token(db, int(row["id"]), TokenPurpose.RESET_PASSWORD)
        subject, text, html = reset_email(f"{_public_base_url()}/reset?token={token}")
        await send_email(email, subject, text, html)
    return generic


@router.post("/reset-password")
async def reset_password(body: ResetRequest, db=Depends(get_db_optional)):
    """Sets a new password from a reset link."""
    if db is None:
        raise HTTPException(status_code=503, detail="Password reset is temporarily unavailable.")
    if len(body.password) < MIN_PASSWORD_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Choose a password of at least {MIN_PASSWORD_LENGTH} characters.",
        )
    user_id = await _consume_token(db, body.token, TokenPurpose.RESET_PASSWORD)
    if user_id is None:
        raise HTTPException(
            status_code=400,
            detail="That reset link is invalid or has expired. Request a new one.",
        )
    await db.execute(
        "UPDATE users SET password_hash = $1 WHERE id = $2",
        hash_password(body.password), user_id,
    )
    # Any other outstanding reset link is now void: a password change is exactly
    # when a link sitting in a compromised inbox must stop working.
    await db.execute(
        """
        UPDATE auth_tokens SET consumed_at = NOW()
         WHERE user_id = $1 AND purpose = $2 AND consumed_at IS NULL
        """,
        user_id, TokenPurpose.RESET_PASSWORD.value,
    )
    logger.info("Password reset completed for account %s", user_id)
    return {"success": True, "message": "Your password has been changed. Sign in with it now."}
