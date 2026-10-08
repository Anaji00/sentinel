"""
shared/utils/session_revocation.py

Ending sessions when a password changes.

Session cookies are stateless HMAC tokens carrying an email, a role and an
expiry, valid for 24 hours, and the gateway trusted them without consulting the
account. So a password reset -- the moment a stolen session most needs to stop
working -- left every existing session alive for up to a day.

A reset now records, per account, the time before which sessions are void. A
token carries no issue time, but every token is minted with the same lifetime
(the login route's `maxAge`), so its issue time is its expiry minus that
lifetime. The record outlives the longest session and then expires itself.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from typing import Optional

logger = logging.getLogger("shared.session_revocation")

# Matches `maxAge: 24 * 60 * 60` in frontend/src/app/api/auth/login/route.ts.
SESSION_LIFETIME_SEC = 24 * 3600
REVOKED_BEFORE_KEY = "sentinel:auth:sessions_valid_after:{email}"


def _key(email: str) -> str:
    return REVOKED_BEFORE_KEY.format(email=(email or "").strip().lower())


async def revoke_sessions(redis_client, email: str, now: Optional[float] = None) -> None:
    """Void every session for this account issued before now."""
    raw = getattr(redis_client, "raw", redis_client)
    await raw.set(_key(email), str(now if now is not None else time.time()),
                  ex=SESSION_LIFETIME_SEC * 2)


def token_expiry(token: str) -> Optional[float]:
    """The expiry of a session token, in epoch seconds, or None.

    Call only after the signature has been verified: this reads the payload.
    Handles both the two-part cookie (`email:role:expires`) and a JWT's `exp`.
    """
    if not token:
        return None
    parts = token.strip().split(".")
    try:
        if len(parts) == 3:
            b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            exp = json.loads(base64.urlsafe_b64decode(b64).decode("utf-8")).get("exp")
        elif len(parts) == 2:
            b64 = parts[0] + "=" * (-len(parts[0]) % 4)
            exp = base64.urlsafe_b64decode(b64).decode("utf-8").split(":")[-1]
        else:
            return None
        value = float(exp)
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return value / 1000.0 if value > 1e11 else value


async def is_revoked(redis_client, email: str, token: str) -> bool:
    """Whether this verified token was issued before the account's last reset.

    Fails open when Redis cannot answer: a cache outage must not sign every
    user out, and the token has already passed its signature and expiry checks.
    """
    if redis_client is None or not email:
        return False
    try:
        raw = getattr(redis_client, "raw", redis_client)
        blob = await raw.get(_key(email))
    except Exception as exc:  # noqa: BLE001 -- logged, and the caller proceeds
        logger.warning("Session revocation lookup failed for %s: %s", email, exc)
        return False
    if not blob:
        return False
    expiry = token_expiry(token)
    if expiry is None:
        return True
    valid_after = float(blob.decode() if isinstance(blob, bytes) else blob)
    return (expiry - SESSION_LIFETIME_SEC) < valid_after


__all__ = ["REVOKED_BEFORE_KEY", "SESSION_LIFETIME_SEC", "is_revoked", "revoke_sessions", "token_expiry"]
