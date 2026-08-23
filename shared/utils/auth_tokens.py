"""
shared/utils/auth_tokens.py

Single-use, expiring tokens for email verification and password reset.

The token is generated once, emailed, and never stored. Only its SHA-256 digest
goes to the database, so a database read yields no working links. That matters
more for reset than for verification: a leaked reset token is an account
takeover, and backups, replicas and query logs all count as reads.

SHA-256 rather than scrypt here, deliberately. These tokens are 256 bits of
`secrets` output, so there is no dictionary to attack and no work factor worth
paying on every verification click; the slow KDF in accounts.py exists because
human-chosen passwords are guessable, which these are not.
"""

import hashlib
import hmac
import logging
import secrets
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, Tuple

from shared.utils.accounts import coerce_utc

logger = logging.getLogger("shared.auth_tokens")

# 256 bits, URL-safe. Long enough that guessing is not a threat model.
TOKEN_BYTES = 32


class TokenPurpose(str, Enum):
    """Purposes are separated so a verification link cannot reset a password."""
    VERIFY_EMAIL = "verify_email"
    RESET_PASSWORD = "reset_password"


# Verification is generous: people sign up, get distracted, and come back.
# Reset is deliberately short -- it is a live key to the account, and the user
# is by definition sitting at their inbox when they request one.
TTL = {
    TokenPurpose.VERIFY_EMAIL: timedelta(hours=48),
    TokenPurpose.RESET_PASSWORD: timedelta(minutes=30),
}


def generate_token() -> Tuple[str, str]:
    """Returns `(token, token_hash)`. Only the hash is ever persisted."""
    token = secrets.token_urlsafe(TOKEN_BYTES)
    return token, hash_token(token)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def tokens_match(token: str, stored_hash: str) -> bool:
    """Constant-time comparison, so a near-miss cannot be found by timing."""
    if not token or not stored_hash:
        return False
    return hmac.compare_digest(hash_token(token), stored_hash)


def expiry_for(purpose: TokenPurpose, now: Optional[datetime] = None) -> datetime:
    base = now or datetime.now(timezone.utc)
    return base + TTL[purpose]


def is_expired(expires_at, now: Optional[datetime] = None) -> bool:
    """True when a token has expired.

    Accepts a datetime or the ISO string the database layer actually returns --
    it serialises every timestamp before the row reaches application code, so
    reading `.tzinfo` here raised and turned a valid confirmation link into a
    500. Anything missing or unparseable counts as expired: fail closed.
    """
    parsed = coerce_utc(expires_at)
    if parsed is None:
        return True
    return (now or datetime.now(timezone.utc)) >= parsed
