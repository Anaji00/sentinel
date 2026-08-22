"""
shared/utils/accounts.py

User accounts, password hashing, and subscription tiers.

Before this the platform had exactly one account, defined by ADMIN_EMAIL and
ADMIN_PASSWORD in the environment and checked with a plaintext `==` comparison.
That is defensible for a single self-hosted operator and unacceptable the moment
real people sign up, because a database read then exposes every password.

Hashing uses `hashlib.scrypt` from the standard library rather than bcrypt or
argon2. scrypt is a memory-hard KDF designed for exactly this, it is available
without adding a dependency, and the parameters are stored alongside each hash
so they can be raised later without invalidating existing accounts.

WHAT IS DELIBERATELY NOT STORED: card numbers, CVCs, billing addresses. Stripe
holds those. This module keeps a customer id and a cached subscription status,
which is the minimum needed to answer "may this request proceed" without a
round-trip, and keeps the platform out of PCI scope entirely.
"""

import base64
import hashlib
import hmac
import logging
import os
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger("shared.accounts")

# ── Password hashing ─────────────────────────────────────────────────────────
# scrypt cost parameters. n=2**15 with r=8 costs roughly 32 MB and ~100ms per
# hash on a modern CPU -- slow enough to make offline cracking expensive, fast
# enough that a login does not feel broken. Stored per-hash so raising them
# later re-hashes on next successful login instead of locking everyone out.
SCRYPT_N = 2 ** 15
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_DKLEN = 64
SALT_BYTES = 16

# scrypt needs 128 * N * r * p bytes. At N=2**15, r=8 that is exactly 32 MiB,
# which is OpenSSL's default maxmem ceiling -- landing on the boundary raises
# "memory limit exceeded". Stated explicitly with headroom so the parameters
# above can be raised without tripping an unrelated limit.
SCRYPT_MAXMEM = 128 * SCRYPT_N * SCRYPT_R * SCRYPT_P * 2

MIN_PASSWORD_LENGTH = 12

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def hash_password(password: str) -> str:
    """Returns a self-describing scrypt hash: `scrypt$n$r$p$salt$key`.

    Embedding the parameters means a hash produced under today's cost settings
    stays verifiable after they are raised.
    """
    if not password or len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters.")

    salt = secrets.token_bytes(SALT_BYTES)
    key = hashlib.scrypt(
        password.encode("utf-8"), salt=salt,
        n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P, dklen=SCRYPT_DKLEN,
        maxmem=SCRYPT_MAXMEM,
    )
    b64 = lambda b: base64.b64encode(b).decode("ascii")
    return f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}${b64(salt)}${b64(key)}"


def verify_password(password: str, stored: str) -> bool:
    """Constant-time verification against a stored hash.

    Returns False for any malformed hash rather than raising: a corrupt row must
    fail the login, not crash the endpoint and reveal that the row is corrupt.
    """
    if not password or not stored:
        return False
    try:
        scheme, n, r, p, salt_b64, key_b64 = stored.split("$")
        if scheme != "scrypt":
            return False
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(key_b64)
        # maxmem derived from the hash's own parameters, so a hash made
        # under different settings still verifies.
        n_i, r_i, p_i = int(n), int(r), int(p)
        computed = hashlib.scrypt(
            password.encode("utf-8"), salt=salt,
            n=n_i, r=r_i, p=p_i, dklen=len(expected),
            maxmem=128 * n_i * r_i * p_i * 2,
        )
        # compare_digest, not ==: a short-circuiting comparison leaks how much
        # of the hash matched through timing.
        return hmac.compare_digest(computed, expected)
    except (ValueError, TypeError, KeyError) as e:
        logger.warning("Malformed password hash encountered: %s", e)
        return False


def needs_rehash(stored: str) -> bool:
    """True when a hash was made under weaker parameters than current policy."""
    try:
        scheme, n, r, p, _, _ = stored.split("$")
        return scheme != "scrypt" or int(n) < SCRYPT_N or int(r) < SCRYPT_R
    except (ValueError, TypeError):
        return True


def normalize_email(email: str) -> str:
    """Lowercased and trimmed. Stored and compared in this form throughout."""
    return (email or "").strip().lower()


def is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(normalize_email(email)))


# ── Tiers ────────────────────────────────────────────────────────────────────

class Tier(str, Enum):
    """What an account is entitled to.

    FREE is the whole analyst platform: ingestion, correlation, the knowledge
    graph, every dashboard. It is a complete product, not a teaser -- a free
    tier that is visibly crippled converts worse than one that is genuinely
    useful and simply stops short of the expensive part.

    PRO adds the reasoning tier: the LLM agent swarm, scenario generation, and
    agent-authored briefs. That is the part with real marginal cost (CPU
    inference), which is what makes it the natural paid boundary.
    """
    FREE = "free"
    PRO = "pro"


# Subscription states Stripe reports that still grant access. `past_due` is
# included deliberately: a failed card retry should not lock someone out
# mid-cycle while Stripe is still retrying, and dunning handles the rest.
ACTIVE_STATUSES = frozenset({"active", "trialing", "past_due"})

# Capabilities gated by tier. Named rather than checked inline so a new gate is
# a data change and every gate is enumerable in one place.
PRO_FEATURES = frozenset({
    "reasoning",          # LLM agent swarm
    "scenarios",          # generated scenario briefs
    "agent_briefs",       # agent-authored analysis
    "backtest_unlimited", # unrestricted backtest runs
})

# Free-tier limits. Not zero -- a free user should be able to try the paid
# surface enough to know whether they want it.
FREE_TIER_LIMITS = {
    "backtest_runs_per_day": 5,
    "watchlist_symbols": 25,
    "api_requests_per_hour": 500,
}


@dataclass
class Account:
    """An authenticated user and what they are entitled to."""
    id: int
    email: str
    display_name: Optional[str]
    role: str
    tier: Tier
    subscription_status: str
    subscription_ends_at: Optional[datetime]
    stripe_customer_id: Optional[str] = None
    is_active: bool = True

    @property
    def has_pro(self) -> bool:
        """True when this account may use paid features right now.

        Checks tier *and* status: a cancelled subscription can leave the tier
        set to pro until period end, and an expired one must not keep access
        merely because nothing rewrote the row.
        """
        if self.tier is not Tier.PRO:
            return False
        if self.subscription_status not in ACTIVE_STATUSES:
            return False
        if self.subscription_ends_at is not None:
            ends = self.subscription_ends_at
            if ends.tzinfo is None:
                ends = ends.replace(tzinfo=timezone.utc)
            if ends < datetime.now(timezone.utc):
                return False
        return True

    def can_use(self, feature: str) -> bool:
        """Whether *feature* is available to this account."""
        return self.has_pro if feature in PRO_FEATURES else True

    def to_public_dict(self) -> Dict[str, Any]:
        """Client-safe view. Carries no Stripe identifiers and no hash."""
        return {
            "email": self.email,
            "display_name": self.display_name,
            "role": self.role,
            "tier": self.tier.value,
            "has_pro": self.has_pro,
            "subscription_status": self.subscription_status,
            "subscription_ends_at": (
                self.subscription_ends_at.isoformat() if self.subscription_ends_at else None
            ),
            "pro_features": sorted(PRO_FEATURES),
            "free_limits": FREE_TIER_LIMITS,
        }


def account_from_row(row: Dict[str, Any]) -> Account:
    """Builds an Account from a `users` row."""
    raw_tier = str(row.get("subscription_tier") or "free").lower()
    tier = Tier.PRO if raw_tier == "pro" else Tier.FREE
    return Account(
        id=int(row["id"]),
        email=row["email"],
        display_name=row.get("display_name"),
        role=str(row.get("role") or "VIEWER"),
        tier=tier,
        subscription_status=str(row.get("subscription_status") or "none"),
        subscription_ends_at=row.get("subscription_ends_at"),
        stripe_customer_id=row.get("stripe_customer_id"),
        is_active=bool(row.get("is_active", True)),
    )
