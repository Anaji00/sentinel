"""
shared/utils/secrets.py

Centralized, zero-leak secrets management for Sentinel.
Provides typed access, validation, logging sanitization, and runtime environment audits.
"""

import os
import re
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger("shared.secrets")

# Known sensitive keys that should always be masked
SENSITIVE_KEY_PATTERNS = [
    r".*KEY.*",
    r".*SECRET.*",
    r".*TOKEN.*",
    r".*PASSWORD.*",
    r".*AUTH.*",
    r".*PRIVATE.*",
    r".*CREDENTIAL.*",
]

_COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in SENSITIVE_KEY_PATTERNS]


def is_sensitive_key(key: str) -> bool:
    """Checks if a key name suggests sensitive credential data."""
    return any(p.match(key) for p in _COMPILED_PATTERNS)


def mask_secret(value: Optional[str], visible_chars: int = 4) -> str:
    """
    Masks a secret string for safe logging / telemetry.
    E.g. 'sk_live_1234567890abcdef' -> 'sk_l************cdef'
    """
    if not value:
        return "<UNSET>"
    val_str = str(value)
    if len(val_str) <= visible_chars * 2:
        return "*" * len(val_str)
    prefix = val_str[:visible_chars]
    suffix = val_str[-visible_chars:]
    masked_middle = "*" * (len(val_str) - (visible_chars * 2))
    return f"{prefix}{masked_middle}{suffix}"


def get_secret(
    key: str,
    default: Optional[str] = None,
    required: bool = False,
    validate_non_empty: bool = True,
) -> Optional[str]:
    """
    Retrieves a secret / configuration environment variable.
    Raises ValueError if required and not found or empty.
    """
    val = os.getenv(key)
    if val is None or (validate_non_empty and not val.strip()):
        if required:
            raise ValueError(f"CRITICAL: Required secret/environment variable '{key}' is missing or empty.")
        return default
    return val.strip()


def get_secret_bool(key: str, default: bool = False) -> bool:
    """Retrieves a boolean configuration variable."""
    val = os.getenv(key)
    if val is None:
        return default
    return val.strip().lower() in ("true", "1", "yes", "t", "on")


def get_secret_int(key: str, default: int = 0) -> int:
    """Retrieves an integer configuration variable."""
    val = os.getenv(key)
    if val is None:
        return default
    try:
        return int(val.strip())
    except ValueError:
        logger.warning(f"Invalid integer value for {key}: '{val}'. Falling back to default: {default}")
        return default


def audit_secrets_environment() -> Dict[str, Any]:
    """
    Audits the current execution environment against key Sentinel services.
    Returns status map with masked previews — safe for health endpoints and admin views.
    """
    critical_keys = [
        ("POSTGRES_HOST", False),
        ("POSTGRES_PORT", False),
        ("POSTGRES_DB", False),
        ("POSTGRES_USER", False),
        ("POSTGRES_PASSWORD", True),
        ("REDIS_HOST", False),
        ("REDIS_PORT", False),
        ("REDIS_PASSWORD", True),
        ("KAFKA_BOOTSTRAP_SERVERS", False),
        ("NEO4J_URI", False),
        ("NEO4J_USER", False),
        ("NEO4J_PASSWORD", True),
        ("FINNHUB_API_KEY", True),
        ("ALPACA_API_KEY", True),
        ("ALPACA_SECRET_KEY", True),
        ("TELEGRAM_BOT_TOKEN", True),
        ("TELEGRAM_CHAT_ID", False),
        ("JWT_SECRET_KEY", True),
    ]

    report = {}
    configured_count = 0
    missing_count = 0

    for key, is_secret in critical_keys:
        val = os.getenv(key)
        configured = bool(val and val.strip())
        if configured:
            configured_count += 1
            preview = mask_secret(val) if is_secret else val
        else:
            missing_count += 1
            preview = "<UNSET>"

        report[key] = {
            "configured": configured,
            "is_sensitive": is_secret,
            "preview": preview,
        }

    return {
        "total_audited": len(critical_keys),
        "configured_count": configured_count,
        "missing_count": missing_count,
        "readiness_ratio": round(configured_count / len(critical_keys), 2),
        "keys": report,
    }
