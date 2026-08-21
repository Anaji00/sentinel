"""
shared/utils/env_guard.py

Fail-closed credential resolution. Replaces the fail-open pattern of
"raise only if SENTINEL_ENV matches a prod-like blacklist" with a
whitelist: hardcoded dev fallbacks are permitted ONLY when SENTINEL_ENV
is explicitly one of a known-safe set. An unset, misspelled, or unknown
SENTINEL_ENV now fails startup instead of silently running with a
hardcoded credential.

Usage:
    from shared.utils.env_guard import resolve_env_var

    dsn = resolve_env_var("DATABASE_URL", "postgresql://sentinel:sentinel_local_dev@localhost:5432/sentinel")
    api_key = resolve_env_var("API_GATEWAY_KEY", "dev-only-key-replace-in-prod", warn_on_fallback=True)
"""

import logging
import os

logger = logging.getLogger(__name__)

# Only these SENTINEL_ENV values allow fallback to hardcoded dev credentials.
# Everything else — unset, empty, misspelled, or production-like — triggers
# a hard RuntimeError at startup.
SAFE_DEV_ENVS = frozenset({"dev", "development", "local", "test", "testing"})


def resolve_env_var(
    var_name: str,
    dev_fallback: str,
    *,
    warn_on_fallback: bool = False,
) -> str:
    """Return the value of *var_name* from the environment, or fail safely.

    Resolution order:
      1. ``os.environ[var_name]`` — returned verbatim if set and non-empty.
      2. If missing/empty, check ``SENTINEL_ENV``:
         a. If ``SENTINEL_ENV`` is in :data:`SAFE_DEV_ENVS`, return
            *dev_fallback* (optionally logging a security warning).
         b. Otherwise, raise :class:`RuntimeError` — the process must not
            start with hardcoded credentials in an unknown environment.
    """
    value = os.getenv(var_name)
    if value:
        return value

    env_name = os.getenv("SENTINEL_ENV", "").lower().strip()

    if env_name in SAFE_DEV_ENVS:
        if warn_on_fallback:
            logger.warning(
                "SECURITY WARNING: %s not set. Falling back to default "
                "dev value (SENTINEL_ENV=%r).",
                var_name,
                env_name,
            )
        return dev_fallback

    raise RuntimeError(
        f"CRITICAL SECURITY FAILURE: {var_name} environment variable is "
        f"missing and SENTINEL_ENV={env_name!r} is not in the safe "
        f"development whitelist {sorted(SAFE_DEV_ENVS)}. Refusing to start "
        f"with hardcoded credentials."
    )
