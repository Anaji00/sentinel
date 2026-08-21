"""
tests/conftest.py

Shared pytest configuration & fixtures.

Sets required environment variables BEFORE any application modules are imported,
ensuring fail-closed secrets validation doesn't block the test suite.
"""

import os
import pytest

# ── Required env vars for test suite ──────────────────────────────────────────
# These MUST be set before any imports of services.api_gateway.dependencies,
# because SESSION_SECRET is resolved at module load time with required=True.
os.environ.setdefault("SENTINEL_ENV", "test")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-not-for-production")
os.environ.setdefault("API_GATEWAY_KEY", "test-api-key-not-for-production")


@pytest.fixture
def anyio_backend():
    return 'asyncio'
