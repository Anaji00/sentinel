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



def pytest_configure(config):
    """Register the marks used outside the unit suite.

    Unregistered marks are a warning, not an error, so a typo in one silently
    selects nothing -- `-m integraton` would run zero tests and exit green.
    """
    config.addinivalue_line(
        "markers", "integration: needs the compose stack running; skipped otherwise"
    )

@pytest.fixture
def anyio_backend():
    return 'asyncio'
