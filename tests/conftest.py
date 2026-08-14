"""
tests/conftest.py

Shared pytest configuration & fixtures.
"""

import pytest

@pytest.fixture
def anyio_backend():
    return 'asyncio'
