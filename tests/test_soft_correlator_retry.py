"""
tests/test_soft_correlator_retry.py

Tests for SoftCorrelator background reconnect loop and status reporting.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from services.correlation.soft_correlator import SoftCorrelator


def test_soft_correlator_retry_loop_recovery():
    async def run_test():
        ollama = AsyncMock()
        correlator = SoftCorrelator(ollama)

        assert correlator.is_enabled is False
        status = correlator.get_status()
        assert status["enabled"] is False

        # Mock Qdrant client connection to fail on first attempt, then succeed
        call_count = 0

        class MockAsyncQdrantClient:
            def __init__(self, host=None, port=None):
                pass

            async def collection_exists(self, collection_name):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("Qdrant connection failed")
                return True

        with patch("sentence_transformers.SentenceTransformer", return_value=MagicMock()):
            with patch("qdrant_client.AsyncQdrantClient", MockAsyncQdrantClient):
                # First load attempt fails cleanly and schedules retry
                await correlator._load()
                assert correlator.is_enabled is False

                # Let the background retry task execute
                if correlator._retry_task:
                    await asyncio.sleep(0.05)

                # Status check reflects live state
                assert correlator.get_status()["model_loaded"] is True

    asyncio.run(run_test())
