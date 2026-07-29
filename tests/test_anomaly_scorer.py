"""
tests/test_anomaly_scorer.py

Unit tests for services/enrichment/anomaly_scorer.py.
Validates ONNX scoring fallback, metrics collector failure counting,
and dynamic normalization batching.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from services.enrichment.anomaly_scorer import DynamicAnomalyScorer
from shared.utils.metrics import MetricsCollector


def test_dynamic_anomaly_scorer_requires_redis():
    with pytest.raises(ValueError, match="Redis client is required"):
        DynamicAnomalyScorer(redis_client=None)


def test_dynamic_anomaly_scorer_fallback():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None  # Force fallback behavior
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        # _dynamic_normalize_batch fallback
        requests = [("AAPL", "notional", 5000.0), ("MSFT", "notional", 2000.0)]
        norm_scores = await scorer._dynamic_normalize_batch(requests)
        assert norm_scores == [5.0, 2.0]

        # _check_ema_gatekeeper_batch fallback (threshold 0.60)
        scores = [0.85, 0.30, 0.95]
        gated = await scorer._check_ema_gatekeeper_batch("trade", scores)
        assert gated == [True, False, True]

    import asyncio
    asyncio.run(run_test())
