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


def test_temporal_anomaly_scorer_batching():
    async def run_test():
        import numpy as np
        redis_mock = MagicMock()
        redis_mock.raw = None

        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        mock_session = MagicMock()
        mock_input = MagicMock()
        mock_input.name = "input_seq"
        mock_session.get_inputs.return_value = [mock_input]

        # 3 valid sequences, shape (3, 10, 5)
        mock_session.run.side_effect = lambda output_names, input_feed: [np.zeros_like(input_feed["input_seq"])]
        scorer.sessions["temporal"] = mock_session

        # Mock sequence retriever to return 3 valid length-10 sequences
        sample_seq = [[0.1] * 5] * 10
        scorer._get_temporal_sequence_batch = AsyncMock(return_value=[sample_seq, sample_seq, sample_seq])

        entities = ["BTC", "ETH", "SOL"]
        features_list = [[1.0] * 5, [2.0] * 5, [3.0] * 5]

        results = await scorer.score_event_batch("crypto_trade", entities, features_list)

        # Ensure session.run was called exactly ONCE with full batch of shape (3, 10, 5)
        assert mock_session.run.call_count == 1
        call_args = mock_session.run.call_args[0]
        input_feed = call_args[1]
        assert "input_seq" in input_feed
        assert input_feed["input_seq"].shape == (3, 10, 5)
        assert len(results) == 3
        assert all(r["domain"] == "temporal" for r in results)

    import asyncio
    asyncio.run(run_test())


