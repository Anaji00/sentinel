"""
tests/test_macro_intelligence_engine.py

Unit tests for Macro Intelligence Engine Z-Score Correctness, Degenerate Spread Guard,
Z-Score Clamping, Fail-Closed Exposure Lookup, and Ticker Metadata Enrichment.
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from services.agents.macro_intelligence_engine import (
    MacroIntelligenceEngine,
    TICKER_METADATA_REGISTRY,
)
from shared.utils import quant_calc


def _make_agent(redis_mock=None, producer_mock=None, neo4j_mock=None):
    redis_mock = redis_mock or MagicMock()
    producer_mock = producer_mock or AsyncMock()
    neo4j_mock = neo4j_mock or MagicMock()
    db_mock = MagicMock()
    consumer_mock = AsyncMock()
    dlq_mock = AsyncMock()

    return MacroIntelligenceEngine(
        agent_name="macro_intelligence",
        input_topics=["events.enriched"],
        redis_client=redis_mock,
        db_client=db_mock,
        neo4j_client=neo4j_mock,
        producer=producer_mock,
        consumer=consumer_mock,
        dlq=dlq_mock,
    )


def test_decoupling_degenerate_spread_rejected():
    """
    Test simulating the exact reported case: when spread_std is small/degenerate (< 1e-4),
    _process_pair_cointegration must skip emitting a decoupling signal entirely.
    """
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.set = AsyncMock()

        # Mock pipeline execution to return time series data
        pipe_mock = AsyncMock()
        # 5th and 6th pipeline results are raw_x and raw_y
        macro_prices = [100.0 + i * 0.1 for i in range(35)]
        micro_prices = [150.0 + i * 0.15 for i in range(35)]
        pipe_mock.execute = AsyncMock(return_value=[None, None, None, None, macro_prices, micro_prices])
        redis_mock.raw.pipeline = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=pipe_mock), __aexit__=AsyncMock()))

        producer_mock = AsyncMock()

        agent = _make_agent(redis_mock=redis_mock, producer_mock=producer_mock)

        # Mock quant_calc.engle_granger_cointegration to return cointegrated=True with degenerate spread_std (e.g. 1e-5)
        with patch("shared.utils.quant_calc.engle_granger_cointegration") as mock_eg:
            mock_eg.return_value = {
                "is_cointegrated": True,
                "beta": 1.5,
                "alpha": 0.0,
                "half_life": 10.0,
                "spread_std": 1e-5,  # Small degenerate spread std (< 1e-4)
            }

            await agent._process_pair_cointegration("ZC=F", "TIP", 103.4, 155.1)

            # Assert no signal emitted (neither redis.set nor producer.send called)
            redis_mock.raw.set.assert_not_called()
            producer_mock.send.assert_not_called()

    asyncio.run(run_test())


def test_decoupling_z_score_clamped_and_enriched():
    """
    Test that a valid cointegrated pair with large residual is clamped to [-5.0, 5.0]
    and enriched with human-readable ticker metadata names.
    """
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.set = AsyncMock()

        pipe_mock = AsyncMock()
        macro_prices = [100.0 + i * 0.1 for i in range(35)]
        micro_prices = [150.0 + i * 0.15 for i in range(35)]
        pipe_mock.execute = AsyncMock(return_value=[None, None, None, None, macro_prices, micro_prices])
        redis_mock.raw.pipeline = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=pipe_mock), __aexit__=AsyncMock()))

        producer_mock = AsyncMock()

        agent = _make_agent(redis_mock=redis_mock, producer_mock=producer_mock)

        with patch("shared.utils.quant_calc.engle_granger_cointegration") as mock_eg:
            mock_eg.return_value = {
                "is_cointegrated": True,
                "beta": 1.0,
                "alpha": 0.0,
                "half_life": 5.0,
                "spread_std": 1.0,
            }

            await agent._process_pair_cointegration("ZC=F", "TIP", 103.4, 155.1)

            # Assert signal was emitted
            assert producer_mock.send.call_count == 1
            call_args = producer_mock.send.call_args[0]
            topic, payload = call_args[0], call_args[1]

            from shared.kafka import Topics
            assert topic == Topics.MACRO_DECOUPLING
            assert payload["z_score"] == 5.0  # Clamped to 5.0
            assert payload["macro_asset"] == "ZC=F"
            assert payload["micro_ticker"] == "TIP"
            assert payload["macro_asset_name"] == "Corn Futures (CBOT)"
            assert payload["micro_ticker_name"] == "iShares TIPS Bond ETF"

    asyncio.run(run_test())


def test_get_exposed_instruments_fail_closed():
    """
    Test that _get_exposed_instruments returns an empty list [] when Neo4j exposure graph
    has no edges for the macro asset, rather than scanning unconstrained quotes in Redis.
    """
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=None)
        redis_mock.raw.set = AsyncMock()
        redis_mock.raw.scan = AsyncMock()

        neo4j_mock = AsyncMock()
        neo4j_mock.query = AsyncMock(return_value=[])  # No edges returned from graph

        agent = _make_agent(redis_mock=redis_mock, neo4j_mock=neo4j_mock)

        with patch("services.agents.macro_intelligence_engine.get_neo4j", return_value=neo4j_mock):
            result = await agent._get_exposed_instruments("ZC=F")

            assert result == []
            assert "TIP" not in result
            # Redis scan should NOT be called
            redis_mock.raw.scan.assert_not_called()

    asyncio.run(run_test())
