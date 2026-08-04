"""
tests/test_anomaly_scorer.py

Unit tests for services/enrichment/anomaly_scorer.py.
Validates streaming RRCF detectors, kinematic Kalman scoring, FSD news novelty,
vessel dark STS context weighting, and BGP graph-topology scoring.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from services.enrichment.anomaly_scorer import DynamicAnomalyScorer


def test_dynamic_anomaly_scorer_requires_redis():
    with pytest.raises(ValueError, match="Redis client is required"):
        DynamicAnomalyScorer(redis_client=None)


def test_dynamic_anomaly_scorer_streaming_batch():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        # Test RRCF streaming batch scoring
        entities = ["VESSEL_1", "VESSEL_2"]
        features_list = [[1.0, 2.0, 3.0], [1.1, 2.1, 3.1]]
        results = await scorer.score_event_batch("vessel_position", entities, features_list)

        assert len(results) == 2
        assert all(0.0 <= r["score"] <= 1.0 for r in results)
        assert all(r["domain"] == "spatial" for r in results)

    asyncio.run(run_test())


def test_kinematic_kalman_scoring():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        # Normal sequential positions
        r1 = await scorer.score_kinematic_event(
            "MMSI_123456789", lat=25.0, lon=55.0, speed=12.0, heading=90, timestamp=1000.0
        )
        assert 0.0 <= r1["score"] <= 1.0
        assert "residual_distance" in r1

        r2 = await scorer.score_kinematic_event(
            "MMSI_123456789", lat=25.05, lon=55.05, speed=12.0, heading=90, timestamp=1900.0
        )
        assert 0.0 <= r2["score"] <= 1.0

    asyncio.run(run_test())


def test_vessel_dark_sts_context():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        # 3-hour gap in open ocean vs 3-hour gap in Strait of Hormuz
        score_ocean = await scorer.score_vessel_dark("MMSI_1", gap_hours=3.0, region="Mid Atlantic", flags=[], heading=90)
        score_hormuz = await scorer.score_vessel_dark("MMSI_2", gap_hours=3.0, region="Strait of Hormuz", flags=[], heading=90)

        # Gap near Hormuz (STS transfer zone) must score significantly higher!
        assert score_hormuz > score_ocean

    asyncio.run(run_test())


def test_news_fsd_novelty_scoring():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        score, tags = await scorer.score_news(
            named_entities=["AAPL", "Fed"],
            sentiment=-0.8,
            reliability=0.9,
            headline="Federal Reserve announces emergency rate hike after inflation surge",
            summary="The central bank took aggressive action following higher CPI print."
        )
        assert 0.0 <= score <= 1.0
        assert isinstance(tags, list)

    asyncio.run(run_test())


def test_bgp_graph_topology_scoring():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        res = await scorer.score_bgp_event(
            origin_as="AS64500",
            prefix="1.2.3.0/24",
            is_hijack=True,
            velocity=0.8,
        )
        assert res["score"] >= 0.85
        assert res["domain"] == "cyber"
        assert "path_novelty" in res

    asyncio.run(run_test())
