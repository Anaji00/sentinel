"""
tests/test_aviation_enricher_batching.py

Tests for AviationEnricher batch parallelization and sub-threshold telemetry retention.
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from shared.models import RawEvent, EventType
from services.enrichment.enrichers.aviation import AviationEnricher
from tests.test_heartbeat_and_liveness import MockRedis


def test_aviation_enricher_batching_and_routine_telemetry_retention():
    async def run_test():
        redis = MockRedis()
        graph = AsyncMock()
        graph.producer = AsyncMock()
        scorer = AsyncMock()
        scorer.score_kinematic_event.return_value = {"score": 0.05}
        scorer.check_watchlist.return_value = False
        scorer.track_frequency.return_value = 0.0

        enricher = AviationEnricher(scorer, redis, graph)

        # 2 raw telemetry events: one routine, one emergency squawk 7700
        raw1 = RawEvent(
            source="opensky",
            occurred_at=datetime.now(timezone.utc),
            raw_payload={
                "icao24": "4b1234",
                "latitude": 25.1,
                "longitude": 55.2,
                "callsign": "UAE123",
                "on_ground": False,
                "velocity": 240.0
            }
        )

        raw2 = RawEvent(
            source="opensky",
            occurred_at=datetime.now(timezone.utc),
            raw_payload={
                "icao24": "4b5678",
                "latitude": 25.5,
                "longitude": 55.8,
                "callsign": "EMERG01",
                "squawk": "7700",
                "on_ground": False,
                "velocity": 300.0
            }
        )

        results = await enricher.enrich_batch([raw1, raw2])

        # Both events must be returned (routine telemetry is NOT dropped!)
        assert len(results) == 2

        # Event 1: Routine flight, score capped at <= 0.15
        e1 = results[0]
        assert e1.type == EventType.FLIGHT_POSITION
        assert e1.anomaly_score <= 0.15

        # Event 2: Emergency flight, high anomaly score
        e2 = results[1]
        assert e2.type == EventType.FLIGHT_ANOMALY
        assert e2.anomaly_score >= 0.80

        # Verify Redis saved aircraft:last_seen for both flights
        val1 = await redis.raw.get("aircraft:last_seen:4b1234")
        val2 = await redis.raw.get("aircraft:last_seen:4b5678")
        assert val1 is not None
        assert val2 is not None

    asyncio.run(run_test())
