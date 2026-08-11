"""
tests/test_aviation_gap_detector.py

Tests for AviationGapDetector FLIGHT_DARK detection and deduplication.
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock

from shared.models import EventType
from services.enrichment.aviation_gap_detector import AviationGapDetector
from tests.test_heartbeat_and_liveness import MockRedis


def test_aviation_gap_detector_fires_flight_dark():
    async def run_test():
        redis = MockRedis()
        producer = AsyncMock()
        scorer = AsyncMock()
        db_writer = AsyncMock()

        # Set healthy ADSB heartbeat
        now = datetime.now(timezone.utc)
        await redis.raw.set("sentinel:heartbeat:collector-adsb", json.dumps({"component": "collector-adsb", "ts": now.isoformat()}))

        # Add aircraft that has been silent for 3 hours in Iranian Airspace (threshold = 0.5h)
        old_ts = (now - timedelta(hours=3)).isoformat()
        await redis.raw.set("aircraft:last_seen:a4b5c6", json.dumps({
            "ts": old_ts,
            "region": "Iranian Airspace",
            "callsign": "IRAN01"
        }))

        detector = AviationGapDetector(producer, scorer, db_writer, redis)
        await detector._check()

        # Verify FLIGHT_DARK was written and produced
        db_writer.write_events_batch.assert_called_once()
        events = db_writer.write_events_batch.call_args[0][0]
        assert len(events) == 1
        assert events[0].type == EventType.FLIGHT_DARK
        assert events[0].primary_entity.name == "IRAN01"
        assert "dark_aircraft" in events[0].primary_entity.flags
        assert producer.send.called

    asyncio.run(run_test())


def test_aviation_gap_detector_deduplication():
    async def run_test():
        redis = MockRedis()
        producer = AsyncMock()
        scorer = AsyncMock()
        db_writer = AsyncMock()

        now = datetime.now(timezone.utc)
        await redis.raw.set("sentinel:heartbeat:collector-adsb", json.dumps({"component": "collector-adsb", "ts": now.isoformat()}))

        old_ts = (now - timedelta(hours=3)).isoformat()
        await redis.raw.set("aircraft:last_seen:a4b5c6", json.dumps({
            "ts": old_ts,
            "region": "Iranian Airspace",
            "callsign": "IRAN01"
        }))

        detector = AviationGapDetector(producer, scorer, db_writer, redis)

        # First check fires alert
        await detector._check()
        assert db_writer.write_events_batch.call_count == 1

        # Second check (same dark flight) should NOT fire duplicate
        db_writer.reset_mock()
        await detector._check()
        assert db_writer.write_events_batch.call_count == 0

    asyncio.run(run_test())
