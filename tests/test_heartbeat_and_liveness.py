"""
tests/test_heartbeat_and_liveness.py

Tests for shared heartbeat utility, liveness checks, and gap detector suppression when collector heartbeats are stale.
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

from shared.models import NormalizedEvent, EventType, Entity, EntityType
from shared.utils.heartbeat import touch_heartbeat, is_component_healthy, start_heartbeat_task
from services.enrichment.gap_detector import VesselGapDetector
from services.enrichment.aviation_gap_detector import AviationGapDetector


class MockPipeline:
    def __init__(self, raw):
        self.raw = raw
        self.commands = []

    def set(self, key, val, ex=None):
        self.commands.append(("set", key, val, ex))

    def get(self, key):
        self.commands.append(("get", key))

    async def execute(self):
        results = []
        for cmd in self.commands:
            if cmd[0] == "set":
                self.raw.store[cmd[1]] = cmd[2]
                results.append(True)
            elif cmd[0] == "get":
                results.append(self.raw.store.get(cmd[1]))
        return results


class MockRawRedis:
    def __init__(self):
        self.store = {}

    def pipeline(self):
        return MockPipeline(self)

    async def set(self, key, val, ex=None):
        self.store[key] = val

    async def get(self, key):
        return self.store.get(key)

    async def scan_iter(self, match, count=100):
        import fnmatch
        pattern = match.replace("*", ".*")
        import re
        regex = re.compile(f"^{pattern}$")
        for k in list(self.store.keys()):
            if regex.match(k):
                yield k


class MockRedis:
    def __init__(self):
        self.raw = MockRawRedis()


def test_touch_and_is_component_healthy():
    async def run_test():
        redis = MockRedis()
        component = "collector-ais"

        # Initially missing
        healthy = await is_component_healthy(redis, component, max_staleness_seconds=10)
        assert healthy is False

        # Touch heartbeat
        res = await touch_heartbeat(redis, component, ttl=60)
        assert res is True

        # Now healthy
        healthy = await is_component_healthy(redis, component, max_staleness_seconds=10)
        assert healthy is True

    asyncio.run(run_test())


def test_stale_heartbeat_detection():
    async def run_test():
        redis = MockRedis()
        component = "collector-adsb"

        # Set stale timestamp (10 minutes ago)
        stale_ts = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
        await redis.raw.set(f"sentinel:heartbeat:{component}", json.dumps({"component": component, "ts": stale_ts}))

        healthy = await is_component_healthy(redis, component, max_staleness_seconds=120)
        assert healthy is False

    asyncio.run(run_test())


def test_vessel_gap_detector_suppression_on_stale_heartbeat():
    async def run_test():
        redis = MockRedis()
        producer = AsyncMock()
        scorer = AsyncMock()
        db_writer = AsyncMock()

        # Set stale AIS heartbeat
        stale_ts = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
        await redis.raw.set("sentinel:heartbeat:collector-ais", json.dumps({"component": "collector-ais", "ts": stale_ts}))

        # Track old vessel in Redis
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        await redis.raw.set("vessel:last_seen:123456789", json.dumps({"ts": old_ts, "region": "Persian Gulf"}))

        detector = VesselGapDetector(producer, scorer, db_writer, redis)
        await detector._check()

        # Verify VESSEL_DARK was NOT emitted, but INFRASTRUCTURE_DEGRADED was persisted/sent
        db_writer.write_events_batch.assert_called_once()
        events = db_writer.write_events_batch.call_args[0][0]
        assert len(events) == 1
        assert events[0].type == EventType.INFRASTRUCTURE_DEGRADED
        assert "collector" in events[0].headline.lower() and "stale" in events[0].headline.lower()

    asyncio.run(run_test())


def test_aviation_gap_detector_suppression_on_stale_heartbeat():
    async def run_test():
        redis = MockRedis()
        producer = AsyncMock()
        scorer = AsyncMock()
        db_writer = AsyncMock()

        # Set stale ADSB heartbeat
        stale_ts = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
        await redis.raw.set("sentinel:heartbeat:collector-adsb", json.dumps({"component": "collector-adsb", "ts": stale_ts}))

        # Track old aircraft in Redis
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=10)).isoformat()
        await redis.raw.set("aircraft:last_seen:a1b2c3", json.dumps({"ts": old_ts, "region": "Iranian Airspace"}))

        detector = AviationGapDetector(producer, scorer, db_writer, redis)
        await detector._check()

        # Verify FLIGHT_DARK was NOT emitted, but INFRASTRUCTURE_DEGRADED was
        db_writer.write_events_batch.assert_called_once()
        events = db_writer.write_events_batch.call_args[0][0]
        assert len(events) == 1
        assert events[0].type == EventType.INFRASTRUCTURE_DEGRADED
        assert "collector" in events[0].headline.lower() and "stale" in events[0].headline.lower()

    asyncio.run(run_test())
