"""
tests/test_retired_collector_is_not_offline.py

The retired cyber collector counted against platform health.

It runs only behind the `cyber` compose profile, which is off, so it never
heartbeats -- and it was scored OFFLINE, putting the health endpoint at 18 of
19 healthy (0.947) on a stack with nothing wrong.
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.heartbeat import OPTIONAL_COMPONENTS, get_all_heartbeats_status


def test_the_cyber_collector_is_optional():
    assert "collector-cyber" in OPTIONAL_COMPONENTS


def test_an_absent_cyber_collector_is_not_deployed_rather_than_offline():
    redis = MagicMock()
    redis.raw.get = AsyncMock(return_value=None)
    redis.raw.mget = AsyncMock(return_value=[None])
    out = asyncio.run(get_all_heartbeats_status(redis, custom_components=["collector-cyber"]))
    assert out["components"]["collector-cyber"]["status"] == "NOT_DEPLOYED"
    assert out["not_deployed_count"] == 1
    assert out["offline_count"] == 0
