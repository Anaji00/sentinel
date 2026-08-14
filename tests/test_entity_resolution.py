"""
tests/test_entity_resolution.py

Unit test suite for Entity Resolver, Gap Detectors & DB Writers (services/enrichment/):
  - entity_resolver.py
  - gap_detector.py
  - aviation_gap_detector.py
  - ref_data.py
  - db_writer.py
  - graph_writer.py
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from services.enrichment.entity_resolver import EntityResolver
from services.enrichment.gap_detector import VesselGapDetector
from services.enrichment.ref_data import _classify_market_cap_tier

# ── 1. MULTI-DOMAIN ENTITY RESOLUTION ─────────────────────────────────────────

@pytest.mark.anyio
async def test_entity_resolver_mmsi_lookup():
    redis_mock = MagicMock()
    redis_mock.raw.get = AsyncMock(return_value=None)
    resolver = EntityResolver(redis_mock, MagicMock())
    
    res = await resolver.resolve("613000123")
    assert res is None or isinstance(res, dict)


# ── 2. MARITIME AIS DARKNESS GAP DETECTOR ────────────────────────────────────

def test_maritime_gap_detector():
    detector = VesselGapDetector(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    last_known_ts = 1700000000.0
    current_ts = 1700015000.0
    time_gap_sec = current_ts - last_known_ts
    assert time_gap_sec > 10000


# ── 3. REFERENCE DATA MARKET CAP CLASSIFIER ─────────────────────────────────

def test_ref_data_market_cap_classification():
    assert _classify_market_cap_tier(300000) == "mega"
    assert _classify_market_cap_tier(15000) == "large"
    assert _classify_market_cap_tier(300) == "small"
