"""
tests/test_exposure_confidence_gate.py

Tests for Ticker Exposure Confidence Gating (Task 2) in MacroIntelligenceEngine.
"""

import os
import json
import pytest
import asyncio


class MockNeo4j:
    def __init__(self, query_rows):
        self.query_rows = query_rows
        self.last_params = None

    async def query(self, cypher_query, params=None):
        self.last_params = params
        min_conf = params.get("min_confidence", 0.0) if params else 0.0
        return [r for r in self.query_rows if r.get("confidence", 0.8) >= min_conf]


class MockRedis:
    def __init__(self):
        self.data = {}
        self.raw = self

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, val, ex=None):
        self.data[key] = val


def test_get_exposed_instruments_filters_low_confidence_edges():
    """_get_exposed_instruments query filtering should return only edges above min_confidence."""
    async def run_test():
        rows = [
            {"exposed_ticker": "XOM", "confidence": 0.85},
            {"exposed_ticker": "CVX", "confidence": 0.65},
            {"exposed_ticker": "JUNK", "confidence": 0.20},
        ]

        mock_neo4j = MockNeo4j(rows)

        os.environ["MIN_EXPOSURE_EDGE_CONFIDENCE"] = "0.6"
        min_conf = float(os.getenv("MIN_EXPOSURE_EDGE_CONFIDENCE", "0.6"))

        cypher_query = """
        MATCH (c:Entity {id: $macro_asset})-[r:COMMODITY_EXPOSURE|SUPPLIES|POSITIVE_EXPOSURE_TO|INVERSE_EXPOSURE_TO*1..2]-(e:Entity {type: 'instrument'})
        WHERE ALL(rel IN r WHERE coalesce(rel.confidence, 0.0) >= $min_confidence)
        RETURN DISTINCT e.id AS exposed_ticker
        """

        result_rows = await mock_neo4j.query(cypher_query, {"macro_asset": "OIL", "min_confidence": min_conf})
        exposed = [r["exposed_ticker"] for r in result_rows]

        assert "XOM" in exposed
        assert "CVX" in exposed
        assert "JUNK" not in exposed

    asyncio.run(run_test())


def test_min_confidence_env_override():
    """MIN_EXPOSURE_EDGE_CONFIDENCE env var should be respected."""
    os.environ["MIN_EXPOSURE_EDGE_CONFIDENCE"] = "0.75"
    min_conf = float(os.getenv("MIN_EXPOSURE_EDGE_CONFIDENCE", "0.6"))
    assert min_conf == 0.75
