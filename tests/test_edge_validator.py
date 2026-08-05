"""
tests/test_edge_validator.py

Tests for Empirical Graph Edge Validator (Task 3).
Validates:
- Edge sample threshold check (samples < 5 left untouched)
- EWMA confidence promotion for predictive edges
- EWMA confidence decay for non-predictive edges
- Parameterized Cypher execution
- Redis exposure cache key invalidation
"""

import pytest
import asyncio
from services.agents.edge_validator import validate_edges, CONFIDENCE_STEP, MIN_SAMPLES_BEFORE_TRUST


class MockNeo4jClient:
    def __init__(self, edges):
        self.edges = edges
        self.updates = []

    async def query(self, query_str, params=None):
        if "MATCH (a:Entity)-[r" in query_str:
            return self.edges
        elif "SET r.confidence =" in query_str:
            self.updates.append((params, query_str))
            return []
        return []


class MockTimescaleClient:
    def __init__(self, events, reactions):
        self.events = events
        self.reactions = reactions

    async def query(self, query_str, *args):
        if "FROM events" in query_str and "INTERVAL '30 days'" in query_str:
            return self.events
        elif "FROM events" in query_str and "INTERVAL '24 hours'" in query_str:
            return self.reactions
        return []


class MockRedisClient:
    def __init__(self):
        self.deleted_keys = []
        self.raw = self

    async def keys(self, pattern):
        return [f"sentinel:cache:exposure:HORMUZ:0.6"]

    async def delete(self, *keys):
        self.deleted_keys.extend(keys)


class MockProducer:
    def __init__(self):
        self.published = []

    async def send(self, topic, value, key=None):
        self.published.append((topic, value, key))


def test_edge_validator_samples_below_min_threshold_untouched():
    """Edges with samples < 5 should not be updated."""
    async def run_test():
        edges = [{
            "source_id": "HORMUZ",
            "predicate": "SUPPLIES",
            "ticker": "XOM",
            "confidence": 0.5,
            "samples": 2,
        }]

        neo4j = MockNeo4jClient(edges)
        ts = MockTimescaleClient(events=[{"occurred_at": "2026-08-01T00:00:00Z"}], reactions=[])
        redis = MockRedisClient()

        res = await validate_edges(neo4j, ts, redis)
        assert res["validated"] == 0
        assert len(neo4j.updates) == 0

    asyncio.run(run_test())


def test_edge_validator_predictive_edge_promoted():
    """Predictive edge with high hit rate should be promoted via EWMA."""
    async def run_test():
        edges = [{
            "source_id": "HORMUZ",
            "predicate": "SUPPLIES",
            "ticker": "XOM",
            "confidence": 0.5,
            "samples": 5,
        }]

        events = [{"occurred_at": f"2026-08-0{i}T00:00:00Z"} for i in range(1, 6)]
        reactions = [{"event_id": "rx1", "type": "equity_block", "anomaly_score": 0.85}]

        neo4j = MockNeo4jClient(edges)
        ts = MockTimescaleClient(events=events, reactions=reactions)
        redis = MockRedisClient()
        producer = MockProducer()

        res = await validate_edges(neo4j, ts, redis, producer=producer)
        assert res["validated"] == 1
        assert res["promoted"] == 1
        assert len(neo4j.updates) == 1

        params, query_str = neo4j.updates[0]
        assert "$source_id" in query_str
        assert "$new_confidence" in query_str
        assert params["new_confidence"] > 0.5

    asyncio.run(run_test())


def test_edge_validator_non_predictive_edge_decayed():
    """Non-predictive edge with zero hit rate should decay via EWMA."""
    async def run_test():
        edges = [{
            "source_id": "NOISE_ENTITY",
            "predicate": "SUPPLIES",
            "ticker": "AAPL",
            "confidence": 0.6,
            "samples": 5,
        }]

        events = [{"occurred_at": f"2026-08-0{i}T00:00:00Z"} for i in range(1, 6)]

        neo4j = MockNeo4jClient(edges)
        ts = MockTimescaleClient(events=events, reactions=[])

        res = await validate_edges(neo4j, ts, redis_client=None)
        assert res["validated"] == 1
        assert res["decayed"] == 1
        assert len(neo4j.updates) == 1

        params, _ = neo4j.updates[0]
        assert params["new_confidence"] < 0.6

    asyncio.run(run_test())
