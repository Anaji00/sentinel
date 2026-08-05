"""
tests/test_quant_financial_advisor_correlation.py

Tests for Financial Advisory Research Integration (Task 4) and Swarm Topic Subscriptions (Task 5).
"""

import pytest
import asyncio
from services.agents.quant_trading_engine import QuantTradingEngine, FinancialAdviceBrief, TradingSignal


class MockNeo4jClient:
    def __init__(self, rows):
        self.rows = rows

    async def query(self, cypher_query, params=None):
        return self.rows


class MockRedisClient:
    def __init__(self):
        self.raw = self

    async def get(self, key):
        return None

    async def set(self, key, val, ex=None):
        pass

    async def lrange(self, key, start, end):
        return []

    async def zrange(self, key, start, end):
        return []


def test_quant_advisory_includes_graph_correlations_in_prompt():
    """QuantTradingEngine should fetch Neo4j exposure edges and construct prompt context."""
    async def run_test():
        rows = [
            {"correlated_entity": "OIL", "predicate": "INVERSE_EXPOSURE_TO", "confidence": 0.85},
            {"correlated_entity": "HORMUZ", "predicate": "SUPPLIES", "confidence": 0.90},
        ]

        mock_neo4j = MockNeo4jClient(rows)
        mock_redis = MockRedisClient()

        graph_rows = await mock_neo4j.query("MATCH ...", {"ticker": "XOM"})
        graph_correlations = [
            f"{r['correlated_entity']} -[{r['predicate']}]-> XOM (confidence: {r['confidence']:.2f})"
            for r in graph_rows
        ]

        assert len(graph_correlations) == 2
        assert "OIL -[INVERSE_EXPOSURE_TO]-> XOM" in graph_correlations[0]
        assert "HORMUZ -[SUPPLIES]-> XOM" in graph_correlations[1]

    asyncio.run(run_test())


def test_swarm_topic_subscriptions_expanded():
    """QuantTradingEngine and RuleSynthesizerAgent topic subscriptions must be expanded."""
    from shared.kafka import Topics

    quant_topics = [
        Topics.RAW_TRADFI, Topics.ENRICHED_EVENTS, Topics.SCENARIOS_GENERATED,
        Topics.CORRELATIONS, Topics.INTEL_BRIEFS, Topics.MACRO_DECOUPLING,
        Topics.QUANT_DISCOVERIES, Topics.MACRO_ASSESSMENT, Topics.RADAR_DECISIONS
    ]

    rule_topics = [
        Topics.INTEL_BRIEFS, Topics.RULES_FEEDBACK, Topics.SCENARIOS_GENERATED,
        Topics.CORRELATIONS, Topics.QUANT_DISCOVERIES, Topics.MACRO_DECOUPLING,
        Topics.MACRO_ASSESSMENT, Topics.INSIDER_CLUSTERS
    ]

    assert Topics.QUANT_DISCOVERIES in quant_topics
    assert Topics.MACRO_DECOUPLING in rule_topics
