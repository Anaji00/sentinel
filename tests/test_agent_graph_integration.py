"""
tests/test_agent_graph_integration.py

Integration tests for Agent Reasoning & Graph Context Pre-checks (§7.1, §7.2, §7.3).
"""

import pytest
import time
from datetime import datetime, timezone
from services.reasoning.context_builder import ContextBuilder
from services.agents.quant_trading_engine import QuantTradingEngine
from shared.models import NormalizedEvent, EventType, EntityType, Entity, FinancialData


class MockNeo4jClient:
    def __init__(self, rel_records=None, struct_records=None):
        self.rel_records = rel_records or []
        self.struct_records = struct_records or []

    async def query(self, query_str, params=None):
        if "OPERATES_IN" in query_str or "MEMBER_OF" in query_str:
            return self.struct_records
        elif "FLAGGED_AS" in query_str:
            return []
        return self.rel_records


class MockRedis:
    def __init__(self):
        self.data = {}

    @property
    def raw(self):
        return self

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, val, ex=None):
        self.data[key] = val

    async def keys(self, pattern):
        return [b"sentinel:quotes:latest:NVDA", b"sentinel:quotes:latest:CL=F"]

    async def mget(self, keys):
        return ["120.50", "75.20"]

    async def zrange(self, key, start, stop):
        return []

    async def zrangebyscore(self, *args, **kwargs):
        return []

    async def lrange(self, key, start, stop):
        return []

    async def zadd(self, key, mapping):
        pass


class MockProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, message, key=None):
        self.sent.append((topic, message, key))


from unittest.mock import MagicMock, AsyncMock

@pytest.mark.anyio
async def test_context_builder_bidirectional_traversal():
    """Validates ContextBuilder retrieves bidirectional relationships and applies edge decay (§7.1)."""
    now_ts = time.time()
    mock_records = [
        {
            "entity_id": "NVDA",
            "rel": "STATISTICALLY_CORRELATED_WITH",
            "connected": "TSM",
            "labels": ["Company"],
            "weight": 0.85,
            "confidence": 0.90,
            "last_updated": now_ts,
        },
        {
            "entity_id": "NVDA",
            "rel": "GRANGER_CAUSES",
            "connected": "AMD",
            "labels": ["Company"],
            "weight": 0.70,
            "confidence": 0.85,
            "last_updated": now_ts - 86400 * 5,  # 5 days old
        },
    ]

    cb = ContextBuilder(db_client=MagicMock())
    mock_neo = MockNeo4jClient(rel_records=mock_records)

    # Monkeypatch get_neo4j in context_builder
    import services.reasoning.context_builder as cb_module
    orig_get_neo = cb_module.get_neo4j
    cb_module.get_neo4j = AsyncMock(return_value=mock_neo)

    try:
        results = await cb._fetch_entity_graph(["NVDA"])
        assert len(results) >= 1
        nvda_entry = [r for r in results if r["entity_id"] == "NVDA"][0]
        rels = nvda_entry["relationships"]
        assert len(rels) == 2
        assert rels[0]["rel"] == "STATISTICALLY_CORRELATED_WITH"
        assert rels[0]["decayed_weight"] > 0.70
        assert rels[1]["rel"] == "GRANGER_CAUSES"
    finally:
        cb_module.get_neo4j = orig_get_neo


@pytest.mark.anyio
async def test_quant_trading_engine_graph_context_precheck():
    """Validates QuantTradingEngine pulls 1-hop topology and attaches graph_context (§7.3)."""
    mock_struct = [
        {
            "sector": "Information Technology",
            "indices": ["S&P 500", "Nasdaq 100"],
            "supply_chain": ["TSM", "ASML"],
            "competitors": ["AMD", "INTC"],
        }
    ]
    mock_corrs = [
        {"correlated_entity": "TSM", "predicate": "SUPPLIER_TO", "confidence": 0.95}
    ]

    mock_neo = MockNeo4jClient(rel_records=mock_corrs, struct_records=mock_struct)
    mock_redis = MockRedis()
    mock_producer = MockProducer()

    engine = QuantTradingEngine(
        agent_name="test_quant_engine",
        db_client=MagicMock(),
        redis_client=mock_redis,
        neo4j_client=mock_neo,
        producer=mock_producer,
    )

    # Mock LLM execution to return a deterministic FinancialAdviceBrief
    from services.agents.quant_trading_engine import FinancialAdviceBrief, TradingSignal
    async def mock_execute(*args, **kwargs):
        return FinancialAdviceBrief(
            market_regime="Normal",
            general_hedging_strategy="Standard protective collars",
            highest_conviction_plays=[
                TradingSignal(
                    ticker="NVDA",
                    action="BUY",
                    trade_type="Long/Buy",
                    entry_level=120.0,
                    target_price=135.0,
                    stop_loss=112.0,
                    risk_reward_ratio=2.0,
                    conviction_score=0.85,
                    kelly_allocation_pct=5.0,
                    quantitative_rationale="Bullish momentum supported by supplier strength.",
                )
            ]
        )

    engine._execute_with_telemetry = mock_execute

    async def mock_prices(ticker):
        closes = [110.0 + i * 0.5 for i in range(30)]
        highs = [c * 1.02 for c in closes]
        lows = [c * 0.98 for c in closes]
        return closes, highs, lows

    engine._fetch_prices = mock_prices

    from services.agents.base import AgentScorecard
    engine.record_prediction = AsyncMock()
    engine.publish_bulletin = AsyncMock()
    engine.get_scorecard = AsyncMock(return_value=AgentScorecard(agent_name="test_quant_engine"))
    engine.get_cross_agent_context = AsyncMock(return_value="")
    engine._fetch_earnings_context = AsyncMock(return_value="")
    engine._fetch_funding_context = AsyncMock(return_value="")

    message = {
        "ticker": "NVDA",
        "primary_entity": {"id": "NVDA", "name": "NVIDIA Corp"},
        "financial_data": {"ticker": "NVDA"},
        "anomaly_score": 0.80,
    }

    result = await engine._process_trading_advisory(message, "NVDA", 0.80)
    assert result is not None
    assert "graph_context" in result

    gc = result["graph_context"]
    assert gc["sector"] == "Information Technology"
    assert "S&P 500" in gc["index_membership"]
    assert "TSM" in gc["supply_chain"]
    assert "AMD" in gc["competitors"]
