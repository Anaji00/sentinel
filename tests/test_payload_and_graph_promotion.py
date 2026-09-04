"""
tests/test_payload_and_graph_promotion.py

Unit and integration tests for:
- Tier 8.1: Index & Sector Reference Data Graph Wiring (ref_data.py, tradfi.py, GraphWriter)
- Tier 8.2: Statistical Matrix correlation_ids Event Linking (statistical_discovery.py, tradfi.py)
- Tier 9: API Gateway 1-Hop and 2-Hop Graph Subgraph Querying with Epistemic Metadata
"""

import pytest
import time
import json
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime, timezone

from shared.models import NormalizedEvent, EventType, EntityType, Entity, RawEvent
from services.enrichment.graph_writer import GraphWriter
from services.enrichment.ref_data import (
    fetch_and_cache_reference_data,
    fetch_index_constituents,
    refresh_watchlist_reference_data,
)
from services.enrichment.enrichers.tradfi import TradFiEnricher
from services.correlation.statistical_discovery import StatisticalDiscoveryEngine
from shared.kafka import Topics


class MockPipeline:
    def __init__(self, redis):
        self.redis = redis

    def set(self, key, val, ex=None):
        self.redis.data[key] = val
        return self

    async def execute(self):
        return True


class MockRedis:
    def __init__(self):
        self.data = {}
        self.sets = {}

    @property
    def raw(self):
        return self

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, val, ex=None):
        self.data[key] = val

    def pipeline(self):
        return MockPipeline(self)

    async def smembers(self, key):
        return self.sets.get(key, set())

    async def sadd(self, key, *vals):
        if key not in self.sets:
            self.sets[key] = set()
        for v in vals:
            self.sets[key].add(v)

    async def expire(self, key, seconds):
        pass

    async def zrevrange(self, key, start, stop):
        return [b"NVDA", b"AAPL"]

    async def zrange(self, key, start, stop):
        return [b"NVDA", b"AAPL"]


class MockProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, message, key=None):
        self.sent.append((topic, message, key))


class MockNeo4j:
    def __init__(self, records=None):
        self.records = records or []

    async def query(self, query_str, params=None):
        return self.records


@pytest.mark.anyio
async def test_ref_data_graph_promotion_wiring():
    """Validates ref_data.py emits Company, Sector, and Index proposals via GraphWriter (§8.1)."""
    producer = MockProducer()
    gw = GraphWriter(producer=producer)
    redis = MockRedis()

    # Pre-populate index membership cache
    await redis.set("sentinel:index_membership:NVDA", json.dumps(["SPX", "NDX"]))

    # Mock session
    class MockResponse:
        status = 200
        async def json(self):
            return {
                "finnhubIndustry": "Semiconductors",
                "gicsSector": "Information Technology",
                "exchange": "NASDAQ",
                "marketCapitalization": 2800000.0,
                "type": "Common Stock",
                "country": "US",
            }
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass

    class MockSession:
        def get(self, url, params=None):
            return MockResponse()

    # Test fetch_and_cache_reference_data with graph_writer
    import services.enrichment.ref_data as ref_module
    orig_key = ref_module.FINNHUB_API_KEY
    ref_module.FINNHUB_API_KEY = "test_key"
    try:
        res = await fetch_and_cache_reference_data(
            redis_client=redis,
            symbol="NVDA",
            session=MockSession(),
            graph_writer=gw,
        )
        assert res is not None
        assert res["sector"] == "Semiconductors"
        assert res["index_membership"] == ["SPX", "NDX"]

        # Assert GraphWriter sent proposals to ONTOLOGY_PROPOSALS
        proposals = [p[1] for p in producer.sent if p[0] == Topics.ONTOLOGY_PROPOSALS]
        assert len(proposals) >= 3

        actions = [p.get("action") for p in proposals]
        assert "MERGE_ONTOLOGY_NODE" in actions
        assert "LINK_ENTITY" in actions

        # Check that MEMBER_OF links were emitted for SPX and NDX
        links = [p for p in proposals if p.get("action") == "LINK_ENTITY"]
        target_ids = [l["data"]["target_id"] for l in links]
        assert "SPX" in target_ids
        assert "NDX" in target_ids
    finally:
        ref_module.FINNHUB_API_KEY = orig_key


@pytest.mark.anyio
async def test_tradfi_enricher_graph_and_correlation_ids_promotion():
    """Validates TradFiEnricher promotes reference data to graph and populates correlation_ids (§8.1, §8.2)."""
    producer = MockProducer()
    gw = GraphWriter(producer=producer)
    redis = MockRedis()

    # Cache ref_data
    ref_info = {
        "symbol": "NVDA",
        "sector": "Information Technology",
        "industry": "Semiconductors",
        "exchange": "NASDAQ",
        "market_cap_tier": "mega",
        "index_membership": ["SPX", "NDX"],
    }
    await redis.set("sentinel:refdata:NVDA", json.dumps(ref_info))

    # Cache active statistical correlation IDs in Redis (§8.2)
    await redis.sadd("sentinel:correlation:active_ids:NVDA", "corr:stat:NVDA:TSM", "granger:NVDA:AMD:lag1")

    mock_scorer = MagicMock()
    # Returns the scorer's full result now, not a bare score: the
    # per-domain significance gate's answer travels with it.
    mock_scorer.score_financial_trade_batch = AsyncMock(
        return_value=[{"score": 0.85, "is_significant": True, "domain": "tradfi"}]
    )
    mock_scorer.check_watchlist = AsyncMock(return_value=False)
    mock_scorer.track_frequency = AsyncMock(return_value=0.0)
    mock_scorer.get_hawkes_intensity = MagicMock(return_value=1.0)

    enricher = TradFiEnricher(
        scorer=mock_scorer,
        redis_client=redis,
        graph_writer=gw,
    )

    raw_event = RawEvent(
        event_id="raw_nvda_trade_01",
        trace_id="trace_01",
        source="finnhub_equities",
        raw_payload={
            "ticker": "NVDA",
            "price": 125.50,
            "volume": 50000,
            "trade_type": "RAW_TRADE",
        },
        occurred_at=datetime.now(timezone.utc),
    )

    enriched = await enricher.enrich(raw_event)
    assert enriched is not None
    assert enriched.financial_data is not None
    assert enriched.financial_data.sector == "Information Technology"
    assert enriched.financial_data.index_membership == ["SPX", "NDX"]

    # Verify statistical correlation_ids are populated (§8.2)
    assert "corr:stat:NVDA:TSM" in enriched.correlation_ids
    assert "granger:NVDA:AMD:lag1" in enriched.correlation_ids


@pytest.mark.anyio
async def test_statistical_discovery_caches_active_ids_in_redis():
    """Validates StatisticalDiscoveryEngine writes correlation_ids to Redis (§8.2)."""
    producer = MockProducer()
    redis = MockRedis()

    engine = StatisticalDiscoveryEngine(
        db_client=MagicMock(),
        redis_client=redis,
        producer=producer,
    )

    # Mock fetch_price_series
    async def mock_series(ticker, limit=120):
        if ticker == "NVDA":
            return [100.0 + i * 0.5 for i in range(60)]
        elif ticker == "TSM":
            return [90.0 + i * 0.45 for i in range(60)]
        return []

    engine.fetch_price_series = mock_series
    engine.build_candidate_pairs = AsyncMock(return_value=[("NVDA", "TSM")])

    discoveries = await engine.discover_pairwise_correlations()
    assert len(discoveries) >= 1

    # Check active correlation IDs were stored in Redis
    nvda_ids = await redis.smembers("sentinel:correlation:active_ids:NVDA")
    assert "corr:stat:NVDA:TSM" in nvda_ids


@pytest.mark.anyio
async def test_api_gateway_1hop_and_2hop_graph_expansion():
    """Validates API Gateway /entity/{id} supports hops=1 and hops=2 with statistical metadata (§9.1)."""
    from services.api_gateway.routes.graph import get_entity_graph

    now_ts = time.time()
    mock_records_1hop = [
        {
            "source_name": "NVDA",
            "source_type": "Company",
            "relationship": "STATISTICALLY_CORRELATED_WITH",
            "target_id": "TSM",
            "target_name": "TSM",
            "target_type": "Company",
            "weight": 0.90,
            "confidence": 0.95,
            "last_updated": now_ts,
            "coefficient": 0.88,
            "p_value": 0.001,
            "lag": None,
            "f_stat": None,
            "branching_ratio": None,
            "method": "pearson",
            "window": "120_bars",
            "hop_level": 1,
        }
    ]

    mock_neo = MockNeo4j(records=mock_records_1hop)
    mock_db = MagicMock()

    # Test 1-hop
    res_1hop = await get_entity_graph(entity_id="NVDA", hops=1, graph=mock_neo, db=mock_db)
    assert res_1hop["hops"] == 1
    assert len(res_1hop["connections"]) == 1
    conn = res_1hop["connections"][0]
    assert conn["relationship"] == "STATISTICALLY_CORRELATED_WITH"
    assert conn["coefficient"] == 0.88
    assert conn["decayed_weight"] > 0.80
    assert conn["hop_level"] == 1

    # Test 2-hop
    mock_records_2hop = mock_records_1hop + [
        {
            "source_name": "TSM",
            "source_type": "Company",
            "relationship": "SUPPLIER_TO",
            "target_id": "AAPL",
            "target_name": "AAPL",
            "target_type": "Company",
            "weight": 1.0,
            "confidence": 0.9,
            "last_updated": now_ts,
            "coefficient": None,
            "p_value": None,
            "lag": None,
            "f_stat": None,
            "branching_ratio": None,
            "method": None,
            "window": None,
            "hop_level": 2,
        }
    ]
    mock_neo.records = mock_records_2hop

    res_2hop = await get_entity_graph(entity_id="NVDA", hops=2, graph=mock_neo, db=mock_db)
    assert res_2hop["hops"] == 2
    assert len(res_2hop["connections"]) == 2
    hop_levels = {c["hop_level"] for c in res_2hop["connections"]}
    assert 1 in hop_levels
    assert 2 in hop_levels
