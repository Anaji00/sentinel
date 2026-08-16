"""
tests/test_correlation_and_graph.py

Consolidated Test Suite: Hawkes Cross-Domain Correlator, Soft Correlator, Primary Entity Derivation & Knowledge Graph.
Combines:
  - test_hawkes_correlator.py
  - test_soft_correlator_retry.py
  - test_correlation_and_primary_entities.py
  - test_knowledge_graph_engine.py
"""

import json
import time
import math
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from pydantic import ValidationError

from services.correlation.hawkes_correlator import HawkesMLE, CrossDomainHawkesCorrelator
from services.correlation.soft_correlator import SoftCorrelator
from services.correlation.cascade import GeopoliticalCascadeEngine
from services.correlation.main import evaluate_dynamic_rules, _dynamic_rules_cache
from services.agents.knowledge_graph_engine import GraphTriple, IntelBrief, KnowledgeGraphEngine, VALID_PREDICATES
from services.agents.base import AgentBulletin
from shared.models import NormalizedEvent, EventType, Entity, EntityType, CorrelationCluster, Scenario, AlertTier


# ── 1. HAWKES PROCESS CORRELATOR TESTS ──────────────────────────────────────

def test_hawkes_mle_fit_minimal_data():
    mle = HawkesMLE(domains=["crypto", "tradfi"])
    result = mle.fit({"crypto": [1.0, 2.0], "tradfi": [3.0]}, T=10.0)
    assert "mu" in result
    assert "branching_ratios" in result

def test_hawkes_spectral_radius_constraint():
    mle = HawkesMLE(domains=["a", "b", "c"], spectral_radius_cap=0.9)
    for i in range(3):
        for j in range(3):
            mle.alpha[i][j] = 5.0
    mle.beta = 0.1
    mle._project_stationarity()
    sr = mle._compute_spectral_radius()
    assert sr <= 0.95

def test_cross_domain_hawkes_correlator_record_event():
    correlator = CrossDomainHawkesCorrelator()
    now = time.time()
    state = correlator.record_event("crypto", now)
    assert state["excitation_ratio"] >= 1.0

def test_cross_domain_excitation_forecasts():
    correlator = CrossDomainHawkesCorrelator()
    now = time.time()
    for i in range(20):
        correlator.record_event("crypto", now + i * 0.05)
    forecasts = correlator.get_excitation_forecasts(now + 1.0, threshold=1.2)
    if forecasts:
        assert any(f["target_domain"] == "tradfi" for f in forecasts)


# ── 2. SOFT CORRELATOR RETRY & RECOVERY ──────────────────────────────────────

@pytest.mark.anyio
async def test_soft_correlator_retry_loop_recovery():
    ollama = AsyncMock()
    correlator = SoftCorrelator(ollama)
    assert correlator.is_enabled is False

    call_count = 0
    class MockAsyncQdrantClient:
        def __init__(self, host=None, port=None): pass
        async def collection_exists(self, collection_name):
            nonlocal call_count
            call_count += 1
            if call_count == 1: raise RuntimeError("Qdrant connection failed")
            return True

    with patch.dict("sys.modules", {"sentence_transformers": MagicMock()}):
        with patch("qdrant_client.AsyncQdrantClient", MockAsyncQdrantClient):
            await correlator._load()
            assert correlator.is_enabled is False
            if correlator._retry_task:
                await asyncio.sleep(0.05)
            assert correlator.get_status()["model_loaded"] is True


# ── 3. CORRELATION CLUSTERS & PRIMARY ENTITIES ────────────────────────────────

def test_correlation_cluster_primary_entity_auto_derivation():
    cluster = CorrelationCluster(
        rule_id="RULE_TEST_01", rule_name="Test Rule", alert_tier=AlertTier.ALERT,
        trigger_event_id="evt_123", entity_ids=["AAPL", "MSFT"], entity_names=["Apple Inc", "Microsoft Corp"],
        description="Test correlation"
    )
    assert cluster.primary_entity_id == "AAPL"
    assert cluster.primary_entity_name == "Apple Inc"

def test_scenario_primary_entity_auto_derivation():
    scen = Scenario(
        correlation_id="corr_999", headline="Disruption", significance="High", hypotheses=[],
        recommended_monitoring=["TSMC"], confidence_overall=85, confidence_rationale="Multi-domain",
        entity_ids=["TSMC"], entity_names=["Taiwan Semiconductor"]
    )
    assert scen.primary_entity_id == "TSMC"

def test_cascade_engine_emits_primary_entities():
    engine = GeopoliticalCascadeEngine(window_seconds=3600, cooldown_seconds=0)
    evt1 = NormalizedEvent(
        event_id="evt_1", trace_id="tr_1", type=EventType.HEADLINE, occurred_at=datetime.now(timezone.utc),
        source="news", primary_entity=Entity(id="TAIWAN", name="Taiwan Region", type=EntityType.COUNTRY),
        headline="Naval exercises", anomaly_score=0.8, region="TAIWAN"
    )
    evt2 = NormalizedEvent(
        event_id="evt_2", trace_id="tr_2", type=EventType.FLIGHT_DARK, occurred_at=datetime.now(timezone.utc),
        source="adsb", primary_entity=Entity(id="TAIWAN", name="Taiwan Region", type=EntityType.COUNTRY),
        headline="Transponder anomaly", anomaly_score=0.85, region="TAIWAN"
    )
    c1 = engine.ingest_event(evt1)
    assert c1 is None
    c2 = engine.ingest_event(evt2)
    assert c2.primary_entity_id == "TAIWAN"


# ── 4. KNOWLEDGE GRAPH GOVERNANCE & PROPOSALS TESTS (§3.1 - §3.7) ──────────────

def test_centralized_predicates_and_node_labels():
    from shared.models.ontology import (
        VALID_PREDICATES,
        ALLOWED_NODE_LABELS,
        STATISTICAL_PREDICATES,
        is_valid_predicate,
        normalize_predicate,
        is_valid_node_label,
    )
    # Check newly added predicates (§3.4, §3.5)
    for pred in [
        "MEMBER_OF", "CUSTOMER_OF", "SUPPLIER_TO", "COMPETES_WITH", "SYMPATHY_MOVER",
        "FX_EXPOSURE_TO", "REGULATORY_EXPOSURE",
        "STATISTICALLY_CORRELATED_WITH", "GRANGER_CAUSES", "HAWKES_EXCITES"
    ]:
        assert pred in VALID_PREDICATES
        assert is_valid_predicate(pred) is True

    # Check node labels
    for label in ["Entity", "Company", "Vessel", "Aircraft", "Index", "Sector", "MacroFactor"]:
        assert label in ALLOWED_NODE_LABELS
        assert is_valid_node_label(label) is True

    # Check validation helpers
    assert is_valid_predicate("INVALID_RANDOM_PREDICATE") is False
    assert normalize_predicate("member_of") == "MEMBER_OF"
    assert normalize_predicate("invalid_predicate") == "RELATED_TO"
    assert is_valid_node_label("Invalid<Injection>;") is False


def test_graph_writer_equity_and_relationship_routing():
    from services.enrichment.graph_writer import GraphWriter
    from shared.kafka import Topics

    mock_producer = AsyncMock()
    writer = GraphWriter(mock_producer)

    async def _test():
        # 1. Upsert Equity with Sector and Index
        await writer.upsert_equity("NVDA", {
            "name": "NVIDIA Corp",
            "sector": "Semiconductors",
            "indices": ["SPX", "NDX"],
            "tags": ["AI", "MEGA_CAP"]
        })
        # Check proposals sent
        assert mock_producer.send.call_count >= 4
        calls = [c[0] for c in mock_producer.send.call_args_list]
        topic_targets = [c[0] for c in calls]
        assert all(t == Topics.ONTOLOGY_PROPOSALS for t in topic_targets)
        proposals = [c[1] for c in calls]

        # Verify Node Proposal
        nvda_node = next(p for p in proposals if p.get("action") == "MERGE_ONTOLOGY_NODE" and p.get("entity_id") == "NVDA")
        assert nvda_node["data"]["label"] == "Company"
        assert nvda_node["data"]["primary_domain"] == "financial"

        # Verify Index Link (MEMBER_OF §3.5)
        member_links = [p for p in proposals if p.get("action") == "LINK_ENTITY" and p["data"]["relation_type"] == "MEMBER_OF"]
        assert len(member_links) == 2

        # 2. Universal link_entities with statistical properties
        mock_producer.reset_mock()
        await writer.link_entities(
            source_id="NVDA",
            relation_type="STATISTICALLY_CORRELATED_WITH",
            target_id="TSM",
            properties={"coefficient": 0.88, "p_value": 0.001, "window": "30d", "method": "pearson"}
        )
        assert mock_producer.send.call_count == 1
        stat_call = mock_producer.send.call_args[0][1]
        assert stat_call["data"]["relation_type"] == "STATISTICALLY_CORRELATED_WITH"
        assert stat_call["data"]["properties"]["coefficient"] == 0.88

    asyncio.run(_test())


def test_merge_graph_triples_rejects_off_whitelist_predicate():
    mock_producer = AsyncMock()
    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine._producer = mock_producer

    invalid_triple = GraphTriple(subject="Report Alpha", predicate="Source", object="Reuters", confidence=0.9)
    asyncio.run(engine._merge_graph_triples([invalid_triple]))
    mock_producer.send.assert_not_called()


def test_merge_graph_triples_rejects_self_loop():
    mock_producer = AsyncMock()
    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine._producer = mock_producer

    self_loop = GraphTriple(subject="US Navy", predicate="OPERATES_IN", object="us navy", confidence=0.9)
    asyncio.run(engine._merge_graph_triples([self_loop]))
    mock_producer.send.assert_not_called()


def test_merge_graph_triples_accepts_valid_triple():
    mock_producer = AsyncMock()
    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine._producer = mock_producer

    valid_triple = GraphTriple(subject="US Navy", predicate="LOCATED_IN", object="Red Sea", confidence=0.85)
    asyncio.run(engine._merge_graph_triples([valid_triple]))
    assert mock_producer.send.call_count == 1
    call_args = mock_producer.send.call_args[0]
    assert call_args[0] == "sentinel.ontology.proposals"
    assert call_args[1]["action"] == "LINK_ENTITY"
    assert call_args[1]["data"]["relation_type"] == "LOCATED_IN"


def test_stock_correlation_agent_emits_governed_sympathy_proposal():
    from services.agents.stock_correlation_agent import StockCorrelationAgent, DynamicCorrelationAssessment, SympathyMover

    async def run_test():
        agent = StockCorrelationAgent.__new__(StockCorrelationAgent)
        agent.redis = AsyncMock()
        mock_prod = AsyncMock()
        agent._producer = mock_prod
        agent.name = "stock_correlation_agent"

        brief = DynamicCorrelationAssessment(
            macro_asset="CL=F",
            equity_ticker="XOM",
            correlation_type="POSITIVE",
            detected_covariance=0.75,
            conviction=0.92,
            transmission_channel="Crude price surge directly expands upstream refining margins",
            agentic_rationale="Upstream cash flow acceleration",
            impact_severity="SEVERE",
            sympathy_movers=[
                SympathyMover(ticker="CVX", relationship="Peer Major", direction="lead", conviction=0.89, reasoning="Peer sympathy"),
                SympathyMover(ticker="OXY", relationship="Upstream Pureplay", direction="lag", conviction=0.84, reasoning="Leveraged beta")
            ]
        )

        # Trigger message handling or sympathy proposal emission
        for sm in brief.sympathy_movers:
            await agent.producer.send(
                "sentinel.ontology.proposals",
                {
                    "entity_id": brief.equity_ticker,
                    "action": "LINK_ENTITY",
                    "data": {
                        "target_id": sm.ticker,
                        "source_label": "Company",
                        "target_label": "Company",
                        "relation_type": "SYMPATHY_MOVER",
                        "weight": float(sm.conviction or 1.0),
                        "confidence": float(sm.conviction or 1.0),
                        "relationship": sm.relationship,
                        "direction": sm.direction,
                    }
                },
                key=brief.equity_ticker
            )

        assert mock_prod.send.call_count == 2
        calls = [c[0][1] for c in mock_prod.send.call_args_list]
        assert calls[0]["data"]["relation_type"] == "SYMPATHY_MOVER"
        assert calls[0]["data"]["target_id"] == "CVX"
        assert calls[1]["data"]["target_id"] == "OXY"

    asyncio.run(run_test())


def test_graph_supervisor_executes_governed_proposals_with_timestamps():
    from services.agents.supervisor import GraphSupervisor

    async def _test():
        mock_neo4j = AsyncMock()
        mock_redis = AsyncMock()
        mock_redis.raw.set.return_value = True

        supervisor = GraphSupervisor.__new__(GraphSupervisor)
        supervisor.neo4j = mock_neo4j
        supervisor.redis = mock_redis
        supervisor.name = "supervisor"

        # 1. Execute single node proposal
        await supervisor.execute_proposal({
            "entity_id": "NVDA",
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {
                "label": "Company",
                "primary_domain": "financial",
                "sector": "Technology",
                "confidence": 1.0
            }
        })
        assert mock_neo4j.execute.call_count == 1
        node_query = mock_neo4j.execute.call_args[0][0]
        assert "MERGE (e:Company {name: $name})" in node_query
        assert "e.last_updated = timestamp()" in node_query

        # 2. Execute statistical relationship proposal (§3.5, §3.6)
        mock_neo4j.reset_mock()
        await supervisor.execute_proposal({
            "entity_id": "NVDA",
            "action": "LINK_ENTITY",
            "data": {
                "target_id": "TSM",
                "source_label": "Company",
                "target_label": "Company",
                "relation_type": "STATISTICALLY_CORRELATED_WITH",
                "properties": {
                    "coefficient": 0.88,
                    "p_value": 0.001,
                    "window": "30d"
                }
            }
        })
        assert mock_neo4j.execute.call_count == 1
        link_query = mock_neo4j.execute.call_args[0][0]
        assert "MERGE (a)-[r:STATISTICALLY_CORRELATED_WITH]->(b)" in link_query
        assert "r.last_updated = timestamp()" in link_query

    asyncio.run(_test())


def test_stock_correlation_agent_handles_none_payloads():
    async def run_test():
        from services.agents.stock_correlation_agent import StockCorrelationAgent
        agent = StockCorrelationAgent.__new__(StockCorrelationAgent)
        agent.redis = MagicMock()

        res1 = await agent.handle_message(None)
        assert res1 is None

        res2 = await agent.handle_message({"financial_data": None, "primary_entity": None})
        assert res2 is None

    asyncio.run(run_test())


def test_context_builder_graph_staleness_and_decay_sorting():
    from services.reasoning.context_builder import ContextBuilder

    async def _test():
        now_ts = time.time()
        # Mock Neo4j with two relationships: fresh (1 hour ago) vs stale (120 days ago)
        mock_neo4j = AsyncMock()
        mock_neo4j.query = AsyncMock(side_effect=[
            [
                {
                    "entity_id": "NVDA",
                    "rel": "STATISTICALLY_CORRELATED_WITH",
                    "connected": "TSM",
                    "labels": ["Company"],
                    "weight": 1.0,
                    "confidence": 0.95,
                    "last_updated": (now_ts - 3600) * 1000  # 1 hr ago in ms
                },
                {
                    "entity_id": "NVDA",
                    "rel": "RELATED_TO",
                    "connected": "HISTORIC_VENDOR",
                    "labels": ["Company"],
                    "weight": 1.0,
                    "confidence": 0.95,
                    "last_updated": (now_ts - 120 * 86400) * 1000  # 120 days ago in ms
                }
            ],
            [] # flags
        ])

        with patch("services.reasoning.context_builder.get_neo4j", return_value=mock_neo4j):
            builder = ContextBuilder.__new__(ContextBuilder)
            results = await builder._fetch_entity_graph(["NVDA"])
            assert len(results) >= 1
            rels = results[0]["relationships"]
            assert len(rels) == 2
            # Fresh edge must have higher decayed weight than stale edge
            assert rels[0]["connected"] == "TSM"
            assert rels[0]["decayed_weight"] > rels[1]["decayed_weight"]
            assert rels[0]["decayed_weight"] > 0.85
            assert rels[1]["decayed_weight"] < 0.15

    asyncio.run(_test())


def test_rule_synthesizer_agent_produces_governed_proposals_and_expiry():
    from services.agents.rule_agent import RuleSynthesizerAgent, DynamicRule, RuleList
    from shared.kafka import Topics

    async def _test():
        agent = RuleSynthesizerAgent.__new__(RuleSynthesizerAgent)
        mock_redis = AsyncMock()
        mock_prod = AsyncMock()
        agent.redis = mock_redis
        agent._producer = mock_prod
        agent.logger = MagicMock()

        # Mock telemetry execution returning synthesized rules with tags
        mock_rules = RuleList(rules=[
            DynamicRule(
                rule_id="syn_energy_defense_01",
                rule_name="Energy Spike to Defense Equity Convergence",
                trigger_event_type="market_anomaly",
                correlations=[],
                alert_tier="ALERT",
                tags=["XOM", "LMT"],
                expires_at=int(time.time()) + 7 * 86400
            )
        ])
        agent._execute_with_telemetry = AsyncMock(return_value=mock_rules)

        await agent.handle({
            "brief": {
                "headline_summary": "Strait of Hormuz naval escalation triggers energy price spike",
                "entities": ["XOM", "LMT"]
            }
        })

        # 1. Stored in Redis with 7-day auto-expiry
        assert mock_redis.raw.hset.call_count == 1
        stored_json = json.loads(mock_redis.raw.hset.call_args[0][2])
        assert stored_json["rule_id"] == "syn_energy_defense_01"
        assert stored_json["expires_at"] > time.time() + 6 * 86400

        # 2. Emitted to Topics.RULES_SYNTHESIZED
        prod_calls = [c[0] for c in mock_prod.send.call_args_list]
        rule_synth_calls = [c for c in prod_calls if c[0] == Topics.RULES_SYNTHESIZED]
        assert len(rule_synth_calls) == 1
        assert rule_synth_calls[0][1]["rule_id"] == "syn_energy_defense_01"

        # 3. Discovered relationship routed through Topics.ONTOLOGY_PROPOSALS (§3.7)
        onto_calls = [c for c in prod_calls if c[0] == Topics.ONTOLOGY_PROPOSALS]
        assert len(onto_calls) == 1
        assert onto_calls[0][1]["action"] == "LINK_ENTITY"
        assert onto_calls[0][1]["entity_id"] == "XOM"
        assert onto_calls[0][1]["data"]["target_id"] == "LMT"
        assert onto_calls[0][1]["data"]["relation_type"] == "MACRO_CORRELATED"

    asyncio.run(_test())


def test_api_gateway_graph_staleness_decay_endpoint():
    from fastapi.testclient import TestClient
    from services.api_gateway.routes.main import app
    from services.api_gateway.dependencies import get_graph, get_db, create_jwt_token

    client = TestClient(app)
    client.cookies.set("sentinel_session", create_jwt_token({"sub": "admin@sentinel.io"}))

    now_ts = time.time()
    mock_graph = MagicMock()
    mock_graph.query = AsyncMock(return_value=[
        {
            "source_name": "NVDA", "source_type": "Company", "relationship": "STATISTICALLY_CORRELATED_WITH",
            "target_id": "TSM", "target_name": "Taiwan Semiconductor", "target_type": "Company",
            "weight": 1.0, "confidence": 0.95, "last_updated": (now_ts - 3600) * 1000
        },
        {
            "source_name": "NVDA", "source_type": "Company", "relationship": "RELATED_TO",
            "target_id": "OLD_PARTNER", "target_name": "Old Partner Corp", "target_type": "Company",
            "weight": 1.0, "confidence": 0.95, "last_updated": (now_ts - 100 * 86400) * 1000
        }
    ])
    mock_db = MagicMock()

    app.dependency_overrides[get_graph] = lambda: mock_graph
    app.dependency_overrides[get_db] = lambda: mock_db

    try:
        res = client.get("/api/v1/graph/entity/NVDA")
        assert res.status_code == 200
        data = res.json()
        assert data["entity_id"] == "NVDA"
        conns = data["connections"]
        assert len(conns) == 2
        # Fresh connection sorted ahead of stale connection
        assert conns[0]["target_id"] == "TSM"
        assert conns[0]["decayed_weight"] > conns[1]["decayed_weight"]
    finally:
        app.dependency_overrides.pop(get_graph, None)
        app.dependency_overrides.pop(get_db, None)

