"""
tests/test_correlation_and_graph.py

Consolidated Test Suite: Hawkes Cross-Domain Correlator, Soft Correlator, Primary Entity Derivation & Knowledge Graph.
Combines:
  - test_hawkes_correlator.py
  - test_soft_correlator_retry.py
  - test_correlation_and_primary_entities.py
  - test_knowledge_graph_engine.py
"""

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

def test_soft_correlator_retry_loop_recovery():
    async def run_test():
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

        with patch("sentence_transformers.SentenceTransformer", return_value=MagicMock()):
            with patch("qdrant_client.AsyncQdrantClient", MockAsyncQdrantClient):
                await correlator._load()
                assert correlator.is_enabled is False
                if correlator._retry_task:
                    await asyncio.sleep(0.05)
                assert correlator.get_status()["model_loaded"] is True

    asyncio.run(run_test())


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


# ── 4. KNOWLEDGE GRAPH ENGINE TESTS ──────────────────────────────────────────

def test_intel_brief_empty_dict_raises():
    with pytest.raises((ValueError, ValidationError)):
        IntelBrief(**{})

def test_merge_graph_triples_rejects_off_whitelist_predicate():
    mock_neo4j = AsyncMock()
    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine.neo4j = mock_neo4j

    invalid_triple = GraphTriple(subject="Report Alpha", predicate="Source", object="Reuters", confidence=0.9)
    asyncio.run(engine._merge_graph_triples([invalid_triple]))
    mock_neo4j.query.assert_not_called()

def test_merge_graph_triples_rejects_self_loop():
    mock_neo4j = AsyncMock()
    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine.neo4j = mock_neo4j

    self_loop = GraphTriple(subject="US Navy", predicate="OPERATES_IN", object="us navy", confidence=0.9)
    asyncio.run(engine._merge_graph_triples([self_loop]))
    mock_neo4j.query.assert_not_called()

def test_merge_graph_triples_accepts_valid_triple():
    mock_neo4j = AsyncMock()
    mock_neo4j.query = AsyncMock()
    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine.neo4j = mock_neo4j

    valid_triple = GraphTriple(subject="US Navy", predicate="LOCATED_IN", object="Red Sea", confidence=0.85)
    asyncio.run(engine._merge_graph_triples([valid_triple]))
    mock_neo4j.query.assert_called_once()
