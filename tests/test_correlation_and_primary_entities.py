import pytest
import asyncio
from datetime import datetime, timezone
import json
from unittest.mock import AsyncMock, MagicMock

from shared.models import (
    NormalizedEvent,
    EventType,
    Entity,
    EntityType,
    CorrelationCluster,
    Scenario,
    AlertTier,
    ScenarioStatus,
)
from services.agents.base import AgentBulletin
from services.correlation.cascade import GeopoliticalCascadeEngine
from services.correlation.main import evaluate_dynamic_rules, _dynamic_rules_cache


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_correlation_cluster_primary_entity_auto_derivation():
    """Verify CorrelationCluster auto-derives primary_entity_id and primary_entity_name from entity lists."""
    cluster = CorrelationCluster(
        rule_id="RULE_TEST_01",
        rule_name="Test Correlation Rule",
        alert_tier=AlertTier.ALERT,
        trigger_event_id="evt_123",
        entity_ids=["AAPL", "MSFT"],
        entity_names=["Apple Inc", "Microsoft Corp"],
        description="Test correlation description",
    )
    assert cluster.primary_entity_id == "AAPL"
    assert cluster.primary_entity_name == "Apple Inc"
    assert "Primary Entity: Apple Inc" in cluster.to_readable_summary()


def test_scenario_primary_entity_auto_derivation():
    """Verify Scenario auto-derives primary_entity_id and primary_entity_name."""
    scen = Scenario(
        correlation_id="corr_999",
        headline="Major Supply Chain Disruption",
        significance="High systemic impact across semiconductor sector",
        hypotheses=[],
        recommended_monitoring=["TSMC", "NVDA"],
        confidence_overall=85,
        confidence_rationale="Multi-domain confirmation",
        entity_ids=["TSMC", "NVDA"],
        entity_names=["Taiwan Semiconductor", "Nvidia Corp"],
    )
    assert scen.primary_entity_id == "TSMC"
    assert scen.primary_entity_name == "Taiwan Semiconductor"


def test_agent_bulletin_primary_entity_fields():
    """Verify AgentBulletin preserves primary entity information."""
    bulletin = AgentBulletin(
        agent_name="QuantEngine",
        bulletin_type="thesis",
        primary_entity_id="NVDA",
        primary_entity_name="NVIDIA Corporation",
        ticker="NVDA",
        conviction=0.9,
        summary="Bullish semiconductor sweep detected",
    )
    assert bulletin.primary_entity_id == "NVDA"
    assert bulletin.primary_entity_name == "NVIDIA Corporation"
    assert bulletin.ticker == "NVDA"


def test_cascade_engine_emits_primary_entities():
    """Verify GeopoliticalCascadeEngine attaches primary_entity_id and primary_entity_name to output clusters."""
    engine = GeopoliticalCascadeEngine(window_seconds=3600, cooldown_seconds=0)
    
    evt1 = NormalizedEvent(
        event_id="evt_1",
        trace_id="tr_1",
        type=EventType.HEADLINE,
        occurred_at=datetime.now(timezone.utc),
        source="news",
        primary_entity=Entity(id="TAIWAN", name="Taiwan Region", type=EntityType.COUNTRY),
        headline="Naval exercises reported near Taiwan Strait",
        anomaly_score=0.8,
        region="TAIWAN",
    )
    evt2 = NormalizedEvent(
        event_id="evt_2",
        trace_id="tr_2",
        type=EventType.FLIGHT_DARK,
        occurred_at=datetime.now(timezone.utc),
        source="adsb",
        primary_entity=Entity(id="TAIWAN", name="Taiwan Region", type=EntityType.COUNTRY),
        headline="Transponder anomaly over Strait",
        anomaly_score=0.85,
        region="TAIWAN",
    )

    c1 = engine.ingest_event(evt1)
    assert c1 is None  # Single event inside window
    
    c2 = engine.ingest_event(evt2)
    assert c2 is not None
    assert c2.primary_entity_id == "TAIWAN"
    assert c2.primary_entity_name == "Taiwan Region"
    assert "TAIWAN" in c2.entity_ids


@pytest.mark.anyio
async def test_dynamic_rule_trigger_matching_list_and_string():
    """Verify evaluate_dynamic_rules triggers correctly on both single string and array trigger_event_type."""
    mock_store = AsyncMock()
    mock_store.get_recent.return_value = [
        {"event_id": "evt_supp_1", "type": "options_flow", "entity_id": "AAPL", "entity_name": "Apple Inc", "headline": "Unusual Call Sweep"}
    ]

    _dynamic_rules_cache["test_rule_list"] = {
        "rule_id": "test_rule_list",
        "rule_name": "Test Multi-Trigger Rule",
        "trigger_event_type": ["equity_block", "price_anomaly"],
        "conditions": {"min_anomaly": 0.20},
        "correlations": [{"event_types": ["options_flow"], "hours": 24, "min_anomaly": 0.20}],
        "alert_tier": "ELEVATED",
        "expires_at": int(datetime.now(timezone.utc).timestamp()) + 10000,
    }

    trig_evt = NormalizedEvent(
        event_id="evt_trig_1",
        trace_id="tr_trig_1",
        type=EventType.EQUITY_BLOCK,
        occurred_at=datetime.now(timezone.utc),
        source="tradfi",
        primary_entity=Entity(id="AAPL", name="Apple Inc", type=EntityType.COMPANY),
        headline="Aggressor buy block of $10M on AAPL",
        anomaly_score=0.75,
    )

    clusters = await evaluate_dynamic_rules(trig_evt, mock_store)
    assert len(clusters) >= 1
    matched = [c for c in clusters if c.rule_id == "test_rule_list"][0]
    assert matched.primary_entity_id == "AAPL"
    assert matched.primary_entity_name == "Apple Inc"
    assert matched.alert_tier == AlertTier.ELEVATED


@pytest.mark.anyio
async def test_context_builder_builds_full_package():
    """Verify ContextBuilder fetches and structures events, graph, bulletins, and consensus."""
    from services.reasoning.context_builder import ContextBuilder

    mock_db = AsyncMock()
    mock_db.query_one.return_value = {
        "event_id": "evt_trig_100",
        "type": "equity_block",
        "occurred_at": datetime.now(timezone.utc),
        "source": "tradfi",
        "region": "GLOBAL",
        "primary_entity_id": "NVDA",
        "primary_entity_name": "NVIDIA Corporation",
        "headline": "Large Block Trade",
        "anomaly_score": 0.82,
        "tags": ["equity_block"],
    }
    mock_db.query.return_value = []

    builder = ContextBuilder(mock_db)

    cluster = CorrelationCluster(
        rule_id="RULE_NVDA_01",
        rule_name="Nvidia Insider & Options Surge",
        alert_tier=AlertTier.CRITICAL,
        trigger_event_id="evt_trig_100",
        entity_ids=["NVDA"],
        entity_names=["NVIDIA Corporation"],
        description="Nvidia options surge correlated with dark pool volume",
    )

    ctx = await builder.build(cluster)
    assert "correlation" in ctx
    assert ctx["correlation"]["rule"] == "Nvidia Insider & Options Surge"
    assert ctx["trigger_event"]["primary_entity_id"] == "NVDA"
    assert "active_bulletins" in ctx
    assert "consensus_analysis" in ctx
