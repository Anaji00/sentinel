import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from shared.models import CorrelationCluster, AlertTier
from services.reasoning.context_builder import ContextBuilder
from services.reasoning.calibration_harness import DynamicBayesianCalibrator
from services.reasoning.scenario_tracker import ScenarioTracker


@pytest.fixture
def anyio_backend():
    return "asyncio"



def test_hierarchical_context_compression_format():
    """Verify ContextBuilder produces a structured Markdown table for event sequences."""
    db_mock = AsyncMock()
    builder = ContextBuilder(db_mock)

    sample_events = [
        {
            "occurred_at": datetime.now(timezone.utc),
            "type": "options_flow",
            "source": "alpaca_options",
            "primary_entity_name": "Apple Inc",
            "region": "GLOBAL",
            "headline": "AAPL $1.5M Call Option Sweep",
            "anomaly_score": 0.88,
        },
        {
            "occurred_at": datetime.now(timezone.utc),
            "type": "vessel_position",
            "source": "aisstream",
            "primary_entity_name": "Vessel Odyssey",
            "region": "BLACK_SEA",
            "vessel_data": {"mmsi": "211334000", "speed_knots": 14.5},
            "anomaly_score": 0.75,
        }
    ]

    table_str = builder._compress_event_sequence(sample_events)

    assert "| Timestamp (UTC) | Type | Source | Entity | Region | Core Detail | Anomaly Score |" in table_str
    assert "options_flow" in table_str
    assert "Apple Inc" in table_str
    assert "Vessel Odyssey" in table_str
    assert "0.88" in table_str
    assert "0.75" in table_str


def test_dynamic_bayesian_calibration_formula():
    """Verify DynamicBayesianCalibrator calculates exact Bayes posteriors summing to 100%."""
    hypotheses = [
        {
            "label": "Sanctions Evasion",
            "probability": 50,
            "watch_signals": ["STS Transfer in Black Sea"],
            "deny_signals": ["Port Customs Inspection Passed"],
        },
        {
            "label": "Commercial Rerouting",
            "probability": 30,
            "watch_signals": ["Normal Cargo Manifest"],
            "deny_signals": ["AIS Transponder Off"],
        },
        {
            "label": "Military Naval Exercise",
            "probability": 20,
            "watch_signals": ["Warship Escort"],
            "deny_signals": [],
        }
    ]

    watch_hits = {0: ["STS Transfer in Black Sea"]}
    deny_hits = {}

    updated, confidence, notes = DynamicBayesianCalibrator.recalibrate_hypotheses(
        hypotheses, watch_hits, deny_hits
    )

    probs = [h["probability"] for h in updated]
    assert sum(probs) == 100
    assert updated[0]["probability"] > 50  # Hypothesis 1 probability increased via Bayes
    assert confidence >= 70
    assert "+Bayes" in notes


@pytest.mark.anyio
async def test_scenario_tracker_bayesian_recalibration_loop():
    """Verify ScenarioTracker updates scenario confidence and hypothesis probabilities using Bayesian calibration."""
    db_mock = AsyncMock()
    db_mock.query.side_effect = [
        # 1. Active scenarios query
        [{
            "scenario_id": "scn_test_100",
            "headline": "Black Sea Dark Fleet Activity",
            "status": "hypothesis",
            "confidence_overall": 45,
            "hypotheses": [
                {
                    "label": "Sanctions Evasion",
                    "probability": 50,
                    "watch_signals": ["STS Transfer in Black Sea"],
                    "deny_signals": ["Port Customs Passed"]
                },
                {
                    "label": "Commercial Transit",
                    "probability": 50,
                    "watch_signals": ["Normal Cargo Manifest"],
                    "deny_signals": []
                }
            ]
        }],
        # 2. Match watch signals query
        [{"1": 1}]
    ]
    db_mock.execute = AsyncMock()

    tracker = ScenarioTracker(db_mock)
    await tracker.check_all()

    assert db_mock.execute.called
    call_args = db_mock.execute.call_args[0]
    # Check that update_scenario was executed with new confidence and updated hypotheses
    assert "UPDATE scenarios" in call_args[0]
