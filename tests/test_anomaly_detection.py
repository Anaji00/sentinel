"""
tests/test_anomaly_detection.py

Consolidated Test Suite: Streaming Anomaly Scorer, RRCF, Kalman & Hawkes Detectors, Model Registry & Conformal Calibration.
Combines:
  - test_anomaly_scorer.py
  - test_streaming_detectors.py
  - test_bayesian_calibration_and_context_compression.py
  - test_model_registry_and_per_domain.py
"""

import math
import pytest
import asyncio
import numpy as np
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from services.enrichment.anomaly_scorer import DynamicAnomalyScorer
from shared.utils.streaming_detectors import (
    RRCFDetector, KalmanResidualFilter, HawkesIntensityTracker, FirstStoryDetector,
    BGPGraphFeatureExtractor, sts_zone_risk_multiplier, HAS_RRCF
)
from shared.utils.model_registry import (
    ModelRegistry, ConformalZScoreCalibrator, validate_training_data, InsufficientTrainingDataError
)
from services.reasoning.context_builder import ContextBuilder
from services.reasoning.calibration_harness import DynamicBayesianCalibrator
from services.reasoning.scenario_tracker import ScenarioTracker


# ── 1. DYNAMIC ANOMALY SCORER INTEGRATION ────────────────────────────────────

def test_dynamic_anomaly_scorer_requires_redis():
    with pytest.raises(ValueError, match="Redis client is required"):
        DynamicAnomalyScorer(redis_client=None)

def test_dynamic_anomaly_scorer_streaming_batch():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        entities = ["VESSEL_1", "VESSEL_2"]
        features_list = [[1.0, 2.0, 3.0], [1.1, 2.1, 3.1]]
        results = await scorer.score_event_batch("vessel_position", entities, features_list)

        assert len(results) == 2
        assert all(0.0 <= r["score"] <= 1.0 for r in results)
        assert all(r["domain"] == "maritime" for r in results)

    asyncio.run(run_test())

def test_kinematic_kalman_scoring():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        r1 = await scorer.score_kinematic_event(
            "MMSI_123456789", lat=25.0, lon=55.0, speed=12.0, heading=90, timestamp=1000.0
        )
        assert 0.0 <= r1["score"] <= 1.0
        assert "residual_distance" in r1

    asyncio.run(run_test())

def test_vessel_dark_sts_context():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DynamicAnomalyScorer(redis_client=redis_mock)

        score_ocean = await scorer.score_vessel_dark("MMSI_1", gap_hours=3.0, region="Mid Atlantic", flags=[], heading=90)
        score_hormuz = await scorer.score_vessel_dark("MMSI_2", gap_hours=3.0, region="Strait of Hormuz", flags=[], heading=90)
        assert score_hormuz > score_ocean

    asyncio.run(run_test())


# ── 2. RRCF DETECTOR & STREAMING DETECTORS ────────────────────────────────────

def test_rrcf_detector_basic():
    detector = RRCFDetector(num_trees=10, window_size=50)
    normal_points = [np.array([1.0, 2.0, 3.0]) + np.random.normal(0, 0.1, 3) for _ in range(30)]
    scores = detector.insert_batch(normal_points)
    assert len(scores) == 30
    assert all(0.0 <= s <= 1.0 for s in scores)

    outlier_score = detector.insert(np.array([100.0, 200.0, 300.0]))
    assert 0.0 <= outlier_score <= 1.0

def test_kalman_residual_filter():
    kf = KalmanResidualFilter()
    res1 = kf.predict_and_update(lat=25.0, lon=55.0, speed=15.0, heading=90, timestamp=1000.0)
    assert res1["residual_distance"] == 0.0

    res2 = kf.predict_and_update(lat=25.0, lon=55.0625, speed=15.0, heading=90, timestamp=1900.0)
    assert res2["residual_distance"] < 1.0

    res3 = kf.predict_and_update(lat=25.83, lon=55.0625, speed=15.0, heading=90, timestamp=1960.0)
    assert res3["residual_distance"] > 40.0

def test_hawkes_intensity_tracker():
    tracker = HawkesIntensityTracker()
    init_ratio = tracker.get_excitation_ratio("tradfi", timestamp=100.0)
    assert abs(init_ratio - 1.0) < 1e-4

    tracker.record_event("crypto", timestamp=100.0)
    tracker.record_event("crypto", timestamp=101.0)
    state = tracker.record_event("crypto", timestamp=102.0)
    assert state["excitation_ratio"] > 1.0

def test_first_story_detector():
    fsd = FirstStoryDetector(window_size=100)
    fsd.score_novelty("Tech companies announce earnings report dates for Q3")
    fsd.score_novelty("Global shipping rates stabilize after Red Sea disruption")
    fsd.score_novelty("European Central Bank maintains key interest rates unchanged")

    s1 = "OPEC announces unexpected crude oil production cut"
    n1 = fsd.score_novelty(s1)
    assert n1 >= 0.6

def test_sts_zone_risk_multiplier():
    assert sts_zone_risk_multiplier("Strait of Hormuz") == 3.0
    assert sts_zone_risk_multiplier("Mid Atlantic") == 1.0


# ── 3. BAYESIAN CALIBRATION & CONTEXT COMPRESSION ────────────────────────────

def test_hierarchical_context_compression_format():
    db_mock = AsyncMock()
    builder = ContextBuilder(db_mock)

    sample_events = [
        {"occurred_at": datetime.now(timezone.utc), "type": "options_flow", "source": "alpaca_options", "primary_entity_name": "Apple Inc", "region": "GLOBAL", "headline": "AAPL $1.5M Sweep", "anomaly_score": 0.88},
        {"occurred_at": datetime.now(timezone.utc), "type": "vessel_position", "source": "aisstream", "primary_entity_name": "Vessel Odyssey", "region": "BLACK_SEA", "vessel_data": {"mmsi": "211334000"}, "anomaly_score": 0.75}
    ]
    table_str = builder._compress_event_sequence(sample_events)
    assert "| Timestamp (UTC) | Type | Source | Entity | Region | Core Detail | Anomaly Score |" in table_str
    assert "Apple Inc" in table_str

def test_dynamic_bayesian_calibration_formula():
    hypotheses = [
        {"label": "Sanctions Evasion", "probability": 50, "watch_signals": ["STS Transfer"], "deny_signals": ["Port Pass"]},
        {"label": "Commercial Transit", "probability": 50, "watch_signals": ["Normal Cargo"], "deny_signals": []}
    ]
    watch_hits = {0: ["STS Transfer"]}
    deny_hits = {}

    updated, confidence, notes = DynamicBayesianCalibrator.recalibrate_hypotheses(hypotheses, watch_hits, deny_hits)
    assert sum(h["probability"] for h in updated) == 100
    assert updated[0]["probability"] > 50


# ── 4. MODEL REGISTRY & CONFORMAL CALIBRATION ────────────────────────────────

def test_validate_training_data_success():
    data = np.random.normal(loc=0.0, scale=1.0, size=(600, 5))
    samples = validate_training_data(data, domain="tradfi", min_samples=500)
    assert samples == 600

def test_validate_training_data_insufficient_samples_raises():
    data = np.random.normal(loc=0.0, scale=1.0, size=(100, 5))
    with pytest.raises(InsufficientTrainingDataError, match="Insufficient training data"):
        validate_training_data(data, domain="tradfi", min_samples=500)

def test_conformal_z_score_calibrator_recalibration():
    cal = ConformalZScoreCalibrator(domain="tradfi", target_far=0.05, min_samples=20)
    import random
    random.seed(42)
    for _ in range(100):
        cal.observe(random.gauss(0, 1))

    status = cal.get_status()
    assert status["calibrated"]
    assert 1.0 <= cal.z_threshold <= 3.5

def test_model_registry_operations():
    reg = ModelRegistry()
    meta = reg.register_model(domain="cyber", model_type="RRCF", num_samples=1200)
    assert meta.domain == "cyber"
    retrieved = reg.get_model_metadata("cyber")
    assert retrieved.num_samples_trained == 1200
