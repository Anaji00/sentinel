"""
tests/test_model_registry_and_per_domain.py

Tests for Model Registry, Training Data Validation (§1.5),
Conformal Z-Score Calibration (§1.3), and Per-Domain Model Split (§1.1).
"""

import pytest
import numpy as np
from shared.utils.model_registry import (
    ModelRegistry,
    ConformalZScoreCalibrator,
    validate_training_data,
    InsufficientTrainingDataError,
)
from services.enrichment.anomaly_scorer import DynamicAnomalyScorer


# ── 1. HARD-FAIL ON SYNTHETIC / INSUFFICIENT DATA (§1.5) ────────────────────

class TestTrainingDataValidation:

    def test_validate_training_data_success(self):
        data = np.random.normal(loc=0.0, scale=1.0, size=(600, 5))
        samples = validate_training_data(data, domain="tradfi", min_samples=500)
        assert samples == 600

    def test_validate_training_data_insufficient_samples_raises(self):
        data = np.random.normal(loc=0.0, scale=1.0, size=(100, 5))
        with pytest.raises(InsufficientTrainingDataError, match="Insufficient training data"):
            validate_training_data(data, domain="tradfi", min_samples=500)

    def test_validate_training_data_none_raises(self):
        with pytest.raises(InsufficientTrainingDataError, match="is None"):
            validate_training_data(None, domain="maritime", min_samples=100)

    def test_validate_training_data_zero_variance_synthetic_raises(self):
        # Synthetic placeholder data where all feature vectors are identical
        data = np.ones((600, 5), dtype=np.float32)
        with pytest.raises(InsufficientTrainingDataError, match="zero variance"):
            validate_training_data(data, domain="cyber", min_samples=100)


# ── 2. CONFORMAL Z-SCORE CALIBRATOR (§1.3) ──────────────────────────────────

class TestConformalZScoreCalibrator:

    def test_initial_default_z_threshold(self):
        cal = ConformalZScoreCalibrator(domain="maritime", default_z_threshold=1.5)
        assert cal.z_threshold == 1.5
        assert not cal.get_status()["calibrated"]

    def test_z_threshold_recalibration(self):
        cal = ConformalZScoreCalibrator(domain="tradfi", target_far=0.05, min_samples=20)
        # Feed rolling z-scores
        import random
        random.seed(42)
        for _ in range(100):
            cal.observe(random.gauss(0, 1))

        status = cal.get_status()
        assert status["calibrated"]
        assert 1.0 <= cal.z_threshold <= 3.5

    def test_confirmed_anomalies_excluded_from_null(self):
        cal = ConformalZScoreCalibrator(domain="crypto", min_samples=10)
        for _ in range(15):
            cal.observe(10.0, is_confirmed_anomaly=True)
        assert len(cal._null_z_scores) == 0


# ── 3. MODEL REGISTRY (§1.5) ────────────────────────────────────────────────

class TestModelRegistry:

    def test_register_and_retrieve_model(self):
        reg = ModelRegistry()
        meta = reg.register_model(domain="cyber", model_type="RRCF", num_samples=1200)
        assert meta.domain == "cyber"
        assert meta.is_active

        retrieved = reg.get_model_metadata("cyber")
        assert retrieved is not None
        assert retrieved.num_samples_trained == 1200

    def test_update_drift_status(self):
        reg = ModelRegistry()
        reg.register_model(domain="aviation", model_type="RRCF")
        reg.update_drift_status("aviation", "DRIFTED", far=0.12)
        meta = reg.get_model_metadata("aviation")
        assert meta.drift_status == "DRIFTED"
        assert meta.false_alarm_rate == 0.12


# ── 4. PER-DOMAIN ANOMALY SCORER INTEGRATION (§1.1) ──────────────────────────

class TestPerDomainAnomalyScorer:

    def test_scorer_initializes_8_domain_detectors(self):
        class DummyRedis:
            pass

        scorer = DynamicAnomalyScorer(redis_client=DummyRedis())
        expected_domains = {"maritime", "aviation", "tradfi", "crypto", "macro", "cyber", "news", "prediction"}
        assert set(scorer._rrcf_detectors.keys()) == expected_domains
        assert set(scorer._conformal_z_calibrators.keys()) == expected_domains

    def test_get_domain_mapping(self):
        class DummyRedis:
            pass

        scorer = DynamicAnomalyScorer(redis_client=DummyRedis())
        assert scorer._get_domain("vessel_position") == "maritime"
        assert scorer._get_domain("flight_position") == "aviation"
        assert scorer._get_domain("equity_block") == "tradfi"
        assert scorer._get_domain("crypto_trade") == "crypto"
        assert scorer._get_domain("macro_economic") == "macro"
        assert scorer._get_domain("bgp_hijack") == "cyber"
        assert scorer._get_domain("news_headline") == "news"
        assert scorer._get_domain("prediction_market") == "prediction"

    def test_registry_populated_on_init(self):
        class DummyRedis:
            pass

        scorer = DynamicAnomalyScorer(redis_client=DummyRedis())
        models = scorer.registry.list_models()
        assert len(models) == 8
