"""
tests/test_drift_scheduler.py

Unit tests for ModelDriftScheduler and ML-Ops Telemetry Drift Monitoring (§6.1, §6.2).
"""

import pytest
import numpy as np
import importlib.util
from pathlib import Path
from shared.kafka import Topics

# Dynamically load services/telemetry-worker/drift_scheduler.py
drift_path = Path(__file__).resolve().parents[1] / "services" / "telemetry-worker" / "drift_scheduler.py"
spec = importlib.util.spec_from_file_location("telemetry_drift_scheduler", drift_path)
telemetry_drift_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(telemetry_drift_module)

ModelDriftScheduler = telemetry_drift_module.ModelDriftScheduler


class MockProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, message, key=None):
        self.sent.append((topic, message, key))


class MockRedis:
    def __init__(self, baseline=None, current=None):
        self.baseline = baseline or []
        self.current = current or []

    @property
    def raw(self):
        return self

    async def lrange(self, key, start, stop):
        if "baseline" in key:
            return [str(x) for x in self.baseline]
        elif "current" in key:
            return [str(x) for x in self.current]
        return []


@pytest.mark.anyio
async def test_psi_calculation_no_drift():
    """Validates PSI is approximately 0 when baseline and current distributions match."""
    scheduler = ModelDriftScheduler()

    np.random.seed(42)
    baseline = list(np.random.beta(a=2, b=5, size=500))
    current = list(np.random.beta(a=2, b=5, size=500))

    psi = scheduler.calculate_psi(baseline, current)
    assert psi < 0.10, f"Expected no drift, got PSI: {psi}"


@pytest.mark.anyio
async def test_psi_calculation_significant_drift():
    """Validates PSI >= 0.25 when distributions diverge significantly."""
    scheduler = ModelDriftScheduler()

    np.random.seed(42)
    # Baseline centered at low anomaly scores (e.g. 0.2)
    baseline = list(np.random.normal(loc=0.2, scale=0.05, size=500))
    # Current distribution shifted to high anomaly scores (e.g. 0.7)
    current = list(np.random.normal(loc=0.7, scale=0.05, size=500))

    psi = scheduler.calculate_psi(baseline, current)
    assert psi >= 0.25, f"Expected significant drift, got PSI: {psi}"


@pytest.mark.anyio
async def test_drift_scheduler_trigger_dispatch():
    """Validates retrain trigger is dispatched to Topics.RULES_FEEDBACK on drift."""
    np.random.seed(42)
    baseline = list(np.random.normal(loc=0.2, scale=0.05, size=200))
    current = list(np.random.normal(loc=0.7, scale=0.05, size=200))

    mock_redis = MockRedis(baseline=baseline, current=current)
    mock_producer = MockProducer()

    scheduler = ModelDriftScheduler(
        redis_client=mock_redis,
        producer=mock_producer,
        check_interval_sec=3600,
    )

    report = await scheduler.check_model_drift()
    assert report["drift_detected"] is True
    assert report["psi"] >= 0.25

    assert len(mock_producer.sent) == 1
    topic, msg, key = mock_producer.sent[0]
    assert topic == Topics.RULES_FEEDBACK
    assert msg["event_type"] == "retrain_trigger"
    assert "PSI drift threshold exceeded" in msg["reason"]
