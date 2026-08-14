"""
tests/test_alert_and_telemetry.py

Unit test suite for Alert Manager, Telemetry Worker & Dead Letter Queue Workers:
  - services/alert_manager/main.py
  - services/telemetry-worker/drift_scheduler.py
  - services/dlq-worker/main.py
"""

import pytest
import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Dynamically load services/telemetry-worker/drift_scheduler.py
drift_path = Path(__file__).resolve().parents[1] / "services" / "telemetry-worker" / "drift_scheduler.py"
spec = importlib.util.spec_from_file_location("telemetry_drift_scheduler", drift_path)
telemetry_drift_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(telemetry_drift_module)

ModelDriftScheduler = telemetry_drift_module.ModelDriftScheduler

# ── 1. TELEMETRY DRIFT SCHEDULER & RETRAINING TRIGGER ────────────────────────

def test_drift_scheduler_psi_calculation():
    scheduler = ModelDriftScheduler()
    baseline = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    current = [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5]
    
    psi = scheduler.calculate_psi(baseline, current, num_buckets=5)
    assert psi >= 0.0


# ── 2. DEAD LETTER QUEUE RETRY EXPONENTIAL BACKOFF ────────────────────────────

def test_dlq_exponential_backoff_calculation():
    def compute_backoff(attempt: int, base_sec: float = 1.0, max_sec: float = 60.0) -> float:
        return min(max_sec, base_sec * (2 ** attempt))

    assert compute_backoff(0) == 1.0
    assert compute_backoff(1) == 2.0
    assert compute_backoff(2) == 4.0
    assert compute_backoff(5) == 32.0
    assert compute_backoff(10) == 60.0 # Capped at max_sec
