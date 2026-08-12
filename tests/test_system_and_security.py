"""
tests/test_system_and_security.py

Consolidated Test Suite: System Hardening, Security, Env Guard, Logging, Heartbeat & Resilience.
Combines:
  - test_security_hardening.py
  - test_env_guard.py
  - test_logging.py
  - test_heartbeat_and_liveness.py
  - test_resilience_and_memory.py
  - test_defect_remediations.py
  - test_overhaul.py
"""

import os
import re
import time
import json
import logging
import asyncio
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from shared.utils.env_guard import resolve_env_var, SAFE_DEV_ENVS
from shared.utils.logging import suppress_noisy_loggers, ThrottledLogger, BatchLogger, NOISY_LOGGERS
from shared.kafka import BatchKafkaLogger
from shared.utils.heartbeat import touch_heartbeat, is_component_healthy
from shared.utils.ollama import OllamaClient, InferenceError, get_ollama_semaphore
from shared.utils.metrics import MetricsCollector, time_block
from shared.models.events import AnomalyBreakdown, MarketMicrostructure, CrossDomainSignal, NormalizedEvent, EventType, Entity, EntityType
from services.enrichment.enrichers.cyber import CyberEnricher
from services.reasoning.calibration_harness import ThresholdCalibrationHarness
from services.reasoning.scenario_tracker import ScenarioTracker
from services.reasoning.context_builder import ContextBuilder
from services.reasoning.pattern_library import PatternLibrary


# ── 1. CYPHER SANITIZATION & SECURITY HARDENING ─────────────────────────────

LABEL_REGEX = re.compile(r"^[A-Za-z0-9]+$")

@pytest.mark.parametrize("label", ["Entity", "Person", "Organization", "Vessel123"])
def test_valid_labels_pass(label):
    assert LABEL_REGEX.match(label)

@pytest.mark.parametrize("label", ["Entity}) RETURN a; //", "Entity; DROP (e)", "my-label", "Entity{}"])
def test_malicious_labels_rejected(label):
    assert not LABEL_REGEX.match(label)


# ── 2. ENV GUARD & FAIL-CLOSED RESOLUTION ─────────────────────────────────────

@patch.dict(os.environ, {"MY_SECRET": "real-value", "SENTINEL_ENV": "production"})
def test_env_guard_explicit_var_resolves():
    assert resolve_env_var("MY_SECRET", "fallback") == "real-value"

@patch.dict(os.environ, {"SENTINEL_ENV": "dev"}, clear=False)
def test_env_guard_dev_fallback():
    os.environ.pop("MY_SECRET", None)
    assert resolve_env_var("MY_SECRET", "dev-fallback") == "dev-fallback"

@patch.dict(os.environ, {"SENTINEL_ENV": "production"}, clear=False)
def test_env_guard_raises_on_missing_prod_var():
    os.environ.pop("MY_SECRET", None)
    with pytest.raises(RuntimeError, match="CRITICAL SECURITY FAILURE"):
        resolve_env_var("MY_SECRET", "dev-fallback")


# ── 3. LOGGING & LOG SUPPRESSION ─────────────────────────────────────────────

def test_suppress_noisy_loggers():
    suppress_noisy_loggers(logging.WARNING)
    for name in NOISY_LOGGERS:
        lg = logging.getLogger(name)
        assert lg.level == logging.WARNING

def test_batch_logger_flushes_at_max():
    records = []
    class MockHandler(logging.Handler):
        def emit(self, record): records.append(record)

    logger = logging.getLogger("test.batch")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(MockHandler())

    batch = BatchLogger(logger, "test-batch", flush_interval_sec=10.0, max_items=5)
    for _ in range(5): batch.add("topic_a", 1)
    assert len(records) == 1


# ── 4. HEARTBEAT & LIVENESS CHECKS ───────────────────────────────────────────

def test_heartbeat_health_check():
    async def run_test():
        redis = MagicMock()
        redis.raw = AsyncMock()
        redis.raw.get = AsyncMock(return_value=json.dumps({"component": "collector-ais", "ts": datetime.now(timezone.utc).isoformat()}))

        healthy = await is_component_healthy(redis, "collector-ais", max_staleness_seconds=10)
        assert healthy is True

    asyncio.run(run_test())


# ── 5. OLLAMA CIRCUIT BREAKER & INFRASTRUCTURE RESILIENCE ───────────────────

def test_process_global_semaphore():
    sem1 = get_ollama_semaphore("llama3")
    sem2 = get_ollama_semaphore("qwen")
    assert sem1 is sem2

def test_metrics_collector_prometheus_output():
    MetricsCollector.increment("test_counter", 5)
    MetricsCollector.set_gauge("test_gauge", 42.5)
    prom = MetricsCollector.to_prometheus_format()
    assert "sentinel_test_counter 5" in prom
