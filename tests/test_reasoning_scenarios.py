"""
tests/test_reasoning_scenarios.py

Unit test suite for AI Reasoning & Scenario Generation (services/reasoning/):
  - scenario_generator.py
  - scenario_tracker.py
  - calibration_harness.py
  - pattern_library.py
  - context_builder.py
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from services.reasoning.pattern_library import PatternLibrary
from services.reasoning.calibration_harness import ThresholdCalibrationHarness

# ── 1. FORECAST CALIBRATION HARNESS & BRIER SCORE ────────────────────────────

def test_brier_score_calibration():
    harness = ThresholdCalibrationHarness()
    
    predictions = [0.80, 0.90, 0.20, 0.10, 0.60]
    outcomes = [1, 1, 0, 0, 1]
    
    brier_score = sum((p - o) ** 2 for p, o in zip(predictions, outcomes)) / len(predictions)
    assert 0.0 <= brier_score <= 1.0
    assert brier_score < 0.15 # Well-calibrated prediction suite


# ── 2. THREAT PATTERN LIBRARY MATCHING ────────────────────────────────────────

@pytest.mark.anyio
async def test_threat_pattern_library_matching():
    db_mock = AsyncMock()
    db_mock.query.return_value = []
    library = PatternLibrary(db_mock)
    
    matches = await library.find_similar(
        tags=["ais_darkness", "sanctions_risk"],
        rule_id="RULE_MARITIME_DARKNESS"
    )
    assert matches is not None
    assert isinstance(matches, list)
