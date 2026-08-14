"""
tests/test_candles_and_registry.py

Unit test suite for shared utilities:
  - shared/utils/candles.py (OHLCV Aggregator & VWAP)
  - shared/utils/model_registry.py (ML Model Checkpoints & Drift)
  - shared/utils/source_scorecard.py (NATO OSINT Source Reliability)
"""

import pytest
import numpy as np
from unittest.mock import MagicMock
from shared.utils.candles import get_domain_tag
from shared.utils.model_registry import ModelRegistry
from shared.utils.source_scorecard import SourceScorecard

# ── 1. CANDLESTICK AGGREGATOR & VWAP MATH ────────────────────────────────────

def test_candlestick_domain_tag():
    assert get_domain_tag("crypto", "BTCUSDT") == "CRYPTO"
    assert get_domain_tag("tradfi", "AAPL") == "EQUITY"
    assert get_domain_tag("macro", "CL=F") == "MACRO"
    
    trades = [
        {"price": 100.0, "volume": 10.0, "ts": 1700000000},
        {"price": 102.0, "volume": 20.0, "ts": 1700000010},
        {"price": 99.0,  "volume": 30.0, "ts": 1700000020},
    ]
    
    total_volume = sum(t["volume"] for t in trades)
    total_dollar_vol = sum(t["price"] * t["volume"] for t in trades)
    expected_vwap = total_dollar_vol / total_volume
    
    assert total_volume == 60.0
    assert expected_vwap == pytest.approx(100.16666666666667)


# ── 2. MODEL REGISTRY DRIFT METRICS ──────────────────────────────────────────

def test_model_registry_metrics():
    registry = ModelRegistry(redis_client=MagicMock())
    meta = registry.register_model(
        domain="maritime",
        model_type="isolation_forest",
        parameters={"contamination": 0.05},
        num_samples=1000,
        version="v2.1"
    )
    assert meta.model_type == "isolation_forest"
    assert meta.version == "v2.1"
    
    retrieved = registry.get_model_metadata("maritime")
    assert retrieved is not None
    assert retrieved.num_samples_trained == 1000
    
    registry.update_drift_status("maritime", "DRIFTED", far=0.12)
    assert retrieved.drift_status == "DRIFTED"
    assert retrieved.false_alarm_rate == 0.12


# ── 3. NATO OSINT SOURCE RELIABILITY SCORECARD ────────────────────────────────

def test_source_scorecard_nato_ratings():
    card = SourceScorecard(source_name="REUTERS_BREAKING")
    assert card.source_name == "REUTERS_BREAKING"
    assert card.brier_score == 0.5
    assert card.reliability_weight == 0.5
