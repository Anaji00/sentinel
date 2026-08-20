"""
tests/test_signal_validation_and_governance.py

Comprehensive test suite verifying Section B: Signal Validation & Model Governance:
  - B.1: StrategyBacktester, validation gating, realized Sharpe, max drawdown, probability calibration curves.
  - B.2: Explainability endpoints, mathematical factor attribution waterfall, score adjustments timeline, model card metadata.
  - B.3: FeatureFlagManager, instant signal kill switches, master kill switch, gradual rollout, and ticker whitelists.
"""

import json
import pytest
import numpy as np
from unittest.mock import AsyncMock, MagicMock, patch

from shared.utils.feature_flags import FeatureFlagManager, DEFAULT_SIGNAL_FLAGS
from services.reasoning.strategy_backtester import StrategyBacktester
from services.api_gateway.dependencies import create_jwt_token
from fastapi.testclient import TestClient
from services.api_gateway.routes.main import app


# ── 1. FEATURE FLAGS & KILL SWITCH TESTS (§B.3) ──────────────────────────────

class MockRedis:
    def __init__(self):
        self.store = {}
        self.raw = self

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, val, ex=None):
        self.store[key] = val
        return True

    async def publish(self, channel, message):
        return 1

    async def keys(self, pattern):
        import fnmatch
        return [k for k in self.store.keys() if fnmatch.fnmatch(k, pattern)]

    async def lrange(self, key, start, end):
        return []

    async def aclose(self):
        pass


@pytest.mark.anyio
async def test_feature_flag_manager_defaults_and_toggle():
    mock_redis = MockRedis()
    manager = FeatureFlagManager(redis_client=mock_redis)

    # 1. Default flags are enabled
    assert await manager.is_enabled("covered_calls") is True
    assert await manager.is_enabled("granger_causality") is True
    assert await manager.is_enabled("hawkes_contagion") is True

    # 2. Toggle flag to disabled
    await manager.set_flag("covered_calls", enabled=False, reason="Maintenance testing")
    assert await manager.is_enabled("covered_calls") is False

    # 3. Re-enable flag
    await manager.set_flag("covered_calls", enabled=True)
    assert await manager.is_enabled("covered_calls") is True


@pytest.mark.anyio
async def test_feature_flag_emergency_kill_switches():
    mock_redis = MockRedis()
    manager = FeatureFlagManager(redis_client=mock_redis)

    # 1. Trip individual signal kill switch
    trip_res = await manager.trip_kill_switch("granger_causality", reason="Granger volatility spike")
    assert trip_res["kill_switched"] is True
    assert await manager.is_enabled("granger_causality") is False
    assert await manager.is_enabled("hawkes_contagion") is True  # Other signals unaffected

    # 2. Reset individual kill switch
    reset_res = await manager.reset_kill_switch("granger_causality")
    assert reset_res["kill_switched"] is False
    assert await manager.is_enabled("granger_causality") is True

    # 3. Master Kill Switch halts everything platform-wide
    await manager.trip_master_kill_switch(reason="System-wide critical incident")
    assert await manager.is_enabled("covered_calls") is False
    assert await manager.is_enabled("granger_causality") is False
    assert await manager.is_enabled("hawkes_contagion") is False

    # 4. Reset master kill switch restores normal operation
    await manager.reset_kill_switch("MASTER")
    assert await manager.is_enabled("covered_calls") is True
    assert await manager.is_enabled("granger_causality") is True


@pytest.mark.anyio
async def test_feature_flag_gradual_rollout_and_ticker_whitelisting():
    mock_redis = MockRedis()
    manager = FeatureFlagManager(redis_client=mock_redis)

    # 1. Ticker whitelist targeting
    await manager.set_flag("covered_calls", enabled=True, enabled_tickers=["NVDA", "AAPL"])
    assert await manager.is_enabled("covered_calls", ticker="NVDA") is True
    assert await manager.is_enabled("covered_calls", ticker="AAPL") is True
    assert await manager.is_enabled("covered_calls", ticker="TSLA") is False  # Not in whitelist

    # 2. Gradual rollout percentage
    await manager.set_flag("crypto_liquidations", enabled=True, rollout_pct=0.0)
    assert await manager.is_enabled("crypto_liquidations", ticker="BTC") is False

    await manager.set_flag("crypto_liquidations", enabled=True, rollout_pct=100.0)
    assert await manager.is_enabled("crypto_liquidations", ticker="BTC") is True


# ── 2. STRATEGY BACKTESTER & CALIBRATION TESTS (§B.1) ─────────────────────────

@pytest.mark.anyio
async def test_strategy_backtester_covered_call():
    mock_redis = MockRedis()
    backtester = StrategyBacktester(redis_client=mock_redis)

    # Generate synthetic price series with upward drift and volatility
    np.random.seed(42)
    closes = [100.0]
    for _ in range(120):
        ret = np.random.normal(0.002, 0.012)
        closes.append(round(closes[-1] * (1.0 + ret), 2))

    bars = [
        {
            "timestamp": f"2026-01-01T{i:02d}:00:00Z",
            "open": c * 0.995,
            "high": c * 1.010,
            "low": c * 0.990,
            "close": c,
            "volume": 25000,
            "vwap": c,
        }
        for i, c in enumerate(closes)
    ]

    report = backtester.backtest_strategy(ticker="NVDA", bars=bars, strategy_type="covered_call")

    assert report["ticker"] == "NVDA"
    assert report["strategy_name"] == "Covered Call"
    assert "performance_metrics" in report
    assert "risk_metrics" in report
    assert "calibration_curve" in report
    assert "validation_gate" in report

    perf = report["performance_metrics"]
    risk = report["risk_metrics"]

    assert perf["total_trades"] > 0
    assert 0.0 <= perf["hit_rate_pct"] <= 100.0
    assert perf["profit_factor"] >= 0.0
    assert isinstance(risk["realized_sharpe_ratio"], float)
    assert risk["max_drawdown_pct"] >= 0.0

    # Calibration Curve Verification (§B.1)
    curve = report["calibration_curve"]
    assert len(curve) > 0
    for bin_data in curve:
        assert "probability_bin" in bin_data
        assert "mean_predicted_prob" in bin_data
        assert "empirical_win_rate" in bin_data
    assert 0.0 <= report["brier_score"] <= 1.0


@pytest.mark.anyio
async def test_strategy_backtester_momentum_and_mean_reversion():
    mock_redis = MockRedis()
    backtester = StrategyBacktester(redis_client=mock_redis)

    np.random.seed(123)
    closes = [150.0]
    for _ in range(150):
        ret = np.random.normal(0.001, 0.015)
        closes.append(round(closes[-1] * (1.0 + ret), 2))

    bars = [
        {
            "timestamp": f"2026-02-01T{i:02d}:00:00Z",
            "open": c * 0.995,
            "high": c * 1.015,
            "low": c * 0.985,
            "close": c,
            "volume": 40000,
            "vwap": c,
        }
        for i, c in enumerate(closes)
    ]

    # 1. Momentum strategy backtest
    mom_rep = backtester.backtest_strategy(ticker="AAPL", bars=bars, strategy_type="momentum_trend")
    assert mom_rep["ticker"] == "AAPL"
    assert mom_rep["strategy_name"] == "Momentum Trend"
    assert mom_rep["performance_metrics"]["total_trades"] >= 1

    # 2. Mean reversion strategy backtest
    mr_rep = backtester.backtest_strategy(ticker="MSFT", bars=bars, strategy_type="mean_reversion")
    assert mr_rep["ticker"] == "MSFT"
    assert mr_rep["strategy_name"] == "Mean Reversion"
    assert "validation_gate" in mr_rep


# ── 3. EXPLAINABILITY & MODEL CARDS REST API TESTS (§B.2) ─────────────────────

def get_auth_cookies():
    token = create_jwt_token({"sub": "test-admin-user", "role": "admin"})
    return {"sentinel_session": token}


def test_explain_event_endpoint():
    client = TestClient(app)
    cookies = get_auth_cookies()
    res = client.get("/api/v1/explain/event/evt_test_12345", cookies=cookies)
    assert res.status_code == 200
    data = res.json()

    assert data["event_id"] == "evt_test_12345"
    assert "factor_attribution" in data
    assert len(data["factor_attribution"]) > 0

    # Verify factor attribution structure
    factor = data["factor_attribution"][0]
    assert "label" in factor
    assert "contribution_pct" in factor
    assert "model_weight" in factor

    # Verify score adjustments derivation timeline
    assert "score_adjustments" in data
    assert len(data["score_adjustments"]) == 4

    # Verify model card metadata
    assert "model_card" in data
    mc = data["model_card"]
    assert "IsolationForest" in mc["model_name"]
    assert "model_drift_status" in mc
    assert mc["model_drift_status"]["drift_state"] == "STABLE"

    # Verify data source provenance
    assert "provenance" in data
    assert "payload_hash" in data["provenance"]
    assert "processing_latency_ms" in data["provenance"]


def test_explain_signal_endpoint():
    client = TestClient(app)
    cookies = get_auth_cookies()
    res = client.get("/api/v1/explain/signal/signal_NVDA_trade", cookies=cookies)
    assert res.status_code == 200
    data = res.json()

    assert data["ticker"] == "NVDA"
    assert "deterministic_math_audit" in data
    audit = data["deterministic_math_audit"]
    assert "half_kelly_inputs" in audit
    assert audit["half_kelly_inputs"]["empirical_win_rate_W"] == 0.62
    assert "stop_formula" in audit

    assert "technical_indicator_inputs" in data
    assert "graph_topology_precheck" in data
    assert "empirical_correlations" in data["graph_topology_precheck"]


# ── 4. API GATEWAY FEATURE FLAGS & BACKTEST ROUTES INTEGRATION ────────────────

def test_api_gateway_feature_flags_routes():
    client = TestClient(app)
    admin_cookies = {"sentinel_session": create_jwt_token({"sub": "admin-user"}, role="ADMIN")}
    viewer_cookies = {"sentinel_session": create_jwt_token({"sub": "viewer-user"}, role="VIEWER")}

    # 1. GET /api/v1/flags allows VIEWER
    res = client.get("/api/v1/flags", cookies=viewer_cookies)
    assert res.status_code == 200
    data = res.json()
    assert "signals" in data
    assert "covered_calls" in data["signals"]

    # 2. VIEWER cannot POST to /toggle, /kill-switch, /reset -> 403 Forbidden
    res_toggle_denied = client.post(
        "/api/v1/flags/toggle",
        cookies=viewer_cookies,
        json={"flag_name": "covered_calls", "enabled": True, "rollout_pct": 80.0},
    )
    assert res_toggle_denied.status_code == 403

    res_kill_denied = client.post(
        "/api/v1/flags/kill-switch",
        cookies=viewer_cookies,
        json={"flag_name": "crypto_liquidations", "reason": "Unauthorized attempt"},
    )
    assert res_kill_denied.status_code == 403

    res_reset_denied = client.post(
        "/api/v1/flags/reset",
        cookies=viewer_cookies,
        json={"flag_name": "crypto_liquidations"},
    )
    assert res_reset_denied.status_code == 403

    # 3. ADMIN can POST to /toggle, /kill-switch, /reset -> 200 OK
    res_toggle = client.post(
        "/api/v1/flags/toggle",
        cookies=admin_cookies,
        json={"flag_name": "covered_calls", "enabled": True, "rollout_pct": 80.0},
    )
    assert res_toggle.status_code == 200
    assert res_toggle.json()["status"] == "success"

    res_kill = client.post(
        "/api/v1/flags/kill-switch",
        cookies=admin_cookies,
        json={"flag_name": "crypto_liquidations", "reason": "Operator test emergency"},
    )
    assert res_kill.status_code == 200
    assert res_kill.json()["status"] == "kill_switch_tripped"

    res_reset = client.post(
        "/api/v1/flags/reset",
        cookies=admin_cookies,
        json={"flag_name": "crypto_liquidations"},
    )
    assert res_reset.status_code == 200
    assert res_reset.json()["status"] == "reset_complete"


def test_api_gateway_backtest_routes():
    client = TestClient(app)
    cookies = get_auth_cookies()

    # 1. POST /api/v1/backtest/run
    res_run = client.post(
        "/api/v1/backtest/run",
        cookies=cookies,
        json={"ticker": "NVDA", "strategy_type": "covered_call", "timeframe": "5m", "initial_capital": 50000.0},
    )
    assert res_run.status_code == 200
    rep = res_run.json()
    assert rep["ticker"] == "NVDA"
    assert "validation_gate" in rep
    assert "calibration_curve" in rep

    # 2. GET /api/v1/backtest/results
    res_all = client.get("/api/v1/backtest/results", cookies=cookies)
    assert res_all.status_code == 200
    assert isinstance(res_all.json(), list)
    assert len(res_all.json()) > 0


def test_backtester_non_fabrication_on_empty_bins_and_insufficient_trades():
    """Verify backtester returns None for Sharpe and calibration when data is insufficient."""
    mock_redis = MockRedis()
    backtester = StrategyBacktester(redis_client=mock_redis)

    # Flat line price series -> zero trading triggers
    flat_bars = [
        {
            "timestamp": f"2026-01-01T{i:02d}:00:00Z",
            "open": 100.0,
            "high": 100.05,
            "low": 99.95,
            "close": 100.0,
            "volume": 1000,
            "vwap": 100.0,
        }
        for i in range(30)
    ]
    rep = backtester.backtest_strategy(ticker="FLAT", bars=flat_bars, strategy_type="covered_call")
    assert rep["risk_metrics"]["realized_sharpe_ratio"] is None
    assert rep["risk_metrics"]["sortino_ratio"] is None

    # Calibration curve empty bins must have empirical_win_rate = None
    for b in rep["calibration_curve"]:
        assert b["trade_count"] == 0
        assert b["empirical_win_rate"] is None


@pytest.mark.anyio
async def test_paper_broker_missing_price_raises_error():
    """Verify PaperBroker rejects orders without explicit limit/market price and no position."""
    from shared.broker.paper import PaperBroker
    from shared.broker.base import OrderSide
    broker = PaperBroker(initial_cash=10000.0)
    with pytest.raises(ValueError, match="missing limit_price"):
        await broker.submit_order(symbol="NVDA", qty=10, side=OrderSide.BUY)
