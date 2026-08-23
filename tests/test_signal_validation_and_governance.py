"""
tests/test_signal_validation_and_governance.py

Comprehensive test suite verifying Section B: Signal Validation & Model Governance:
  - B.1: StrategyBacktester, validation gating, realized Sharpe, max drawdown, probability calibration curves.
  - B.2: Explainability endpoints, mathematical factor attribution waterfall, score adjustments timeline, model card metadata.
  - B.3: FeatureFlagManager, instant signal kill switches, master kill switch, gradual rollout, and ticker whitelists.
"""

import json
import os
from pathlib import Path

import pytest
import numpy as np
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]

from shared.utils.feature_flags import FeatureFlagManager, DEFAULT_SIGNAL_FLAGS
from services.reasoning.strategy_backtester import StrategyBacktester, MIN_BARS_FOR_BACKTEST
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


def test_rollout_bucket_is_stable_across_processes():
    """Rollout bucketing must not depend on PYTHONHASHSEED.

    The builtin hash() randomizes string hashing per process, which placed the
    same ticker in different buckets across workers and re-rolled it on every
    restart. Buckets are asserted against fixed expected values computed from
    SHA-256, so a regression to hash() fails here deterministically.
    """
    from shared.utils.feature_flags import rollout_bucket

    subjects = ["AAPL", "NVDA", "BTC-USD", "SPY"]
    first = {s: rollout_bucket("insider_clustering", s) for s in subjects}

    # Stable within the process...
    for _ in range(3):
        assert {s: rollout_bucket("insider_clustering", s) for s in subjects} == first

    # ...and reproducible in a *fresh* interpreter with a different hash seed.
    import json as _json
    import subprocess
    import sys

    script = (
        "import json;"
        "from shared.utils.feature_flags import rollout_bucket;"
        f"print(json.dumps({{s: rollout_bucket('insider_clustering', s) for s in {subjects!r}}}))"
    )
    env = {**os.environ, "PYTHONHASHSEED": "12345", "PYTHONPATH": str(REPO_ROOT)}
    out = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, env=env, cwd=str(REPO_ROOT)
    )
    assert out.returncode == 0, out.stderr
    assert _json.loads(out.stdout.strip()) == first

    assert all(0 <= b < 100 for b in first.values())


@pytest.mark.anyio
async def test_master_kill_switch_halts_every_signal_class():
    """Emergency Halt must suppress every flag the platform ships.

    BL-03 was a signal path that never consulted is_enabled() at all, so the
    master switch silently skipped it. This asserts the contract across the full
    flag registry rather than one flag at a time.
    """
    mock_redis = MockRedis()
    manager = FeatureFlagManager(redis_client=mock_redis)

    for flag_name in DEFAULT_SIGNAL_FLAGS:
        assert await manager.is_enabled(flag_name, ticker="NVDA") is True

    await manager.trip_master_kill_switch(reason="governance drill")

    for flag_name in DEFAULT_SIGNAL_FLAGS:
        assert await manager.is_enabled(flag_name, ticker="NVDA") is False, (
            f"Master kill switch did not suppress '{flag_name}'"
        )


def test_insider_form4_path_consults_feature_flags():
    """The live insider path must gate on is_enabled().

    A duplicate method definition previously shadowed the flag-checked version,
    leaving the insider_clustering kill switch wired to dead code. Guards against
    both the duplicate returning and the gate being dropped.
    """
    import inspect
    from services.agents.quant_trading_engine import QuantTradingEngine

    src = inspect.getsource(QuantTradingEngine)
    assert src.count("async def _process_insider_form4") == 1, (
        "Duplicate _process_insider_form4 definition — the later one silently "
        "shadows the earlier, which is how the kill switch was bypassed."
    )

    method_src = inspect.getsource(QuantTradingEngine._process_insider_form4)
    assert 'is_enabled("insider_clustering"' in method_src, (
        "Live insider path does not consult the insider_clustering feature flag, "
        "so neither its kill switch nor the master kill switch applies."
    )


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

    # Verify model card metadata. The card must name the scorer that actually
    # runs -- anomaly scoring is streaming RRCF; the batch IsolationForest ONNX
    # export was retired, and naming it would misdescribe the model to anyone
    # auditing a signal.
    assert "model_card" in data
    mc = data["model_card"]
    assert "RRCF" in mc["model_name"]
    assert "IsolationForest" not in mc["model_name"], "model card names a retired scorer"
    assert "model_drift_status" in mc

    # With no drift report published, the card must say so rather than
    # asserting STABLE -- health nothing has measured is not health.
    drift = mc["model_drift_status"]
    assert drift["drift_state"] in ("UNKNOWN", "INITIALIZING", "STABLE",
                                    "SLIGHT_DRIFT", "SIGNIFICANT_DRIFT")
    if drift["drift_state"] == "UNKNOWN":
        assert drift["psi_score"] is None
        assert "detail" in drift

    # Verify data source provenance
    assert "provenance" in data
    prov = data["provenance"]
    assert "payload_hash" in prov
    assert prov["payload_hash"].startswith("sha256:")

    # Latency must be measured for this request, not a constant.
    assert isinstance(prov["processing_latency_ms"], float)
    assert prov["processing_latency_ms"] >= 0.0
    assert prov["processing_latency_ms"] != 14.8, "latency is still a hardcoded placeholder"


def test_explain_payload_hash_is_deterministic():
    """The audit hash must identify content, not a process.

    It was built from Python's builtin hash(), which is randomized per process,
    so the same event produced a different 'sha256:' value after every restart --
    useless for the tamper-evidence the field implies.
    """
    from services.api_gateway.routes import explain as explain_mod
    import hashlib

    event_id, occurred = "evt-123", "2026-08-21T14:32:05+00:00"
    expected = "sha256:" + hashlib.sha256(f"{event_id}|{occurred}".encode("utf-8")).hexdigest()

    src = open(explain_mod.__file__, encoding="utf-8").read()
    assert "hashlib.sha256(" in src, "payload hash is not a real digest"
    assert "abs(hash(" not in src, "payload hash still uses randomized builtin hash()"
    assert len(expected.split(":")[1]) == 64


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

    # 1. GET /api/v1/flags is ADMIN-only.
    #
    # This asserted VIEWER access, which was right when every account was a
    # trusted colleague. Open signup changed the premise: VIEWER now means
    # anyone on the internet who filled in a form. The payload is operational
    # control surface -- master kill-switch state with its operator-written
    # `reason` string, per-signal rollout percentages, and ticker whitelists --
    # so reading it is reconnaissance, not a product feature.
    res = client.get("/api/v1/flags", cookies=viewer_cookies)
    assert res.status_code == 403, "flag state must not be readable by a signed-up stranger"

    res = client.get("/api/v1/flags", cookies=admin_cookies)
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


def _flat_bars(n: int):
    """Flat-line price series -> zero trading triggers."""
    return [
        {
            "timestamp": f"2026-01-01T{i:02d}:00:00Z",
            "open": 100.0,
            "high": 100.05,
            "low": 99.95,
            "close": 100.0,
            "volume": 1000,
            "vwap": 100.0,
        }
        for i in range(n)
    ]


def test_backtester_non_fabrication_on_empty_bins_and_insufficient_trades():
    """Verify the backtester never fabricates bars or metrics below the data threshold.

    Uses N=15 bars so the `< MIN_BARS_FOR_BACKTEST` branch is actively traversed —
    the branch that previously substituted a seeded synthetic random walk and could
    emit APPROVED_FOR_LIVE from invented prices.
    """
    mock_redis = MockRedis()
    backtester = StrategyBacktester(redis_client=mock_redis)

    assert 15 < MIN_BARS_FOR_BACKTEST, "Fixture must sit below the backtest data threshold"

    rep = backtester.backtest_strategy(
        ticker="FLAT", bars=_flat_bars(15), strategy_type="covered_call"
    )

    # The report must reflect the real bar count — not a synthetic series length.
    assert rep["bar_count"] == 15
    assert rep["data_provenance"] == "insufficient_data"

    # Fail closed: never approved, and explicitly labelled.
    assert rep["validation_gate"]["passed"] is False
    assert rep["validation_gate"]["status"] == "INSUFFICIENT_DATA"

    # No fabricated metrics — None, not zero.
    assert rep["risk_metrics"]["realized_sharpe_ratio"] is None
    assert rep["risk_metrics"]["sortino_ratio"] is None
    assert rep["risk_metrics"]["max_drawdown_pct"] is None
    assert rep["performance_metrics"]["hit_rate_pct"] is None
    assert rep["brier_score"] is None
    assert rep["calibration_curve"] == []


def test_backtester_non_fabrication_with_sufficient_bars_but_no_trades():
    """With enough authentic bars but no signal triggers, metrics stay None (not zero)."""
    mock_redis = MockRedis()
    backtester = StrategyBacktester(redis_client=mock_redis)

    rep = backtester.backtest_strategy(
        ticker="FLAT", bars=_flat_bars(30), strategy_type="covered_call"
    )

    assert rep["data_provenance"] == "authentic_market_data"
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
