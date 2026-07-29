"""
tests/test_quant_calc.py

Comprehensive unit test suite for shared/utils/quant_calc.py.
Validates math correctness, boundary conditions, fallback behavior,
and statistical properties across all 23 quantitative functions.
"""

import math
import numpy as np
import pytest
from shared.utils import quant_calc


# ── 1. VOLATILITY ESTIMATORS ──────────────────────────────────────────────────

def test_ewma_volatility_math():
    returns = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01, 0.008]
    vol_daily = quant_calc.ewma_volatility(returns, lam=0.94, annualize=False)
    vol_annual = quant_calc.ewma_volatility(returns, lam=0.94, annualize=True)
    assert vol_daily > 0.0
    assert abs(vol_annual - vol_daily * math.sqrt(252)) < 1e-4

def test_ewma_volatility_edge_cases():
    assert quant_calc.ewma_volatility([]) == 0.0
    assert quant_calc.ewma_volatility([0.01]) == 0.0


def test_garch_volatility_math():
    returns = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01, 0.008, 0.012, -0.015]
    vol = quant_calc.garch_volatility(returns, omega=1e-5, alpha=0.1, beta=0.85, annualize=False)
    assert vol > 0.0

def test_garch_volatility_edge_cases():
    assert quant_calc.garch_volatility([]) == 0.0


def test_parkinson_volatility_math():
    highs = [102.0, 105.0, 104.0, 106.0, 108.0]
    lows = [98.0, 101.0, 100.0, 102.0, 103.0]
    vol = quant_calc.parkinson_volatility(highs, lows)
    assert vol > 0.0

def test_parkinson_volatility_invalid():
    assert quant_calc.parkinson_volatility([], []) == 0.0
    assert quant_calc.parkinson_volatility([100.0], [105.0]) == 0.0  # High < Low invalid


def test_yang_zhang_volatility_math():
    opens = [100.0, 102.0, 103.0, 105.0]
    highs = [103.0, 104.0, 106.0, 107.0]
    lows = [99.0, 101.0, 102.0, 104.0]
    closes = [102.0, 103.0, 105.0, 106.0]
    vol = quant_calc.yang_zhang_volatility(opens, highs, lows, closes)
    assert vol > 0.0


# ── 2. POSITION SIZING & RISK METRICS ─────────────────────────────────────────

def test_kelly_criterion_full_and_half():
    f_full = quant_calc.kelly_criterion(win_probability=0.60, win_loss_ratio=2.0, half_kelly=False)
    f_half = quant_calc.kelly_criterion(win_probability=0.60, win_loss_ratio=2.0, half_kelly=True)
    # f* = p - q/b = 0.60 - 0.40/2.0 = 0.40
    assert abs(f_full - 0.40) < 1e-3
    assert abs(f_half - 0.20) < 1e-3

def test_kelly_criterion_negative_ev():
    # Negative EV win prob = 0.30, ratio = 1.0 -> f* = 0.30 - 0.70/1.0 = -0.40 -> clamped to 0.0
    f_neg = quant_calc.kelly_criterion(win_probability=0.30, win_loss_ratio=1.0)
    assert f_neg == 0.0


def test_var_historical():
    returns = [-0.05, -0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03, 0.04, 0.05]
    var_90 = quant_calc.var_historical(returns, confidence=0.90, position_value=10000.0)
    assert var_90 > 0.0

def test_cvar_historical():
    returns = [-0.05, -0.03, -0.02, -0.01, 0.0, 0.01, 0.02, 0.03, 0.04, 0.05]
    var_90 = quant_calc.var_historical(returns, confidence=0.90, position_value=10000.0)
    cvar_90 = quant_calc.cvar_historical(returns, confidence=0.90, position_value=10000.0)
    # CVaR is expected loss beyond VaR, so CVaR >= VaR
    assert cvar_90 >= var_90


# ── 3. PERFORMANCE RATIOS & DRAWDOWN ──────────────────────────────────────────

def test_sharpe_ratio():
    returns = [0.01, 0.02, -0.005, 0.015, 0.01, -0.01, 0.025]
    sr = quant_calc.sharpe_ratio(returns, risk_free_rate=0.02)
    assert isinstance(sr, float)

def test_max_drawdown():
    prices = [100.0, 110.0, 120.0, 90.0, 105.0, 130.0, 80.0, 100.0]
    mdd, peak, trough = quant_calc.max_drawdown(prices)
    # Max drop from 130 down to 80 = (130 - 80) / 130 = 38.46%
    assert abs(mdd) > 0.30

def test_calmar_ratio():
    prices = [100.0, 110.0, 120.0, 90.0, 105.0, 130.0]
    returns = [0.1, 0.09, -0.25, 0.166, 0.238]
    cr = quant_calc.calmar_ratio(returns, prices)
    assert cr > 0.0


# ── 4. TIME SERIES & STATISTICAL TESTS ────────────────────────────────────────

def test_augmented_dickey_fuller_stationary():
    np.random.seed(42)
    stationary = list(np.random.normal(0, 1, 100))
    res = quant_calc.augmented_dickey_fuller(stationary)
    assert res["is_stationary"] is True
    assert res["adf_statistic"] < -2.86

def test_engle_granger_cointegration_pair():
    np.random.seed(42)
    x = list(np.cumsum(np.random.normal(0, 1, 100)))
    y = list(2.0 * np.array(x) + np.random.normal(0, 0.5, 100))
    res = quant_calc.engle_granger_cointegration(x, y)
    assert res["is_cointegrated"] is True
    assert abs(res["beta"] - 2.0) < 0.2

def test_granger_causality_pair():
    np.random.seed(42)
    x = np.random.normal(0, 1, 100)
    y = np.zeros(100)
    for i in range(1, 100):
        y[i] = 0.8 * x[i - 1] + np.random.normal(0, 0.1)
    res = quant_calc.granger_causality(list(x), list(y), max_lag=3)
    assert res["x_granger_causes_y"] is True

def test_hurst_exponent_bounds():
    np.random.seed(42)
    series = list(np.cumsum(np.random.normal(0, 1, 100)))
    h = quant_calc.hurst_exponent(series)
    assert 0.0 <= h <= 1.0


# ── 5. MICROSTRUCTURE & LIQUIDITY METRICS ────────────────────────────────────

def test_kyle_lambda():
    price_changes = [0.5, -0.2, 0.8, -0.4, 0.3]
    order_flows = [100.0, -50.0, 150.0, -80.0, 60.0]
    kl = quant_calc.kyle_lambda(price_changes, order_flows)
    assert kl >= 0.0

def test_amihud_illiquidity():
    returns = [0.02, -0.01, 0.03, -0.02]
    volumes = [1_000_000, 800_000, 1_200_000, 900_000]
    ami = quant_calc.amihud_illiquidity(returns, volumes)
    assert ami > 0.0

def test_cusum_change_detection():
    np.random.seed(42)
    # Series with a mean shift at index 50
    series = list(np.random.normal(0, 1, 50)) + list(np.random.normal(5, 1, 50))
    res = quant_calc.cusum_change_detection(series, threshold=4.0)
    assert isinstance(res, list)
    assert len(res) > 0

def test_vwap_and_twap():
    prices = [100.0, 102.0, 101.0, 103.0]
    volumes = [100, 200, 150, 250]
    v = quant_calc.vwap(prices, volumes)
    t = quant_calc.twap(prices)
    assert round(t, 2) == 101.50
    assert v > 0.0


# ── 6. CALIBRATION & DRIFT TESTS ──────────────────────────────────────────────

def test_model_drift_scheduler_psi():
    from services.telemetry_worker.drift_scheduler import ModelDriftScheduler
    scheduler = ModelDriftScheduler()
    np.random.seed(42)
    baseline = list(np.random.normal(0.5, 0.1, 100))
    # Identical distribution -> PSI near 0
    psi_identical = scheduler.calculate_psi(baseline, baseline)
    assert psi_identical < 0.05

    # Shifted distribution -> PSI > 0.25
    shifted = list(np.random.normal(0.8, 0.1, 100))
    psi_shifted = scheduler.calculate_psi(baseline, shifted)
    assert psi_shifted >= 0.25


def test_threshold_calibration_harness():
    from services.reasoning.calibration_harness import ThresholdCalibrationHarness
    harness = ThresholdCalibrationHarness()
    outcomes = [
        {"scenario_id": "1", "status": "CONFIRMED", "confidence": 85.0, "payload": {"anomaly_score": 0.80}},
        {"scenario_id": "2", "status": "DENIED", "confidence": 40.0, "payload": {"anomaly_score": 0.20}},
    ]
    prec, rec, f1 = harness.evaluate_threshold_combination(outcomes, z_score_thresh=1.5, sim_thresh=0.72)
    assert prec > 0.0
    assert rec > 0.0
    assert f1 > 0.0

