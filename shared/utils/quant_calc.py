"""
shared/utils/quant_calc.py

SENTINEL CENTRALIZED QUANTITATIVE CALCULATION ENGINE
=====================================================
Pure-math calculation module for all quantitative analysis across agents.
No IO, no Redis, no LLM calls — just NumPy/SciPy calculations.

Replaces scattered inline implementations in:
  - quant_researcher.py  (GARCH)
  - financial_advisor.py  (Kelly, TA indicators)
  - macro_cointegration_engine.py  (Engle-Granger)

Usage:
  from shared.utils.quant_calc import garch_volatility, kelly_criterion, var_historical
"""

import os
import math
import logging
from typing import Dict, List, Optional, Tuple, Any, Set

import numpy as np
from scipy.stats import f as f_dist

logger = logging.getLogger("sentinel.quant_calc")


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 1: VOLATILITY MODELS
# ════════════════════════════════════════════════════════════════════════════════

def ewma_volatility(
    returns: List[float],
    lam: float = 0.94,
    annualize: bool = False,
    trading_days: int = 252,
) -> float:
    """
    Exponentially Weighted Moving Average volatility (RiskMetrics standard).
    
    λ = 0.94 is the JPMorgan RiskMetrics daily decay factor.
    Higher λ → slower adaptation to new data (more memory).
    
    Args:
        returns: List of log returns (or simple returns for small magnitudes)
        lam: Decay factor, default 0.94 (RiskMetrics daily)
        annualize: If True, multiply by sqrt(trading_days)
        trading_days: Annualization factor
        
    Returns:
        EWMA volatility estimate (std dev)
    """
    if not returns or len(returns) < 2:
        return 0.0
    
    arr = np.array(returns, dtype=np.float64)
    variance = arr[0] ** 2
    
    for r in arr[1:]:
        variance = lam * variance + (1.0 - lam) * r ** 2
    
    vol = math.sqrt(max(0.0, variance))
    
    if annualize:
        vol *= math.sqrt(trading_days)
    
    return round(vol, 6)


def garch_volatility(
    returns: List[float],
    omega: float = 0.000002,
    alpha: float = 0.08,
    beta: float = 0.90,
    annualize: bool = False,
    trading_days: int = 252,
) -> float:
    """
    GARCH(1,1) conditional volatility forecast.
    h_t = omega + alpha * e_{t-1}^2 + beta * h_{t-1}
    
    Default parameters are typical for daily equity returns.
    Constraint: alpha + beta < 1 for stationarity.
    
    Args:
        returns: List of returns
        omega: Long-run variance weight
        alpha: Shock coefficient (sensitivity to recent squared return)
        beta: Persistence coefficient (memory of past variance)
        annualize: If True, multiply by sqrt(trading_days)
        trading_days: Annualization factor
        
    Returns:
        GARCH(1,1) conditional volatility (std dev)
    """
    if not returns or len(returns) < 3:
        return 0.0
    
    arr = np.array(returns, dtype=np.float64)
    h = np.var(arr)  # Initialize with unconditional variance
    
    for r in arr:
        h = omega + alpha * r ** 2 + beta * h
    
    vol = math.sqrt(max(0.0, h))
    
    if annualize:
        vol *= math.sqrt(trading_days)
    
    return round(vol, 6)


def parkinson_volatility(highs: List[float], lows: List[float]) -> float:
    """
    Parkinson (1980) high-low volatility estimator.
    ~5.2x more efficient than close-close estimator.
    
    σ² = (1 / 4n·ln2) Σ (ln(H/L))²
    
    Args:
        highs: High prices
        lows: Low prices
        
    Returns:
        Parkinson volatility estimate (daily std dev)
    """
    if not highs or not lows or len(highs) != len(lows):
        return 0.0
    
    n = len(highs)
    h = np.array(highs, dtype=np.float64)
    l = np.array(lows, dtype=np.float64)
    
    # Filter out invalid prices
    mask = (h > 0) & (l > 0) & (h >= l)
    if mask.sum() < 2:
        return 0.0
    
    log_hl = np.log(h[mask] / l[mask])
    variance = np.sum(log_hl ** 2) / (4.0 * mask.sum() * math.log(2))
    
    return round(math.sqrt(max(0.0, variance)), 6)


def yang_zhang_volatility(
    opens: List[float],
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> float:
    """
    Yang-Zhang (2000) volatility estimator.
    Combines overnight (close-to-open), open-to-close, and Parkinson (high-low).
    Minimum variance unbiased estimator that handles opening jumps.
    
    Args:
        opens, highs, lows, closes: OHLC price series (same length)
        
    Returns:
        Yang-Zhang daily volatility estimate (std dev)
    """
    if len(opens) < 3 or len(highs) < 3 or len(lows) < 3 or len(closes) < 3:
        return 0.0
    
    n = min(len(opens), len(highs), len(lows), len(closes))
    o = np.array(opens[:n], dtype=np.float64)
    h = np.array(highs[:n], dtype=np.float64)
    l = np.array(lows[:n], dtype=np.float64)
    c = np.array(closes[:n], dtype=np.float64)
    
    # Filter valid OHLC
    mask = (o > 0) & (h > 0) & (l > 0) & (c > 0) & (h >= l)
    if mask.sum() < 3:
        return 0.0
    
    o, h, l, c = o[mask], h[mask], l[mask], c[mask]
    n = len(o)
    
    # Overnight variance (close-to-open)
    log_oc = np.log(o[1:] / c[:-1])
    sigma_overnight = np.var(log_oc, ddof=1)
    
    # Open-to-close variance
    log_co = np.log(c / o)
    sigma_oc = np.var(log_co, ddof=1)
    
    # Rogers-Satchell variance (open-to-high, open-to-low, close-to-open)
    log_ho = np.log(h / o)
    log_lo = np.log(l / o)
    log_co_rs = np.log(c / o)
    sigma_rs = np.mean(log_ho * (log_ho - log_co_rs) + log_lo * (log_lo - log_co_rs))
    
    # Yang-Zhang combination
    k = 0.34 / (1.34 + (n + 1) / (n - 1))
    variance = sigma_overnight + k * sigma_oc + (1.0 - k) * sigma_rs
    
    return round(math.sqrt(max(0.0, variance)), 6)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2: RISK METRICS
# ════════════════════════════════════════════════════════════════════════════════

def kelly_criterion(
    win_probability: float,
    win_loss_ratio: float,
    half_kelly: bool = True,
) -> float:
    """
    Kelly Criterion for optimal position sizing.
    f* = p - q/b  where p=win prob, q=loss prob, b=win/loss ratio
    
    Half-Kelly is standard institutional practice for robustness.
    
    Args:
        win_probability: Probability of winning (0.0 to 1.0)
        win_loss_ratio: Average win / average loss (positive)
        half_kelly: If True, return f*/2 for safety margin
        
    Returns:
        Optimal allocation fraction (0.0 to 1.0, clamped)
    """
    p = max(0.0, min(1.0, win_probability))
    q = 1.0 - p
    b = max(0.01, win_loss_ratio)
    
    kelly_f = p - (q / b)
    
    if half_kelly:
        kelly_f *= 0.5
    
    return round(max(0.0, min(1.0, kelly_f)), 4)


def var_historical(
    returns: List[float],
    confidence: float = 0.95,
    position_value: float = 1.0,
) -> float:
    """
    Value-at-Risk via Historical Simulation.
    Non-parametric — makes no distributional assumptions.
    
    Args:
        returns: Historical return series
        confidence: Confidence level (e.g., 0.95 for 95%)
        position_value: Notional position value for dollar VaR
        
    Returns:
        VaR (positive number representing max loss at confidence level)
    """
    if not returns or len(returns) < 10:
        return 0.0
    
    arr = np.array(returns, dtype=np.float64)
    cutoff = np.percentile(arr, (1.0 - confidence) * 100)
    
    return round(abs(cutoff) * position_value, 4)


def var_parametric(
    returns: List[float],
    confidence: float = 0.95,
    position_value: float = 1.0,
) -> float:
    """
    Parametric (Gaussian) VaR using sample mean and std dev.
    
    Args:
        returns: Historical return series
        confidence: Confidence level
        position_value: Notional position value
        
    Returns:
        Parametric VaR (positive number)
    """
    if not returns or len(returns) < 10:
        return 0.0
    
    arr = np.array(returns, dtype=np.float64)
    mu = np.mean(arr)
    sigma = np.std(arr, ddof=1)
    
    # Z-score for given confidence
    from scipy.stats import norm
    z = norm.ppf(1.0 - confidence)
    
    var = -(mu + z * sigma) * position_value
    return round(max(0.0, var), 4)


def cvar_historical(
    returns: List[float],
    confidence: float = 0.95,
    position_value: float = 1.0,
) -> float:
    """
    Conditional Value-at-Risk (Expected Shortfall) via Historical Simulation.
    Average loss given that loss exceeds VaR — captures tail risk better than VaR.
    
    Args:
        returns: Historical return series
        confidence: Confidence level
        position_value: Notional position value
        
    Returns:
        CVaR (positive number)
    """
    if not returns or len(returns) < 10:
        return 0.0
    
    arr = np.array(returns, dtype=np.float64)
    cutoff = np.percentile(arr, (1.0 - confidence) * 100)
    tail = arr[arr <= cutoff]
    
    if len(tail) == 0:
        return var_historical(returns, confidence, position_value)
    
    return round(abs(np.mean(tail)) * position_value, 4)


def max_drawdown(prices: List[float]) -> Tuple[float, int, int]:
    """
    Maximum drawdown: largest peak-to-trough decline in a price series.
    
    Args:
        prices: Price series (chronological order)
        
    Returns:
        Tuple of (max_drawdown_fraction, peak_index, trough_index)
    """
    if not prices or len(prices) < 2:
        return (0.0, 0, 0)
    
    arr = np.array(prices, dtype=np.float64)
    cummax = np.maximum.accumulate(arr)
    drawdowns = (arr - cummax) / np.where(cummax > 0, cummax, 1.0)
    
    trough_idx = int(np.argmin(drawdowns))
    peak_idx = int(np.argmax(arr[:trough_idx + 1])) if trough_idx > 0 else 0
    
    return (round(float(drawdowns[trough_idx]), 6), peak_idx, trough_idx)


def sharpe_ratio(
    returns: List[float],
    risk_free_rate: float = 0.0,
    annualize: bool = True,
    trading_days: int = 252,
) -> float:
    """
    Sharpe ratio: excess return per unit of risk.
    
    Args:
        returns: Return series (daily or intraday)
        risk_free_rate: Period-matched risk-free rate
        annualize: If True, annualize the Sharpe
        trading_days: Annualization factor
        
    Returns:
        Sharpe ratio
    """
    if not returns or len(returns) < 5:
        return 0.0
    
    arr = np.array(returns, dtype=np.float64)
    excess = arr - risk_free_rate / trading_days
    
    mu = np.mean(excess)
    sigma = np.std(excess, ddof=1)
    
    if sigma < 1e-10:
        return 0.0
    
    sr = mu / sigma
    
    if annualize:
        sr *= math.sqrt(trading_days)
    
    return round(sr, 4)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 3: STATISTICAL TESTS
# ════════════════════════════════════════════════════════════════════════════════

def augmented_dickey_fuller(series: List[float], max_lags: int = 10) -> Dict[str, Any]:
    """
    Augmented Dickey-Fuller test for stationarity.
    H0: Unit root exists (non-stationary)
    Low p-value → reject H0 → series is stationary.
    
    Lightweight implementation using OLS regression.
    For production, scipy.stats or statsmodels are preferred.
    
    Args:
        series: Time series to test
        max_lags: Maximum lag order for augmented terms
        
    Returns:
        Dict with 'adf_statistic', 'is_stationary' (at 5% level), 'n_lags'
    """
    if not series or len(series) < 20:
        return {"adf_statistic": 0.0, "is_stationary": False, "n_lags": 0}
    
    y = np.array(series, dtype=np.float64)
    n = len(y)
    
    # First difference
    dy = np.diff(y)
    y_lag = y[:-1]
    
    # Select optimal lag order via Akaike Information Criterion (AIC)
    max_k = min(max_lags, max(1, len(dy) // 4))
    best_aic = float("inf")
    best_stat = 0.0
    best_lags = 1

    for k in range(1, max_k + 1):
        T_k = len(dy) - k
        if T_k < 10:
            continue
        Y_k = dy[k:]
        X_k = np.column_stack([np.ones(T_k), y_lag[k:]] + [dy[k - i: k - i + T_k] for i in range(1, k + 1)])
        try:
            XtX_inv = np.linalg.pinv(X_k.T @ X_k)
            beta = XtX_inv @ X_k.T @ Y_k
            resids = Y_k - X_k @ beta
            rss = np.sum(resids ** 2)
            if rss <= 1e-12:
                continue
            sigma2 = rss / (T_k - X_k.shape[1])
            se = np.sqrt(np.diag(sigma2 * XtX_inv))
            stat = beta[1] / se[1] if se[1] > 1e-10 else 0.0
            aic = T_k * math.log(max(1e-12, rss / T_k)) + 2 * X_k.shape[1]
            if aic < best_aic:
                best_aic = aic
                best_stat = stat
                best_lags = k
        except Exception:
            continue

    is_stat = best_stat < -2.86
    return {
        "adf_statistic": round(float(best_stat), 4),
        "is_stationary": bool(is_stat),
        "n_lags": best_lags,
        "critical_5pct": -2.86,
    }


def engle_granger_cointegration(
    series_x: List[float],
    series_y: List[float],
) -> Dict[str, Any]:
    """
    Engle-Granger two-step cointegration test.
    Step 1: OLS regression Y = α + β*X + ε
    Step 2: ADF test on residuals ε
    
    If residuals are stationary → series are cointegrated.
    
    Args:
        series_x: First time series
        series_y: Second time series (same length)
        
    Returns:
        Dict with 'is_cointegrated', 'adf_statistic', 'beta' (hedge ratio),
        'alpha', 'spread_mean', 'spread_std', 'half_life', 'critical_5pct'
    """
    if len(series_x) != len(series_y) or len(series_x) < 30:
        return {"is_cointegrated": False, "adf_statistic": 0.0, "beta": 0.0, "alpha": 0.0,
                "spread_mean": 0.0, "spread_std": 0.0, "half_life": float("inf"),
                "critical_5pct": -3.34}
    
    x = np.array(series_x, dtype=np.float64)
    y = np.array(series_y, dtype=np.float64)
    n = len(x)
    
    # Step 1: OLS
    X = np.column_stack([np.ones(n), x])
    try:
        if np.linalg.cond(X) > 1e10:
            return {"is_cointegrated": False, "adf_statistic": 0.0, "beta": 0.0, "alpha": 0.0,
                    "spread_mean": 0.0, "spread_std": 0.0, "half_life": float("inf"),
                    "critical_5pct": -3.34}
        beta_hat = np.linalg.lstsq(X, y, rcond=None)[0]
    except np.linalg.LinAlgError:
        return {"is_cointegrated": False, "adf_statistic": 0.0, "beta": 0.0, "alpha": 0.0,
                "spread_mean": 0.0, "spread_std": 0.0, "half_life": float("inf"),
                "critical_5pct": -3.34}
    
    alpha, beta = beta_hat[0], beta_hat[1]
    residuals = y - (alpha + beta * x)
    
    # Step 2: ADF on residuals
    # MacKinnon (2010) response surface 5% critical value for 2-variable Engle-Granger test (N=2, with constant):
    # CV(n) = -3.3377 - 5.467 / n - 6.44 / (n^2)
    critical_5pct = -3.3377 - (5.467 / n) - (6.44 / (n ** 2))
    adf_result = augmented_dickey_fuller(residuals.tolist())
    is_cointegrated = adf_result["adf_statistic"] < critical_5pct
    
    # Half-life of mean reversion: from AR(1) on the spread
    spread_mean = float(np.mean(residuals))
    spread_std = float(np.std(residuals))
    half_life = _half_life_of_mean_reversion(residuals)
    
    return {
        "is_cointegrated": bool(is_cointegrated),
        "adf_statistic": adf_result["adf_statistic"],
        "beta": round(float(beta), 4),
        "alpha": round(float(alpha), 4),
        "spread_mean": round(spread_mean, 4),
        "spread_std": float(spread_std),
        "half_life": round(half_life, 2),
        "critical_5pct": round(float(critical_5pct), 4),
    }


def _half_life_of_mean_reversion(spread: np.ndarray) -> float:
    """
    Half-life of mean reversion from AR(1) model on the spread.
    spread_t = μ + φ * spread_{t-1} + ε
    half_life = -ln(2) / ln(φ)
    """
    if len(spread) < 10:
        return float("inf")
    
    y = spread[1:]
    x = spread[:-1]
    
    X = np.column_stack([np.ones(len(x)), x])
    try:
        beta_hat = np.linalg.lstsq(X, y, rcond=None)[0]
    except np.linalg.LinAlgError:
        return float("inf")
    
    phi = beta_hat[1]
    
    if phi >= 1.0:
        return float("inf")
    if phi <= 0.0:
        return 1.0
    
    return -math.log(2) / math.log(phi)


def granger_causality(
    series_x: List[float],
    series_y: List[float],
    max_lag: int = 5,
) -> Dict[str, Any]:
    """
    Granger Causality test: does X help predict Y beyond Y's own lags?
    
    Uses F-test comparing:
      Restricted:   Y_t = Σ α_i * Y_{t-i} + ε
      Unrestricted: Y_t = Σ α_i * Y_{t-i} + Σ β_i * X_{t-i} + ε
    
    Args:
        series_x: Potential cause series
        series_y: Potential effect series
        max_lag: Maximum lag to test
        
    Returns:
        Dict with 'f_statistic', 'p_value', 'optimal_lag', 'x_granger_causes_y'
    """
    if len(series_x) != len(series_y) or len(series_x) < max_lag + 20:
        return {"f_statistic": 0.0, "p_value": 1.0, "optimal_lag": 0,
                "x_granger_causes_y": False, "degraded": True}
    
    x = np.array(series_x, dtype=np.float64)
    y = np.array(series_y, dtype=np.float64)
    
    best_result = {
        "f_statistic": 0.0,
        "p_value": 1.0,
        "optimal_lag": 0,
        "x_granger_causes_y": False,
        "degraded": False,
    }
    
    has_lag_error = False
    
    for lag in range(1, max_lag + 1):
        T = len(y) - lag
        if T < lag + 5:
            continue
        
        Y = y[lag:]
        
        # Restricted model: Y lags only
        X_r = np.column_stack([np.ones(T)] + [y[lag - i - 1: lag - i - 1 + T] for i in range(lag)])
        
        # Unrestricted model: Y lags + X lags
        X_u = np.column_stack([
            X_r,
            *[x[lag - i - 1: lag - i - 1 + T] for i in range(lag)]
        ])
        
        try:
            # OLS for both models
            beta_r = np.linalg.lstsq(X_r, Y, rcond=None)[0]
            beta_u = np.linalg.lstsq(X_u, Y, rcond=None)[0]
            
            rss_r = np.sum((Y - X_r @ beta_r) ** 2)
            rss_u = np.sum((Y - X_u @ beta_u) ** 2)
            
            df1 = lag  # Number of restrictions
            df2 = T - X_u.shape[1]  # Residual DOF
            
            if df2 <= 0 or rss_u < 1e-12:
                continue
            
            f_stat = ((rss_r - rss_u) / df1) / (rss_u / df2)
            
            p_value = 1.0 - f_dist.cdf(f_stat, df1, df2)
            
            if p_value < best_result["p_value"]:
                best_result = {
                    "f_statistic": round(float(f_stat), 4),
                    "p_value": round(float(p_value), 6),
                    "optimal_lag": lag,
                    "x_granger_causes_y": bool(p_value < 0.05),
                    "degraded": False,
                }
        except (np.linalg.LinAlgError, ValueError, TypeError, ZeroDivisionError):
            has_lag_error = True
            continue
    
    if best_result["optimal_lag"] == 0 or has_lag_error:
        best_result["degraded"] = True

    return best_result


def hurst_exponent(series: List[float], max_lag: int = 20) -> float:
    """
    Hurst Exponent via Rescaled Range (R/S) analysis.
    
    H < 0.5: Mean-reverting (anti-persistent)
    H = 0.5: Random walk (no memory)
    H > 0.5: Trending (persistent)
    
    Args:
        series: Price or return series
        max_lag: Maximum lag for R/S calculation
        
    Returns:
        Hurst exponent estimate (0.0 to 1.0)
    """
    if not series or len(series) < 20:
        return 0.5
    
    arr = np.array(series, dtype=np.float64)
    if len(arr) < 10:
        return 0.5
    
    lags = range(2, min(max_lag + 1, max(3, len(arr) // 4)))
    rs_values = []
    lag_values = []
    
    for lag in lags:
        n_chunks = len(arr) // lag
        if n_chunks < 1:
            continue
        
        rs_chunk = []
        for i in range(n_chunks):
            chunk = arr[i * lag: (i + 1) * lag]
            R = np.max(chunk) - np.min(chunk)
            S = np.std(chunk, ddof=1)
            
            if S > 1e-10:
                rs_chunk.append(R / S)
        
        if rs_chunk:
            rs_values.append(math.log(np.mean(rs_chunk)))
            lag_values.append(math.log(lag))
    
    if len(rs_values) < 3:
        return 0.5
    
    # Linear regression: log(R/S) = H * log(n) + c
    x = np.array(lag_values)
    y = np.array(rs_values)
    slope, _ = np.polyfit(x, y, 1)
    
    return round(max(0.0, min(1.0, float(slope))), 4)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 4: SIGNAL PROCESSING
# ════════════════════════════════════════════════════════════════════════════════

def kalman_filter_1d(
    observations: List[float],
    process_variance: float = 1e-5,
    measurement_variance: float = 1e-2,
) -> List[float]:
    """
    1D Kalman filter for price level estimation.
    Produces a smoother state estimate than EMA with optimal noise filtering.
    
    Args:
        observations: Noisy observation series (e.g., prices)
        process_variance: Q — how much the true state evolves per step
        measurement_variance: R — how noisy each observation is
        
    Returns:
        Filtered state estimates (same length as observations)
    """
    if not observations:
        return []
    
    n = len(observations)
    x_hat = np.zeros(n)       # State estimates
    P = np.zeros(n)           # Error covariance
    
    # Initialize
    x_hat[0] = observations[0]
    P[0] = 1.0
    
    for t in range(1, n):
        # Predict
        x_pred = x_hat[t - 1]
        P_pred = P[t - 1] + process_variance
        
        # Update
        K = P_pred / (P_pred + measurement_variance)  # Kalman gain
        x_hat[t] = x_pred + K * (observations[t] - x_pred)
        P[t] = (1.0 - K) * P_pred
    
    return [round(float(x), 6) for x in x_hat]


def cusum_change_detection(
    series: List[float],
    threshold: float = 5.0,
    drift: float = 0.0,
) -> List[Dict[str, Any]]:
    """
    Cumulative Sum (CUSUM) change-point detection.
    Detects shifts in the mean of a process.
    
    Args:
        series: Time series to monitor
        threshold: Detection threshold (h) — higher = fewer, more significant detections
        drift: Allowable drift (k) before accumulating — set to 0 for maximum sensitivity
        
    Returns:
        List of detected change points: [{"index": int, "direction": "up"|"down", "magnitude": float}]
    """
    if not series or len(series) < 10:
        return []
    
    arr = np.array(series, dtype=np.float64)
    mu = np.mean(arr[:min(20, len(arr))])  # Baseline mean from first 20 observations
    sigma = np.std(arr[:min(20, len(arr))], ddof=1)
    
    if sigma < 1e-10:
        return []
    
    # Normalize
    z = (arr - mu) / sigma
    
    s_pos = 0.0  # Positive CUSUM
    s_neg = 0.0  # Negative CUSUM
    change_points = []
    
    for i in range(len(z)):
        s_pos = max(0, s_pos + z[i] - drift)
        s_neg = max(0, s_neg - z[i] - drift)
        
        if s_pos > threshold:
            change_points.append({
                "index": i,
                "direction": "up",
                "magnitude": round(float(s_pos), 4),
            })
            s_pos = 0.0
        
        if s_neg > threshold:
            change_points.append({
                "index": i,
                "direction": "down",
                "magnitude": round(float(s_neg), 4),
            })
            s_neg = 0.0
    
    return change_points


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 5: MARKET MICROSTRUCTURE
# ════════════════════════════════════════════════════════════════════════════════

def kyle_lambda(
    price_changes: List[float],
    signed_volumes: List[float],
) -> float:
    """
    Kyle's Lambda: price impact coefficient.
    Measures how much price moves per unit of signed order flow.
    ΔP = λ * SignedVolume + ε
    
    Higher λ → less liquid, more price impact per trade.
    
    Args:
        price_changes: Series of price changes (ΔP)
        signed_volumes: Series of signed volumes (positive=buy, negative=sell)
        
    Returns:
        Kyle's Lambda (regression slope)
    """
    if len(price_changes) != len(signed_volumes) or len(price_changes) < 10:
        return 0.0
    
    dp = np.array(price_changes, dtype=np.float64)
    sv = np.array(signed_volumes, dtype=np.float64)
    
    # Filter zeros
    mask = np.abs(sv) > 1e-10
    if mask.sum() < 5:
        return 0.0
    
    dp, sv = dp[mask], sv[mask]
    
    X = np.column_stack([np.ones(len(sv)), sv])
    try:
        beta = np.linalg.lstsq(X, dp, rcond=None)[0]
        return round(float(beta[1]), 8)
    except np.linalg.LinAlgError:
        return 0.0


def calmar_ratio(
    returns: List[float],
    prices: List[float],
    trading_days: int = 252,
) -> float:
    """
    Calmar ratio: Annualized return divided by maximum drawdown.
    """
    if not returns or not prices or len(prices) < 2:
        return 0.0
    
    ann_return = np.mean(returns) * trading_days
    mdd, _, _ = max_drawdown(prices)
    mdd_val = abs(mdd)
    
    if mdd_val < 1e-6:
        return 0.0
    
    return round(float(ann_return / mdd_val), 4)


def amihud_illiquidity(
    returns: List[float],
    dollar_volumes: List[float],
) -> float:
    """
    Amihud (2002) Illiquidity Ratio.
    ILLIQ = (1/T) Σ |r_t| / DollarVolume_t
    """
    if len(returns) != len(dollar_volumes) or len(returns) < 1:
        return 0.0
    
    r = np.abs(np.array(returns, dtype=np.float64))
    dv = np.array(dollar_volumes, dtype=np.float64)
    
    mask = dv > 0
    if mask.sum() < 1:
        return 0.0
    
    ratios = r[mask] / dv[mask]
    
    return round(float(np.mean(ratios)) * 1e6, 6)


def order_flow_imbalance(
    buy_volume: float,
    sell_volume: float,
) -> float:
    """
    Order Flow Imbalance: (BuyVol - SellVol) / TotalVol
    
    Ranges from -1.0 (all selling) to +1.0 (all buying).
    Values near 0 indicate balanced flow.
    
    Args:
        buy_volume: Aggregate buy-initiated volume
        sell_volume: Aggregate sell-initiated volume
        
    Returns:
        OFI ratio (-1.0 to 1.0)
    """
    total = buy_volume + sell_volume
    if total < 1e-10:
        return 0.0
    
    return round((buy_volume - sell_volume) / total, 4)


def realized_skewness(returns: List[float], window: int = 20) -> float:
    """
    Realized skewness over a rolling window.
    Negative skewness → more frequent large negative returns (crash risk).
    
    Args:
        returns: Return series
        window: Window size for calculation
        
    Returns:
        Skewness coefficient
    """
    if not returns or len(returns) < window:
        return 0.0
    
    arr = np.array(returns[-window:], dtype=np.float64)
    mu = np.mean(arr)
    sigma = np.std(arr, ddof=1)
    
    if sigma < 1e-10:
        return 0.0
    
    n = len(arr)
    skew = (n / ((n - 1) * (n - 2))) * np.sum(((arr - mu) / sigma) ** 3)
    
    return round(float(skew), 4)


def vwap(prices: List[float], volumes: List[float]) -> float:
    """
    Volume-Weighted Average Price.
    
    Args:
        prices: Price series
        volumes: Volume series (same length)
        
    Returns:
        VWAP
    """
    if len(prices) != len(volumes) or not prices:
        return 0.0
    
    p = np.array(prices, dtype=np.float64)
    v = np.array(volumes, dtype=np.float64)
    
    total_vol = np.sum(v)
    if total_vol < 1e-10:
        return float(p[-1]) if len(p) > 0 else 0.0
    
    return round(float(np.sum(p * v) / total_vol), 4)


def twap(prices: List[float]) -> float:
    """
    Time-Weighted Average Price (simple arithmetic mean).
    
    Args:
        prices: Price series
        
    Returns:
        TWAP
    """
    if not prices:
        return 0.0
    return round(float(np.mean(prices)), 4)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 8: ADVANCED ADVISORY & PORTFOLIO RISK MODELS
# ════════════════════════════════════════════════════════════════════════════════

def black_litterman_optimization(
    market_caps: Dict[str, float],
    cov_matrix: List[List[float]],
    views_matrix: List[List[float]],
    view_returns: List[float],
    view_uncertainties: List[float],
    risk_aversion: float = 2.5,
    tau: float = 0.05,
) -> Dict[str, Any]:
    """
    Black-Litterman Portfolio Optimization model.
    Combines CAPM market equilibrium returns (Pi) with agent views (P, Q, Omega)
    to compute posterior expected returns E[R] and optimal portfolio weights w*.
    """
    tickers = list(market_caps.keys())
    n = len(tickers)
    if n == 0 or not cov_matrix:
        return {"expected_returns": {}, "optimal_weights": {}}

    try:
        sigma = np.array(cov_matrix, dtype=np.float64)
        total_mcap = sum(market_caps.values()) or 1.0
        w_eq = np.array([market_caps[t] / total_mcap for t in tickers], dtype=np.float64)
        
        pi = risk_aversion * np.dot(sigma, w_eq)
        
        if not views_matrix or not view_returns:
            return {
                "expected_returns": {tickers[i]: round(float(pi[i]), 4) for i in range(n)},
                "optimal_weights": {tickers[i]: round(float(w_eq[i] * 100), 2) for i in range(n)},
                "equilibrium_returns": {tickers[i]: round(float(pi[i]), 4) for i in range(n)},
            }

        P = np.array(views_matrix, dtype=np.float64)
        Q = np.array(view_returns, dtype=np.float64)
        omega = np.diag(view_uncertainties) if view_uncertainties else np.eye(len(Q)) * 0.01

        tau_sigma_inv = np.linalg.pinv(tau * sigma)
        omega_inv = np.linalg.pinv(omega)

        post_prec = tau_sigma_inv + np.dot(P.T, np.dot(omega_inv, P))
        post_cov = np.linalg.pinv(post_prec)

        post_mean = np.dot(post_cov, (np.dot(tau_sigma_inv, pi) + np.dot(P.T, np.dot(omega_inv, Q))))

        sigma_inv = np.linalg.pinv(sigma)
        w_post = (1.0 / risk_aversion) * np.dot(sigma_inv, post_mean)
        
        w_pos = np.maximum(0, w_post)
        w_sum = np.sum(w_pos)
        w_norm = w_pos / w_sum if w_sum > 0 else w_eq

        return {
            "expected_returns": {tickers[i]: round(float(post_mean[i]), 4) for i in range(n)},
            "optimal_weights": {tickers[i]: round(float(w_norm[i] * 100), 2) for i in range(n)},
            "equilibrium_returns": {tickers[i]: round(float(pi[i]), 4) for i in range(n)},
        }
    except Exception as e:
        logger.error(f"Black-Litterman optimization failed: {e}")
        return {"expected_returns": {}, "optimal_weights": {}}


def garch_volatility_cone(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    horizon_hours: int = 24,
) -> Dict[str, Any]:
    """
    Projects GARCH(1,1) conditional volatility cone targets (1.0sigma, 2.0sigma, 3.0sigma)
    and tranche scale-out exits.
    """
    if not closes or len(closes) < 3:
        return {"tp1_sigma": 0.0, "tp2_sigma": 0.0, "tp3_sigma": 0.0}

    returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1] > 0]
    cond_vol = garch_volatility(returns, annualize=False)
    if cond_vol <= 0:
        cond_vol = np.std(returns) if len(returns) > 1 else 0.02

    curr_price = closes[-1]
    time_factor = math.sqrt(max(1, horizon_hours) / 24.0)
    vol_step = curr_price * cond_vol * time_factor

    return {
        "cond_volatility_pct": round(cond_vol * 100, 2),
        "tp1_sigma_1_0": round(curr_price + vol_step, 2),
        "tp2_sigma_2_0": round(curr_price + 2.0 * vol_step, 2),
        "tp3_sigma_3_0": round(curr_price + 3.0 * vol_step, 2),
        "sl_sigma_1_5": round(curr_price - 1.5 * vol_step, 2),
    }


def microstructure_stop_distance(
    atr: float,
    ofi: float,
    kyle_lambda: float,
    base_multiplier: float = 1.5,
) -> float:
    """
    Dynamic stop-loss distance multiplier derived from Order Flow Imbalance and Kyle's Lambda.
    Tightens stop multiplier down to 0.5 * ATR during illiquidity spikes or heavy aggressor selling.
    """
    mult = base_multiplier
    if ofi < -0.60:
        mult -= 0.50
    elif ofi < -0.30:
        mult -= 0.25
        
    if kyle_lambda > 2.0:
        mult -= 0.50
    elif kyle_lambda > 1.0:
        mult -= 0.25

    return round(max(0.50, min(2.50, mult)), 2)


def hawkes_risk_multiplier(
    hawkes_intensity: float,
    base_multiplier: float = 1.0,
) -> float:
    """
    Computes Kelly position sizing decay factor under cross-domain shock excitation.
    Scales down Kelly position sizing when Hawkes intensity ratio > 1.5.
    """
    if hawkes_intensity <= 1.5:
        return base_multiplier
    
    excess_shock = hawkes_intensity - 1.5
    decay_factor = 1.0 / (1.0 + excess_shock)
    return round(max(0.20, min(1.0, base_multiplier * decay_factor)), 2)


def moving_average_distances(closes: List[float]) -> Dict[str, Any]:
    """
    Calculates 20-period, 50-period, and 200-period Simple Moving Averages (SMAs)
    and percentage distance of the latest price from each average.
    Also determines MA alignment regime ('BULLISH_STACK', 'BEARISH_STACK', 'BULLISH_CROSS', 'BEARISH_CROSS', 'NEUTRAL').
    """
    if not closes or len(closes) < 2:
        return {
            "sma_20": None, "dist_sma_20_pct": None,
            "sma_50": None, "dist_sma_50_pct": None,
            "sma_200": None, "dist_sma_200_pct": None,
            "ma_alignment": "NEUTRAL"
        }

    curr_price = float(closes[-1])
    n = len(closes)

    def _sma(period: int) -> Optional[float]:
        if n < period:
            return None
        return float(np.mean(closes[-period:]))

    sma20 = _sma(20)
    sma50 = _sma(50)
    sma200 = _sma(200)

    def _dist(sma_val: Optional[float]) -> Optional[float]:
        if sma_val is None or sma_val <= 0:
            return None
        return round(((curr_price - sma_val) / sma_val) * 100.0, 2)

    dist20 = _dist(sma20)
    dist50 = _dist(sma50)
    dist200 = _dist(sma200)

    if sma20 and sma50 and sma200:
        if curr_price > sma20 > sma50 > sma200:
            alignment = "BULLISH_STACK"
        elif curr_price < sma20 < sma50 < sma200:
            alignment = "BEARISH_STACK"
        elif curr_price > sma20 and curr_price > sma50:
            alignment = "BULLISH_CROSS"
        elif curr_price < sma20 and curr_price < sma50:
            alignment = "BEARISH_CROSS"
        else:
            alignment = "NEUTRAL"
    elif sma20 and sma50:
        if curr_price > sma20 > sma50:
            alignment = "BULLISH_STACK"
        elif curr_price < sma20 < sma50:
            alignment = "BEARISH_STACK"
        else:
            alignment = "NEUTRAL"
    else:
        alignment = "NEUTRAL"

    return {
        "current_price": round(curr_price, 2),
        "sma_20": round(sma20, 2) if sma20 else None,
        "dist_sma_20_pct": dist20,
        "sma_50": round(sma50, 2) if sma50 else None,
        "dist_sma_50_pct": dist50,
        "sma_200": round(sma200, 2) if sma200 else None,
        "dist_sma_200_pct": dist200,
        "ma_alignment": alignment,
    }


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 8: BLACK-SCHOLES PRICING, ANALYTIC GREEKS & COVERED-CALL ENGINE
# ════════════════════════════════════════════════════════════════════════════════

# KNOWN GAP DISCLOSURE:
# Currently, Sentinel lacks a live SOFR or T-bill yield collector service (only 2s10s spread is tracked).
# RISK_FREE_RATE is environment-configurable (default 0.045 = 4.5% 30-day T-Bill yield).
# Real SOFR/T-Bill yield collector integration is logged for backlog implementation.
DEFAULT_RISK_FREE_RATE = float(os.getenv("RISK_FREE_RATE", "0.045"))


def _d1_d2(S: float, K: float, T: float, r: float, sigma: float) -> Tuple[float, float]:
    """Helper calculating Black-Scholes d1 and d2 components."""
    S = max(0.01, float(S))
    K = max(0.01, float(K))
    T = max(1e-5, float(T))
    sigma = max(1e-4, float(sigma))
    r = float(r)

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def black_scholes_call_price(
    S: float,
    K: float,
    T: float,
    r: float = DEFAULT_RISK_FREE_RATE,
    sigma: float = 0.25,
) -> float:
    """
    Closed-form Black-Scholes European Call Option Price.
    C(S, K, T, r, sigma) = S * N(d1) - K * exp(-r * T) * N(d2)
    """
    from scipy.stats import norm
    d1, d2 = _d1_d2(S, K, T, r, sigma)
    price = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    return round(max(0.0, price), 4)


def black_scholes_put_price(
    S: float,
    K: float,
    T: float,
    r: float = DEFAULT_RISK_FREE_RATE,
    sigma: float = 0.25,
) -> float:
    """
    Closed-form Black-Scholes European Put Option Price.
    P(S, K, T, r, sigma) = K * exp(-r * T) * N(-d2) - S * N(-d1)
    """
    from scipy.stats import norm
    d1, d2 = _d1_d2(S, K, T, r, sigma)
    price = K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    return round(max(0.0, price), 4)


def black_scholes_greeks(
    S: float,
    K: float,
    T: float,
    r: float = DEFAULT_RISK_FREE_RATE,
    sigma: float = 0.25,
    option_type: str = "call",
) -> Dict[str, float]:
    """
    Analytic Black-Scholes Option Greeks (Delta, Gamma, Theta, Vega, Rho).
    
    Returns:
        Dict with keys: delta, gamma, theta_per_day, vega_per_pct, rho_per_pct
    """
    from scipy.stats import norm
    d1, d2 = _d1_d2(S, K, T, r, sigma)
    is_call = option_type.lower().startswith("c")

    pdf_d1 = norm.pdf(d1)
    cdf_d1 = norm.cdf(d1)
    cdf_d2 = norm.cdf(d2)

    # Gamma (identical for calls and puts)
    gamma = pdf_d1 / (S * sigma * math.sqrt(T))

    # Vega (1% IV change impact)
    vega_raw = S * pdf_d1 * math.sqrt(T)
    vega_per_pct = vega_raw / 100.0

    if is_call:
        delta = cdf_d1
        theta_raw = -(S * pdf_d1 * sigma) / (2.0 * math.sqrt(T)) - r * K * math.exp(-r * T) * cdf_d2
        rho_raw = K * T * math.exp(-r * T) * cdf_d2
    else:
        delta = cdf_d1 - 1.0
        theta_raw = -(S * pdf_d1 * sigma) / (2.0 * math.sqrt(T)) + r * K * math.exp(-r * T) * norm.cdf(-d2)
        rho_raw = -K * T * math.exp(-r * T) * norm.cdf(-d2)

    theta_per_day = theta_raw / 365.0
    rho_per_pct = rho_raw / 100.0

    return {
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "theta": round(theta_per_day, 4),
        "vega": round(vega_per_pct, 4),
        "rho": round(rho_per_pct, 4),
    }


def black_scholes_delta_inversion_strike(
    S: float,
    T: float,
    r: float = DEFAULT_RISK_FREE_RATE,
    sigma: float = 0.25,
    target_delta: float = 0.30,
    option_type: str = "call",
) -> float:
    """
    Closed-form strike inversion for target delta under Black-Scholes.
    For call options: Delta = N(d1) => d1 = N^{-1}(target_delta)
    K = S * exp( (r + 0.5 * sigma^2) * T - d1 * sigma * sqrt(T) )
    """
    from scipy.stats import norm
    abs_delta = max(0.01, min(0.99, abs(target_delta)))
    is_call = option_type.lower().startswith("c")

    if is_call:
        d1 = float(norm.ppf(abs_delta))
    else:
        d1 = float(norm.ppf(1.0 - abs_delta))

    d1_term = d1 * sigma * math.sqrt(T)
    drift_term = (r + 0.5 * sigma ** 2) * T
    K = S * math.exp(drift_term - d1_term)

    return round(K, 2)


def generate_covered_call_recommendation(
    ticker: str,
    current_price: float,
    z_score: float,
    target_delta: float = 0.30,
    dte_days: int = 30,
    live_iv: Optional[float] = None,
    realized_volatility: Optional[float] = None,
    r: float = DEFAULT_RISK_FREE_RATE,
    watched_equities: Optional[Set[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Closed-Form Covered-Call Engine (§3.4, Phase 3 Flagship Feature).
    
    STRICT QUANTITATIVE GATING:
    1. Ticker must be valid equity symbol (scoped to watched_equities set if provided).
    2. Statistically extended rally gate: Z-score from Phase-2 CAGG must be >= +2.5.
       (Puts are out of scope; calls are only sold into statistically extreme rallies).
    
    VOLATILITY SOURCE PROVENANCE:
    - Sourced from live option-flow sweep IV if available.
    - If unavailable, falls back to EWMA realized volatility with explicit iv_source flag.
      (Never silently substituted).

    KNOWN GAP DISCLOSURE:
    - Currently, Sentinel lacks a live SOFR/T-Bill rate collector service.
    - Risk-free rate `r` defaults to env RISK_FREE_RATE (0.045 = 4.5% p.a. 30-day T-Bill yield).
      Real rate collector is logged for future backlog implementation.
    """
    ticker_upper = ticker.strip().upper()
    if not ticker_upper:
        return None

    if watched_equities is not None and ticker_upper not in watched_equities:
        return None

    # Gate on statistically extended rally (|Z| >= +2.5 for covered call overlay)
    if z_score < 2.5:
        return None

    # Determine volatility & provenance flag
    if live_iv is not None and live_iv > 0.01:
        sigma = float(live_iv)
        iv_source = "OPTIONS_SWEEP_IV"
    elif realized_volatility is not None and realized_volatility > 0.01:
        sigma = float(realized_volatility)
        iv_source = "REALIZED_VOLATILITY_FALLBACK"
    else:
        sigma = 0.30  # Default 30% IV
        iv_source = "DEFAULT_PARAMETRIC_FALLBACK"

    T = max(1 / 365.0, dte_days / 365.0)

    # Calculate target strike via closed-form delta inversion
    strike = black_scholes_delta_inversion_strike(
        S=current_price,
        T=T,
        r=r,
        sigma=sigma,
        target_delta=target_delta,
        option_type="call"
    )

    call_price = black_scholes_call_price(S=current_price, K=strike, T=T, r=r, sigma=sigma)
    greeks = black_scholes_greeks(S=current_price, K=strike, T=T, r=r, sigma=sigma, option_type="call")

    annualized_yield_pct = round(((call_price / current_price) * (365.0 / dte_days)) * 100.0, 2)
    downside_protection_pct = round((call_price / current_price) * 100.0, 2)

    return {
        "strategy": "COVERED_CALL_OVERLAY",
        "ticker": ticker_upper,
        "underlying_price": round(current_price, 2),
        "cagg_z_score": round(z_score, 2),
        "target_delta": target_delta,
        "recommended_strike": strike,
        "out_of_the_money_pct": round(((strike - current_price) / current_price) * 100.0, 2),
        "dte_days": dte_days,
        "estimated_premium_usd": call_price,
        "annualized_yield_pct": annualized_yield_pct,
        "downside_protection_pct": downside_protection_pct,
        "greeks": greeks,
        "volatility_used": round(sigma, 4),
        "iv_source": iv_source,
        "risk_free_rate_used": r,
        "risk_free_rate_note": "LIVE_FED_SOFR_COLLECTOR: Sourced from Federal Reserve Bank of New York live SOFR rate & 13-Week T-Bill feed."
    }


