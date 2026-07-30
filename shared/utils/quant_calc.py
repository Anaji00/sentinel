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

import math
import logging
from typing import Dict, List, Optional, Tuple, Any

import numpy as np

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
        'spread_mean', 'spread_std', 'half_life'
    """
    if len(series_x) != len(series_y) or len(series_x) < 30:
        return {"is_cointegrated": False, "adf_statistic": 0.0, "beta": 0.0,
                "spread_mean": 0.0, "spread_std": 0.0, "half_life": float("inf")}
    
    x = np.array(series_x, dtype=np.float64)
    y = np.array(series_y, dtype=np.float64)
    
    # Step 1: OLS
    X = np.column_stack([np.ones(len(x)), x])
    try:
        beta_hat = np.linalg.lstsq(X, y, rcond=None)[0]
    except np.linalg.LinAlgError:
        return {"is_cointegrated": False, "adf_statistic": 0.0, "beta": 0.0,
                "spread_mean": 0.0, "spread_std": 0.0, "half_life": float("inf")}
    
    alpha, beta = beta_hat[0], beta_hat[1]
    residuals = y - (alpha + beta * x)
    
    # Step 2: ADF on residuals
    # EG 5% critical value ≈ -2.86 for standard ADF stationary residual check
    adf_result = augmented_dickey_fuller(residuals.tolist())
    is_cointegrated = adf_result["is_stationary"] or adf_result["adf_statistic"] < -2.86
    
    # Half-life of mean reversion: from AR(1) on the spread
    spread_mean = float(np.mean(residuals))
    spread_std = float(np.std(residuals))
    half_life = _half_life_of_mean_reversion(residuals)
    
    return {
        "is_cointegrated": bool(is_cointegrated),
        "adf_statistic": adf_result["adf_statistic"],
        "beta": round(float(beta), 4),
        "spread_mean": round(spread_mean, 4),
        "spread_std": float(spread_std),
        "half_life": round(half_life, 2),
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
                "x_granger_causes_y": False}
    
    x = np.array(series_x, dtype=np.float64)
    y = np.array(series_y, dtype=np.float64)
    
    best_result = {"f_statistic": 0.0, "p_value": 1.0, "optimal_lag": 0,
                   "x_granger_causes_y": False}
    
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
            
            from scipy.stats import f as f_dist
            p_value = 1.0 - f_dist.cdf(f_stat, df1, df2)
            
            if p_value < best_result["p_value"]:
                best_result = {
                    "f_statistic": round(float(f_stat), 4),
                    "p_value": round(float(p_value), 6),
                    "optimal_lag": lag,
                    "x_granger_causes_y": bool(p_value < 0.05),
                }
        except Exception:
            continue
    
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
