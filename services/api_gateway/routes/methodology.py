"""
services/api_gateway/routes/methodology.py

PUBLIC MATHEMATICAL METHODOLOGY SURFACE (§3.3)
==============================================
Exposes mathematical derivations, assumptions, closed-form equations,
data dependencies, and Z-score calibration gates for all Sentinel signals.
"""

from shared.utils.rbac import require_role, Role
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/v1/methodology", tags=["Mathematical Methodology & Whitepaper"])


class MethodologyParameter(BaseModel):
    name: str
    symbol: str
    description: str
    default_value: str
    calibration_method: str


class SignalMethodology(BaseModel):
    id: str
    name: str
    category: str
    epistemic_tier: str
    description: str
    formula_latex: str
    assumptions: List[str]
    parameters: List[MethodologyParameter]
    data_inputs: List[str]
    validation_gate: str
    falsifiability_condition: str


METHODOLOGY_CATALOG: Dict[str, SignalMethodology] = {
    "covered_call_optimization": SignalMethodology(
        id="covered_call_optimization",
        name="Covered Call Overlay Optimization",
        category="Derivatives & Options",
        epistemic_tier="Deterministic Closed-Form",
        description="Selects optimal out-of-the-money (OTM) strike prices that maximize annualized option yield while bounding upside assignment probability.",
        formula_latex=r"C(S, K, T) = S \cdot N(d_1) - K \cdot e^{-r T} \cdot N(d_2), \quad d_1 = \frac{\ln(S/K) + (r + \sigma^2/2)T}{\sigma \sqrt{T}}, \quad d_2 = d_1 - \sigma \sqrt{T}",
        assumptions=[
            "Geometric Brownian Motion for underlying asset price",
            "Frictionless market with continuous hedging",
            "Implied volatility solved via Brent-Dekker root finding on market bid/ask midpoints",
        ],
        parameters=[
            MethodologyParameter(
                name="Delta Target Window",
                symbol=r"\Delta",
                description="Target delta range for short call strike selection",
                default_value="[0.20, 0.35]",
                # Was "Empirically calibrated to maximize risk-adjusted Sharpe
                # over 5-year rolling backtests". The backtester is invoked with
                # timeframe="5m" and limit=BACKTEST_REFRESH_BARS=300 -- 1,500
                # minutes, under four trading days -- and "5-year" appeared
                # nowhere else in the codebase. It described a process that did
                # not exist, on an endpoint that required no authentication.
                calibration_method=(
                    "Hand-set defaults, validated against a rolling short-horizon "
                    "backtest over the available bar history; not yet calibrated "
                    "over a multi-year sample"
                ),
            ),
            MethodologyParameter(
                name="Risk-Free Rate",
                symbol=r"r",
                description="Annualized SOFR benchmark risk-free rate",
                default_value="Live NY Fed SOFR",
                calibration_method="Queried dynamically from nyfed_sofr feed in Redis",
            ),
        ],
        data_inputs=["tradfi:equity_quote", "alpaca_options:chain", "nyfed_sofr:rate"],
        validation_gate="Backtested over historical CAGG bars; signal rejected if annualized yield < 6% or drawdown > 15%",
        falsifiability_condition="Realized return underperforms passive buy-and-hold across 3 consecutive expiration cycles.",
    ),
    "granger_causality": SignalMethodology(
        id="granger_causality",
        name="Pairwise Bivariate Granger Causality",
        category="Statistical Discovery",
        epistemic_tier="Statistical Empirical",
        description="Tests whether historical lagged returns of Series X improve the autoregressive forecast accuracy of Series Y beyond Y's own history.",
        formula_latex=r"Y_t = c + \sum_{i=1}^p \alpha_i Y_{t-i} + \sum_{j=1}^p \beta_j X_{t-j} + \epsilon_t, \quad F = \frac{(RSS_R - RSS_U)/p}{RSS_U / (T - 2p - 1)}",
        assumptions=[
            "Stationary time series (verified via Augmented Dickey-Fuller test)",
            "Linear lag dependencies up to maximum lag order p",
            "Independent and identically distributed Gaussian error residuals",
        ],
        parameters=[
            MethodologyParameter(
                name="P-Value Significance Cutoff",
                symbol=r"\alpha",
                description="Maximum acceptable p-value under null hypothesis H0: beta_1 = ... = beta_p = 0",
                default_value="0.05",
                calibration_method="ThresholdCalibrationHarness rolling 95th percentile",
            ),
            MethodologyParameter(
                name="Lag Horizon",
                symbol=r"p",
                description="Optimal lag order determined by Akaike Information Criterion (AIC)",
                default_value="3 periods",
                calibration_method="Minimized AIC over lag grid [1, 10]",
            ),
        ],
        data_inputs=["tradfi_bars:continuous_aggregates", "graph_topology:candidate_pairs"],
        validation_gate="Candidate edges must pass rolling 30-day Granger F-test with p < 0.05 before ingestion as GRANGER_CAUSES edges.",
        falsifiability_condition="Out-of-sample directional predictive accuracy drops below 50% across a 20-period test window.",
    ),
    "hawkes_point_process": SignalMethodology(
        id="hawkes_point_process",
        name="Mutually Exciting Hawkes Cross-Domain Contagion",
        category="Cross-Domain Epistemic",
        epistemic_tier="Stochastic Process",
        description="Quantifies self- and mutually-exciting jump clustering across maritime chokepoint events, geopolitical news, cyber disclosures, and market volatility.",
        formula_latex=r"\lambda_i(t) = \mu_i + \sum_{j=1}^M \sum_{t_k < t} \alpha_{ij} \cdot e^{-\beta_{ij}(t - t_k)}",
        assumptions=[
            "Temporal point process with exponential memory decay kernels",
            "Spectral radius of branching matrix ||Alpha|| < 1 to guarantee subcritical stability",
        ],
        parameters=[
            MethodologyParameter(
                name="Decay Half-Life",
                symbol=r"t_{1/2}",
                description="Temporal half-life of cross-domain excitation memory",
                default_value="3600 seconds (1 hour)",
                calibration_method="Maximum likelihood estimation on empirical multi-domain event bursts",
            ),
            MethodologyParameter(
                name="Branching Ratio",
                symbol=r"\eta",
                description="Average number of secondary daughter events triggered by an initial parent event",
                default_value="0.65",
                calibration_method="Fitted via EM algorithm on historical event logs",
            ),
        ],
        data_inputs=["events.enriched:all_domains", "redis:hawkes_tracker"],
        validation_gate="Subcriticality gate: spectral radius must strictly remain < 1.0; branches clamped if self-excitation diverges.",
        falsifiability_condition="Observed inter-arrival event times conform to a homogeneous Poisson process (p > 0.10 via KS test).",
    ),
    "parametric_var_cvar": SignalMethodology(
        id="parametric_var_cvar",
        name="Parametric Value at Risk (VaR) & Expected Shortfall (CVaR)",
        category="Risk & Governance",
        epistemic_tier="Deterministic Closed-Form",
        description="Computes 1-Day 95% Parametric VaR and 99% Conditional VaR using an empirical covariance matrix and Herfindahl concentration index.",
        formula_latex=r"\text{VaR}_{\alpha} = Z_{\alpha} \cdot \sigma_P \cdot V_P, \quad \text{CVaR}_{\alpha} = \frac{\phi(Z_{\alpha})}{1 - \alpha} \cdot \sigma_P \cdot V_P",
        assumptions=[
            "Multivariate normal distribution of asset log returns over 1-day horizon",
            "Linear portfolio valuation with constant position weights over the holding period",
        ],
        parameters=[
            MethodologyParameter(
                name="VaR Confidence Level",
                symbol=r"\alpha",
                description="1-tail confidence percentile",
                default_value="95% (Z = 1.6449)",
                calibration_method="Basel Committee market risk standard",
            ),
            MethodologyParameter(
                name="CVaR Confidence Level",
                symbol=r"\beta",
                description="Conditional tail expectation percentile",
                default_value="99% (Z = 2.3263)",
                calibration_method="FRTB (Fundamental Review of the Trading Book) standard",
            ),
        ],
        data_inputs=["broker:positions", "broker:account", "tradfi:historical_covariance"],
        validation_gate="Kupiec proportion-of-failures (POF) test: exceptions must match theoretical 5% frequency within binomial confidence bands.",
        falsifiability_condition="Realized daily portfolio losses breach 95% VaR threshold more than 4 times in a 60-day window.",
    ),
    "half_kelly_position_sizing": SignalMethodology(
        id="half_kelly_position_sizing",
        name="Half-Kelly Optimal Capital Sizing",
        category="Quantitative Strategy",
        epistemic_tier="Deterministic Closed-Form",
        description="Calculates fraction of portfolio equity to allocate to high-conviction trade setups while reducing drawdown variance by half.",
        formula_latex=r"K^* = \frac{p \cdot b - q}{b}, \quad K_{\text{alloc}} = \min\left(K_{\text{max}}, 0.5 \cdot K^*\right)",
        assumptions=[
            "Logarithmic utility maximization (growth-optimal)",
            "Known win probability p and risk/reward payoff ratio b",
        ],
        parameters=[
            MethodologyParameter(
                name="Kelly Damping Factor",
                symbol=r"\gamma",
                description="Fraction of full Kelly criterion implemented to prevent over-betting",
                default_value="0.50 (Half-Kelly)",
                calibration_method="Standard quantitative practice for fat-tailed asset distributions",
            ),
            MethodologyParameter(
                name="Hard Position Clamp",
                symbol=r"K_{\text{max}}",
                description="Maximum allowable position size as a percentage of total equity",
                default_value="15.0%",
                calibration_method="Platform risk governance hard rule",
            ),
        ],
        data_inputs=["quant_engine:win_rate", "quant_engine:risk_reward_ratio", "broker:account_equity"],
        validation_gate="Allocation rejected if calculated Kelly fraction <= 0 or expected value EV <= 0.",
        falsifiability_condition="Realized strategy win rate drops below break-even threshold 1/(1+b).",
    ),
}


@router.get("", response_model=List[Dict[str, Any]])
async def get_methodology_catalog(
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
):
    """
    Returns high-level catalog of all mathematical methodologies implemented in Sentinel.
    """
    return [
        {
            "id": m.id,
            "name": m.name,
            "category": m.category,
            "epistemic_tier": m.epistemic_tier,
            "description": m.description,
            "data_inputs": m.data_inputs,
        }
        for m in METHODOLOGY_CATALOG.values()
    ]


@router.get("/{signal_id}", response_model=SignalMethodology)
async def get_signal_methodology(
    signal_id: str,
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
):
    """
    Returns exact mathematical equation, assumptions, parameter calibrations,
    and falsifiability conditions for a specific signal type.
    """
    clean_id = signal_id.lower().strip()
    if clean_id not in METHODOLOGY_CATALOG:
        raise HTTPException(status_code=404, detail=f"Methodology for '{signal_id}' not found.")
    return METHODOLOGY_CATALOG[clean_id]
