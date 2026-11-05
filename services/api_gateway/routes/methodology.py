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
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1/methodology", tags=["Mathematical Methodology & Whitepaper"])


class MethodologyParameter(BaseModel):
    name: str
    symbol: str
    description: str
    default_value: str
    # Whether this parameter is learned from live data or fixed in code.
    #
    # A published fixed threshold is a specification of what to stay under, for
    # a platform whose product is noticing behaviour. A learned one moves with
    # the distribution it is drawn from, so knowing today's value buys much
    # less. Saying which is which is the honest half of that: a reader can tell
    # a constant an adversary can plan against from a quantile that will have
    # moved by the time they act.
    is_learned: bool = False
    calibration_method: str


class SignalMethodology(BaseModel):
    id: str
    name: str
    # What it does, for someone who has never met the maths.
    plain: str
    category: str
    epistemic_tier: str
    description: str
    formula_latex: str
    # The same formula as readable text: the page has no LaTeX renderer.
    formula_plain: str
    assumptions: List[str]
    parameters: List[MethodologyParameter]
    data_inputs: List[str]
    validation_gate: str
    # The callable that actually runs the gate above, as a dotted path.
    #
    # `validation_gate` is prose on a public endpoint whose entire purpose is to
    # let a reader check the platform's claims, and prose cannot be checked.
    # This audit already found one gate describing a Kupiec test that nothing in
    # the platform runs; that entry now opens with "None." and says why. This
    # field is what makes the distinction mechanical rather than remembered:
    # a gate is either implemented and names its implementation, or it declares
    # that it is not. test_validation_gates_name_real_code resolves every one.
    validation_gate_impl: Optional[str] = None
    falsifiability_condition: str


METHODOLOGY_CATALOG: Dict[str, SignalMethodology] = {
    "covered_call_optimization": SignalMethodology(
        id="covered_call_optimization",
        name="Covered Call Overlay",
        plain=(
            "If you own a stock, you can sell someone the right to buy it from you at a higher price and keep "
            "the fee. After an unusually strong rally, this works out which price to offer on a 30-day contract, "
            "so that there is roughly a 30% chance of having to sell."
        ),
        category="Derivatives & Options",
        epistemic_tier="Deterministic Closed-Form",
        description=(
            "Finds the strike whose Black-Scholes delta is 0.30 on a 30-day call, offered only when the stock's "
            "z-score is at least +2.5. It solves for one fixed delta; it does not search for the highest yield."
        ),
        formula_latex=r"C(S, K, T) = S \cdot N(d_1) - K \cdot e^{-r T} \cdot N(d_2), \quad d_1 = \frac{\ln(S/K) + (r + \sigma^2/2)T}{\sigma \sqrt{T}}, \quad d_2 = d_1 - \sigma \sqrt{T}",
        formula_plain="C(S, K, T) = S · N(d1) - K · e^(-rT) · N(d2),  where d1 = [ln(S/K) + (r + σ²/2)T] / (σ√T),  d2 = d1 - σ√T",
        assumptions=[
            "Geometric Brownian Motion for underlying asset price",
            "Frictionless market with continuous hedging",
            # Was "solved via Brent-Dekker root finding on market bid/ask
            # midpoints". Nothing on this path solves for volatility: it is
            # handed one, and iv_source on the result says which of these it was.
            "Volatility is the implied volatility from a recent large option print when there is one, then the stock's "
            "recent realised volatility, then a flat 30%; each recommendation records which it used",
        ],
        parameters=[
            MethodologyParameter(
                name="Target Delta",
                symbol=r"\Delta",
                description="Delta the short call's strike is solved for",
                default_value="0.30",
                # Was "Empirically calibrated to maximize risk-adjusted Sharpe
                # over 5-year rolling backtests". The backtester is invoked with
                # timeframe="5m" and limit=BACKTEST_REFRESH_BARS=300 -- 1,500
                # minutes, under four trading days -- and "5-year" appeared
                # nowhere else in the codebase. It described a process that did
                # not exist, on an endpoint that required no authentication.
                # The window [0.20, 0.35] and the "validated against a backtest"
                # that replaced it had nothing behind them either: the engine
                # passes target_delta=0.30 and no backtest result reaches it.
                calibration_method="Hand-set and fixed; not fitted to any sample",
            ),
            MethodologyParameter(
                name="Risk-Free Rate",
                symbol=r"r",
                description="Annualized risk-free rate used to price the call",
                # Not live SOFR. The engine reads SOFR for its Sharpe ratios, but
                # never passes it to generate_covered_call_recommendation, whose
                # `r` therefore stays at DEFAULT_RISK_FREE_RATE.
                default_value="4.5% (RISK_FREE_RATE setting)",
                calibration_method="Fixed setting; the live NY Fed SOFR reading is not used in this calculation",
            ),
        ],
        data_inputs=["tradfi:equity_quote", "option_flow:large_print_iv", "quant_engine:ewma_volatility"],
        # Was "rejected if annualized yield < 6% or drawdown > 15%". The
        # backtester has neither limit, and its verdict does not reach this
        # recommendation. What runs is the rally gate.
        validation_gate=(
            "Offered only when the stock's z-score is at least +2.5 and the covered-calls switch is on. "
            "It is not required to pass a backtest."
        ),
        validation_gate_impl="shared.utils.quant_calc:generate_covered_call_recommendation",
        falsifiability_condition="Realized return underperforms passive buy-and-hold across 3 consecutive expiration cycles.",
    ),
    "granger_causality": SignalMethodology(
        id="granger_causality",
        name="Pairwise Bivariate Granger Causality",
        plain=(
            "Checks whether moves in one thing (say, oil) tend to come before moves in another (say, airline "
            "stocks), more often than chance would allow. It shows which tends to move first, not that one "
            "causes the other."
        ),
        category="Statistical Discovery",
        epistemic_tier="Statistical Empirical",
        description="Tests whether historical lagged returns of Series X improve the autoregressive forecast accuracy of Series Y beyond Y's own history.",
        formula_latex=r"Y_t = c + \sum_{i=1}^p \alpha_i Y_{t-i} + \sum_{j=1}^p \beta_j X_{t-j} + \epsilon_t, \quad F = \frac{(RSS_R - RSS_U)/p}{RSS_U / (T - 2p - 1)}",
        formula_plain="Y_t = c + Σ(α_i · Y_{t-i}) + Σ(β_j · X_{t-j}) + ε_t,  F = [(RSS_R - RSS_U)/p] / [RSS_U / (T - 2p - 1)]",
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
                default_value="0.05, unless recalibrated",
                calibration_method=(
                    "Read from the thresholds ThresholdCalibrationHarness stores; each sweep's tests are then "
                    "corrected together with Benjamini-Hochberg"
                ),
            ),
            MethodologyParameter(
                name="Lag Horizon",
                symbol=r"p",
                description="Lag order chosen by the Bayesian Information Criterion (BIC)",
                default_value="selected per pair, up to 3",
                # BIC, not AIC. The lag was chosen by the smallest p-value over
                # the grid until this audit replaced it -- selection over a
                # family reported as a single test, measured at 12.5% false
                # positives on white noise and 22.5% on random walks against a
                # nominal 5%. The replacement selects by BIC.
                calibration_method="Minimised BIC over the lag grid, on differenced series where an ADF test finds a unit root",
                is_learned=True,
            ),
        ],
        data_inputs=["tradfi_bars:continuous_aggregates", "graph_topology:candidate_pairs"],
        validation_gate="Candidate edges must pass a rolling 30-day Granger F-test, after false-discovery correction at the current cutoff, before ingestion as GRANGER_CAUSES edges.",
        validation_gate_impl="shared.utils.quant_calc:granger_causality",
        falsifiability_condition="Out-of-sample directional predictive accuracy drops below 50% across a 20-period test window.",
    ),
    "hawkes_point_process": SignalMethodology(
        id="hawkes_point_process",
        name="Hawkes Event Contagion",
        plain=(
            "Some events set off more events: a port closure, then a sell-off, then more alerts. This measures "
            "how strongly one burst of events makes the next more likely, and how fast that fades."
        ),
        category="Cross-Market Events",
        epistemic_tier="Stochastic Process",
        description="Quantifies self- and mutually-exciting jump clustering across maritime chokepoint events, geopolitical news, and market volatility.",
        formula_latex=r"\lambda_i(t) = \mu_i + \sum_{j=1}^M \sum_{t_k < t} \alpha_{ij} \cdot e^{-\beta(t - t_k)}",
        formula_plain="λ_i(t) = μ_i + Σ_j Σ_{t_k < t} α_{ij} · e^(-β · (t - t_k))",
        assumptions=[
            "Temporal point process with exponential memory decay kernels",
            "Spectral radius of branching matrix ||Alpha|| < 1 to guarantee subcritical stability",
        ],
        parameters=[
            MethodologyParameter(
                name="Decay Rate",
                symbol=r"\beta",
                description="How fast the effect of an event fades; one rate shared by every domain",
                # Was "Decay Half-Life, 3600 seconds". No such constant exists:
                # HawkesMLE fits beta along with the rest.
                default_value="fitted; no fixed default",
                calibration_method="L-BFGS-B maximum likelihood, refit on the last 168 hours of events",
                is_learned=True,
            ),
            MethodologyParameter(
                name="Branching Ratio",
                symbol=r"\eta",
                description="Average number of secondary daughter events triggered by an initial parent event",
                # Not 0.65, and not EM.
                #
                # Nothing in this codebase fits a Hawkes process by
                # expectation-maximisation -- the estimator is L-BFGS-B
                # maximum likelihood with analytic gradients, and the ratio is
                # estimated per ordered domain pair rather than held as a
                # constant. Publishing a method the code does not perform is
                # the same defect as the "5-year rolling backtests" claim
                # corrected on this endpoint earlier in this audit, and it
                # survived that pass.
                default_value="estimated per domain pair; no fixed default",
                calibration_method=(
                    "L-BFGS-B maximum likelihood on the multivariate exponential "
                    "Hawkes log-likelihood, with non-negativity bounds and a "
                    "stationarity projection on the branching matrix"
                ),
                is_learned=True,
            ),
        ],
        data_inputs=["events.enriched:all_domains", "redis:hawkes_tracker"],
        validation_gate="Subcriticality gate: spectral radius must strictly remain < 1.0; branches clamped if self-excitation diverges.",
        validation_gate_impl="services.correlation.hawkes_correlator:HawkesMLE._spectral_radius",
        falsifiability_condition="Observed inter-arrival event times conform to a homogeneous Poisson process (p > 0.10 via KS test).",
    ),
    "parametric_var_cvar": SignalMethodology(
        id="parametric_var_cvar",
        name="Parametric Value at Risk (VaR) & Expected Shortfall (CVaR)",
        plain=(
            'Answers "how much could this lose on a bad day?": the loss a portfolio should only exceed about one '
            "day in twenty, and the average loss on the very worst days (about one in a hundred)."
        ),
        category="Risk & Governance",
        epistemic_tier="Deterministic Closed-Form",
        description=(
            "Computes 1-Day 95% Parametric VaR and 99% Gaussian Expected Shortfall "
            "from the L2 norm of position weights and a measured daily volatility. "
            "Note that there is no covariance matrix in this platform: the implementation "
            "assumes zero correlation between positions, which is what sqrt(sum(w^2)) "
            "encodes, and which understates risk for a long equity book."
        ),
        formula_latex=r"\text{VaR}_{\alpha} = Z_{\alpha} \cdot \sigma_P \cdot V_P, \quad \text{CVaR}_{\alpha} = \frac{\phi(Z_{\alpha})}{1 - \alpha} \cdot \sigma_P \cdot V_P",
        formula_plain="VaR_α = Z_α · σ_P · V_P,  CVaR_α = [φ(Z_α) / (1 - α)] · σ_P · V_P",
        assumptions=[
            "Multivariate normal distribution of asset log returns over 1-day horizon",
            "Linear portfolio valuation with constant position weights over the holding period",
            "Zero correlation between positions, and one volatility shared by all of "
            "them: sigma_P is approximated as sigma_daily * sqrt(sum(w^2))",
            "sigma_daily is the platform's market-wide realised volatility "
            "(QQQ/SPY 1m bars, annualised, over sqrt(252)); when no measurement is "
            "available the response is marked DISCLOSED_PLACEHOLDER rather than computed",
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
        data_inputs=["broker:positions", "broker:account", "redis:sentinel:macro:realised_vol"],
        # `tradfi:historical_covariance` was listed as an input and does not
        # exist, and the gate named a Kupiec proportion-of-failures test that
        # nothing in this platform runs. A validation gate nobody executes is a
        # claim, and this field sits on the endpoint whose whole purpose is to
        # let a reader check the claims.
        validation_gate=(
            "None. A Kupiec proportion-of-failures backtest would require a "
            "recorded history of daily portfolio P&L against the VaR published "
            "on each prior day, which this platform does not retain. The figure "
            "is unvalidated and reported as such."
        ),
        falsifiability_condition="Realized daily portfolio losses breach 95% VaR threshold more than 4 times in a 60-day window.",
    ),
    "half_kelly_position_sizing": SignalMethodology(
        id="half_kelly_position_sizing",
        name="Half-Kelly Capital Sizing",
        plain=(
            "Decides how much to put into each trade idea. It uses a well-known formula for the best size, then "
            "deliberately bets half of that, because the formula assumes we know the odds exactly, and we never do."
        ),
        category="Quantitative Strategy",
        epistemic_tier="Deterministic Closed-Form",
        # "while reducing drawdown variance by half" was not a property of
        # half-Kelly, and the paper book is equal-weight: only the quant engine's
        # plays reach kelly_criterion.
        description=(
            "Calculates the fraction of portfolio equity for one of the quant engine's trade ideas: half the "
            "Kelly fraction, capped. The paper auto-trader's book is equal-weight and does not use it."
        ),
        formula_latex=r"K^* = \frac{p \cdot b - q}{b}, \quad K_{\text{alloc}} = \min\left(K_{\text{max}}, 0.5 \cdot K^*\right)",
        formula_plain="K* = (p · b - q) / b,  K_alloc = min(K_max, 0.5 · K*)",
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
                # 10%, which is what MAX_SINGLE_POSITION_PCT holds. This said
                # 15.0% -- a risk-governance hard rule published at one number
                # and enforced at another. A test now pins the two together.
                default_value="10.0%",
                calibration_method="Platform risk governance hard rule",
            ),
        ],
        data_inputs=["quant_engine:win_rate", "quant_engine:risk_reward_ratio", "broker:account_equity"],
        validation_gate="An idea whose Kelly fraction comes out at or below zero is sized at zero.",
        validation_gate_impl="shared.utils.quant_calc:kelly_criterion",
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
