"use client";

import React, { useState } from "react";

interface Parameter {
  name: string;
  symbol: string;
  description: string;
  default_value: string;
  calibration_method: string;
}

interface SignalMethodology {
  id: string;
  name: string;
  category: string;
  epistemic_tier: string;
  description: string;
  formula_latex: string;
  assumptions: string[];
  parameters: Parameter[];
  data_inputs: string[];
  validation_gate: string;
  falsifiability_condition: string;
}

const STATIC_METHODOLOGIES: Record<string, SignalMethodology> = {
  covered_call_optimization: {
    id: "covered_call_optimization",
    name: "Covered Call Overlay Optimization",
    category: "Derivatives & Options",
    epistemic_tier: "Deterministic Closed-Form",
    description:
      "Selects optimal out-of-the-money (OTM) strike prices that maximize annualized option yield while bounding upside assignment probability.",
    formula_latex:
      "C(S, K, T) = S · N(d1) - K · e^(-rT) · N(d2),  where d1 = [ln(S/K) + (r + σ²/2)T] / (σ√T),  d2 = d1 - σ√T",
    assumptions: [
      "Geometric Brownian Motion for underlying asset price",
      "Frictionless market with continuous hedging",
      "Implied volatility solved via Brent-Dekker root finding on market bid/ask midpoints",
    ],
    parameters: [
      {
        name: "Delta Target Window",
        symbol: "Δ",
        description: "Target delta range for short call strike selection",
        default_value: "[0.20, 0.35]",
        calibration_method: "Empirically calibrated to maximize risk-adjusted Sharpe over 5-year rolling backtests",
      },
      {
        name: "Risk-Free Rate",
        symbol: "r",
        description: "Annualized SOFR benchmark risk-free rate",
        default_value: "Live NY Fed SOFR",
        calibration_method: "Queried dynamically from nyfed_sofr feed in Redis",
      },
    ],
    data_inputs: ["tradfi:equity_quote", "alpaca_options:chain", "nyfed_sofr:rate"],
    validation_gate:
      "Backtested over historical CAGG bars; signal rejected if annualized yield < 6% or drawdown > 15%",
    falsifiability_condition:
      "Realized return underperforms passive buy-and-hold across 3 consecutive expiration cycles.",
  },
  granger_causality: {
    id: "granger_causality",
    name: "Pairwise Bivariate Granger Causality",
    category: "Statistical Discovery",
    epistemic_tier: "Statistical Empirical",
    description:
      "Tests whether historical lagged returns of Series X improve the autoregressive forecast accuracy of Series Y beyond Y's own history.",
    formula_latex:
      "Y_t = c + Σ(α_i · Y_{t-i}) + Σ(β_j · X_{t-j}) + ε_t,  F = [(RSS_R - RSS_U)/p] / [RSS_U / (T - 2p - 1)]",
    assumptions: [
      "Stationary time series (verified via Augmented Dickey-Fuller test)",
      "Linear lag dependencies up to maximum lag order p",
      "Independent and identically distributed Gaussian error residuals",
    ],
    parameters: [
      {
        name: "P-Value Cutoff",
        symbol: "α",
        description: "Maximum acceptable p-value under null hypothesis H0",
        default_value: "0.05",
        calibration_method: "ThresholdCalibrationHarness rolling 95th percentile",
      },
      {
        name: "Lag Horizon",
        symbol: "p",
        description: "Optimal lag order determined by Akaike Information Criterion (AIC)",
        default_value: "3 periods",
        calibration_method: "Minimized AIC over lag grid [1, 10]",
      },
    ],
    data_inputs: ["tradfi_bars:continuous_aggregates", "graph_topology:candidate_pairs"],
    validation_gate:
      "Candidate edges must pass rolling 30-day Granger F-test with p < 0.05 before ingestion as GRANGER_CAUSES edges.",
    falsifiability_condition:
      "Out-of-sample directional predictive accuracy drops below 50% across a 20-period test window.",
  },
  hawkes_point_process: {
    id: "hawkes_point_process",
    name: "Mutually Exciting Hawkes Cross-Domain Contagion",
    category: "Cross-Domain Epistemic",
    epistemic_tier: "Stochastic Process",
    description:
      "Quantifies self- and mutually-exciting jump clustering across maritime chokepoint events, geopolitical news, cyber disclosures, and market volatility.",
    formula_latex:
      "λ_i(t) = μ_i + Σ_j Σ_{t_k < t} α_{ij} · e^(-β_{ij} · (t - t_k))",
    assumptions: [
      "Temporal point process with exponential memory decay kernels",
      "Spectral radius of branching matrix ||Alpha|| < 1 to guarantee subcritical stability",
    ],
    parameters: [
      {
        name: "Decay Half-Life",
        symbol: "t_{1/2}",
        description: "Temporal half-life of cross-domain excitation memory",
        default_value: "3600 seconds (1 hour)",
        calibration_method: "Maximum likelihood estimation on empirical multi-domain event bursts",
      },
      {
        name: "Branching Ratio",
        symbol: "η",
        description: "Average number of secondary daughter events triggered by an initial parent event",
        default_value: "0.65",
        calibration_method: "Fitted via EM algorithm on historical event logs",
      },
    ],
    data_inputs: ["events.enriched:all_domains", "redis:hawkes_tracker"],
    validation_gate:
      "Subcriticality gate: spectral radius must strictly remain < 1.0; branches clamped if self-excitation diverges.",
    falsifiability_condition:
      "Observed inter-arrival event times conform to a homogeneous Poisson process (p > 0.10 via KS test).",
  },
  parametric_var_cvar: {
    id: "parametric_var_cvar",
    name: "Parametric Value at Risk (VaR) & Expected Shortfall (CVaR)",
    category: "Risk & Governance",
    epistemic_tier: "Deterministic Closed-Form",
    description:
      "Computes 1-Day 95% Parametric VaR and 99% Conditional VaR using an empirical covariance matrix and Herfindahl concentration index.",
    formula_latex:
      "VaR_α = Z_α · σ_P · V_P,  CVaR_α = [φ(Z_α) / (1 - α)] · σ_P · V_P",
    assumptions: [
      "Multivariate normal distribution of asset log returns over 1-day horizon",
      "Linear portfolio valuation with constant position weights over the holding period",
    ],
    parameters: [
      {
        name: "VaR Confidence Level",
        symbol: "α",
        description: "1-tail confidence percentile",
        default_value: "95% (Z = 1.6449)",
        calibration_method: "Basel Committee market risk standard",
      },
      {
        name: "CVaR Confidence Level",
        symbol: "β",
        description: "Conditional tail expectation percentile",
        default_value: "99% (Z = 2.3263)",
        calibration_method: "FRTB (Fundamental Review of the Trading Book) standard",
      },
    ],
    data_inputs: ["broker:positions", "broker:account", "tradfi:historical_covariance"],
    validation_gate:
      "Kupiec proportion-of-failures (POF) test: exceptions must match theoretical 5% frequency within binomial confidence bands.",
    falsifiability_condition:
      "Realized daily portfolio losses breach 95% VaR threshold more than 4 times in a 60-day window.",
  },
  half_kelly_position_sizing: {
    id: "half_kelly_position_sizing",
    name: "Half-Kelly Optimal Capital Sizing",
    category: "Quantitative Strategy",
    epistemic_tier: "Deterministic Closed-Form",
    description:
      "Calculates fraction of portfolio equity to allocate to high-conviction trade setups while reducing drawdown variance by half.",
    formula_latex:
      "K* = (p · b - q) / b,  K_alloc = min(K_max, 0.5 · K*)",
    assumptions: [
      "Logarithmic utility maximization (growth-optimal)",
      "Known win probability p and risk/reward payoff ratio b",
    ],
    parameters: [
      {
        name: "Kelly Damping Factor",
        symbol: "γ",
        description: "Fraction of full Kelly criterion implemented to prevent over-betting",
        default_value: "0.50 (Half-Kelly)",
        calibration_method: "Standard quantitative practice for fat-tailed asset distributions",
      },
      {
        name: "Hard Position Clamp",
        symbol: "K_max",
        description: "Maximum allowable position size as a percentage of total equity",
        default_value: "15.0%",
        calibration_method: "Platform risk governance hard rule",
      },
    ],
    data_inputs: ["quant_engine:win_rate", "quant_engine:risk_reward_ratio", "broker:account_equity"],
    validation_gate:
      "Allocation rejected if calculated Kelly fraction <= 0 or expected value EV <= 0.",
    falsifiability_condition:
      "Realized strategy win rate drops below break-even threshold 1/(1+b).",
  },
};

export default function MethodologyPage() {
  const [selectedId, setSelectedId] = useState<string>("covered_call_optimization");
  const current = STATIC_METHODOLOGIES[selectedId] || STATIC_METHODOLOGIES["covered_call_optimization"];

  return (
    <div className="flex flex-col gap-6 p-6 max-w-7xl mx-auto text-slate-100">
      {/* Header */}
      <div className="flex flex-col gap-1 border-b border-slate-800 pb-5">
        <div className="flex items-center gap-2.5">
          <span className="text-2xl">📐</span>
          <h1 className="text-2xl font-bold tracking-tight text-white">Mathematical Methodology & Open Whitepaper</h1>
          <span className="px-2.5 py-0.5 text-xs font-bold rounded-full bg-sky-500/20 text-sky-400 border border-sky-500/30 ml-2">
            Anti-Palantir Integrity Standard
          </span>
        </div>
        <p className="text-sm text-slate-400 mt-1">
          Zero black boxes. Every mathematical equation, statistical derivation, parameter calibration method, and falsifiability condition is fully published and verifiable.
        </p>
      </div>

      {/* Main Layout */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {/* Sidebar Selector */}
        <div className="flex flex-col gap-2">
          <span className="text-xs font-bold text-slate-400 uppercase tracking-wider px-1">Signal Catalog</span>
          {Object.values(STATIC_METHODOLOGIES).map((m) => {
            const isSelected = m.id === selectedId;
            return (
              <button
                key={m.id}
                onClick={() => setSelectedId(m.id)}
                className={`flex flex-col items-start p-3 rounded-lg border text-left transition-all ${
                  isSelected
                    ? "bg-sky-600/20 border-sky-500 text-sky-200 shadow-md shadow-sky-500/10"
                    : "bg-slate-900/60 border-slate-800 text-slate-400 hover:border-slate-700 hover:text-slate-200"
                }`}
              >
                <span className="text-xs font-bold text-white">{m.name}</span>
                <span className="text-[11px] text-slate-400 mt-0.5">{m.category}</span>
              </button>
            );
          })}
        </div>

        {/* Detailed Methodology View */}
        <div className="md:col-span-3 flex flex-col gap-5 bg-slate-900/80 border border-slate-800 rounded-xl p-6 shadow-xl">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 pb-4">
            <div>
              <span className="px-2 py-0.5 text-[11px] font-semibold rounded bg-purple-500/20 text-purple-300 border border-purple-500/40 uppercase tracking-wide">
                {current.epistemic_tier}
              </span>
              <h2 className="text-xl font-bold text-white mt-1.5">{current.name}</h2>
              <p className="text-xs text-slate-400 mt-1">{current.description}</p>
            </div>
          </div>

          {/* Mathematical Formula Card */}
          <div className="flex flex-col gap-2 p-4 bg-slate-950/90 border border-slate-800 rounded-lg">
            <span className="text-xs font-semibold text-sky-400 flex items-center gap-1.5">
              <span>∑</span>
              <span>Mathematical Formulation</span>
            </span>
            <div className="font-mono text-sm text-emerald-400 bg-slate-900/90 p-3 rounded border border-slate-800/80 overflow-x-auto">
              {current.formula_latex}
            </div>
          </div>

          {/* Theoretical Assumptions */}
          <div className="flex flex-col gap-2">
            <h3 className="text-xs font-bold uppercase tracking-wider text-slate-400">Underlying Epistemic Assumptions</h3>
            <ul className="list-disc list-inside text-xs text-slate-300 space-y-1 bg-slate-950/40 p-3 rounded-lg border border-slate-800/60">
              {current.assumptions.map((assump, i) => (
                <li key={i}>{assump}</li>
              ))}
            </ul>
          </div>

          {/* Parameter Calibration Grid */}
          <div className="flex flex-col gap-2">
            <h3 className="text-xs font-bold uppercase tracking-wider text-slate-400">Parameter Calibration & Empirical Tuning</h3>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs border-collapse border border-slate-800">
                <thead>
                  <tr className="bg-slate-950 text-slate-400 border-b border-slate-800">
                    <th className="p-2.5">Parameter</th>
                    <th className="p-2.5">Symbol</th>
                    <th className="p-2.5">Default Value</th>
                    <th className="p-2.5">Calibration Method</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {current.parameters.map((p, i) => (
                    <tr key={i} className="hover:bg-slate-800/30">
                      <td className="p-2.5 font-semibold text-white">{p.name}</td>
                      <td className="p-2.5 font-mono text-sky-400">{p.symbol}</td>
                      <td className="p-2.5 font-mono text-emerald-400">{p.default_value}</td>
                      <td className="p-2.5 text-slate-300">{p.calibration_method}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Validation Gate & Falsifiability */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
            <div className="p-3.5 bg-emerald-950/20 border border-emerald-500/30 rounded-lg">
              <span className="text-xs font-bold text-emerald-400 flex items-center gap-1">
                <span>🛡️</span>
                <span>Pre-Deployment Validation Gate</span>
              </span>
              <p className="text-xs text-emerald-200/90 mt-1">{current.validation_gate}</p>
            </div>
            <div className="p-3.5 bg-rose-950/20 border border-rose-500/30 rounded-lg">
              <span className="text-xs font-bold text-rose-400 flex items-center gap-1">
                <span>⚠️</span>
                <span>Falsifiability / Model Degradation Condition</span>
              </span>
              <p className="text-xs text-rose-200/90 mt-1">{current.falsifiability_condition}</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
