'use client';

import React, { useState } from 'react';
import { PageHeader } from '@/components/ui/PageHeader';
import { IconAlert, IconShield } from '@/components/ui/icons';

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
  /** What it does, for someone who has never met the maths. */
  plain: string;
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
    id: 'covered_call_optimization',
    plain:
      'If you own a stock, you can sell someone the right to buy it from you at a higher price and keep the fee. This picks the price and date that pay the most fee for the least chance of having to sell.',
    name: 'Covered Call Overlay Optimization',
    category: 'Derivatives & Options',
    epistemic_tier: 'Deterministic Closed-Form',
    description:
      'Selects optimal out-of-the-money (OTM) strike prices that maximize annualized option yield while bounding upside assignment probability.',
    formula_latex:
      'C(S, K, T) = S · N(d1) - K · e^(-rT) · N(d2),  where d1 = [ln(S/K) + (r + σ²/2)T] / (σ√T),  d2 = d1 - σ√T',
    assumptions: [
      'Geometric Brownian Motion for underlying asset price',
      'Frictionless market with continuous hedging',
      'Implied volatility solved via Brent-Dekker root finding on market bid/ask midpoints',
    ],
    parameters: [
      {
        name: 'Delta Target Window',
        symbol: 'Δ',
        description: 'Target delta range for short call strike selection',
        default_value: '[0.20, 0.35]',
        calibration_method:
          'Empirically calibrated to maximize risk-adjusted Sharpe over 5-year rolling backtests',
      },
      {
        name: 'Risk-Free Rate',
        symbol: 'r',
        description: 'Annualized SOFR benchmark risk-free rate',
        default_value: 'Live NY Fed SOFR',
        calibration_method: 'Queried dynamically from nyfed_sofr feed in Redis',
      },
    ],
    data_inputs: ['tradfi:equity_quote', 'alpaca_options:chain', 'nyfed_sofr:rate'],
    validation_gate:
      'Backtested over historical CAGG bars; signal rejected if annualized yield < 6% or drawdown > 15%',
    falsifiability_condition:
      'Realized return underperforms passive buy-and-hold across 3 consecutive expiration cycles.',
  },
  granger_causality: {
    id: 'granger_causality',
    plain:
      'Checks whether moves in one thing (say, oil) tend to come before moves in another (say, airline stocks), more often than chance would allow. It shows which way news travels, not that one causes the other.',
    name: 'Pairwise Bivariate Granger Causality',
    category: 'Statistical Discovery',
    epistemic_tier: 'Statistical Empirical',
    description:
      "Tests whether historical lagged returns of Series X improve the autoregressive forecast accuracy of Series Y beyond Y's own history.",
    formula_latex:
      'Y_t = c + Σ(α_i · Y_{t-i}) + Σ(β_j · X_{t-j}) + ε_t,  F = [(RSS_R - RSS_U)/p] / [RSS_U / (T - 2p - 1)]',
    assumptions: [
      'Stationary time series (verified via Augmented Dickey-Fuller test)',
      'Linear lag dependencies up to maximum lag order p',
      'Independent and identically distributed Gaussian error residuals',
    ],
    parameters: [
      {
        name: 'P-Value Cutoff',
        symbol: 'α',
        description: 'Maximum acceptable p-value under null hypothesis H0',
        default_value: '0.05',
        calibration_method: 'ThresholdCalibrationHarness rolling 95th percentile',
      },
      {
        name: 'Lag Horizon',
        symbol: 'p',
        description: 'Optimal lag order determined by Akaike Information Criterion (AIC)',
        default_value: '3 periods',
        calibration_method: 'Minimized AIC over lag grid [1, 10]',
      },
    ],
    data_inputs: ['tradfi_bars:continuous_aggregates', 'graph_topology:candidate_pairs'],
    validation_gate:
      'Candidate edges must pass rolling 30-day Granger F-test with p < 0.05 before ingestion as GRANGER_CAUSES edges.',
    falsifiability_condition:
      'Out-of-sample directional predictive accuracy drops below 50% across a 20-period test window.',
  },
  hawkes_point_process: {
    id: 'hawkes_point_process',
    plain:
      'Some events set off more events: a port closure, then a sell-off, then more alerts. This measures how strongly one burst of events makes the next more likely, and how fast that fades.',
    name: 'Mutually Exciting Hawkes Cross-Domain Contagion',
    category: 'Cross-Domain Epistemic',
    epistemic_tier: 'Stochastic Process',
    description:
      'Quantifies self- and mutually-exciting jump clustering across maritime chokepoint events, geopolitical news, and market volatility.',
    formula_latex: 'λ_i(t) = μ_i + Σ_j Σ_{t_k < t} α_{ij} · e^(-β_{ij} · (t - t_k))',
    assumptions: [
      'Temporal point process with exponential memory decay kernels',
      'Spectral radius of branching matrix ||Alpha|| < 1 to guarantee subcritical stability',
    ],
    parameters: [
      {
        name: 'Decay Half-Life',
        symbol: 't_{1/2}',
        description: 'Temporal half-life of cross-domain excitation memory',
        default_value: '3600 seconds (1 hour)',
        calibration_method: 'Maximum likelihood estimation on empirical multi-domain event bursts',
      },
      {
        name: 'Branching Ratio',
        symbol: 'η',
        description:
          'Average number of secondary daughter events triggered by an initial parent event',
        default_value: '0.65',
        calibration_method: 'Fitted via EM algorithm on historical event logs',
      },
    ],
    data_inputs: ['events.enriched:all_domains', 'redis:hawkes_tracker'],
    validation_gate:
      'Subcriticality gate: spectral radius must strictly remain < 1.0; branches clamped if self-excitation diverges.',
    falsifiability_condition:
      'Observed inter-arrival event times conform to a homogeneous Poisson process (p > 0.10 via KS test).',
  },
  parametric_var_cvar: {
    id: 'parametric_var_cvar',
    plain:
      'Answers "how much could this lose on a bad day?": the loss a portfolio should only exceed about one day in twenty, and the average loss on the very worst days (about one in a hundred).',
    name: 'Parametric Value at Risk (VaR) & Expected Shortfall (CVaR)',
    category: 'Risk & Governance',
    epistemic_tier: 'Deterministic Closed-Form',
    description:
      'Computes 1-Day 95% Parametric VaR and 99% Conditional VaR using an empirical covariance matrix and Herfindahl concentration index.',
    formula_latex: 'VaR_α = Z_α · σ_P · V_P,  CVaR_α = [φ(Z_α) / (1 - α)] · σ_P · V_P',
    assumptions: [
      'Multivariate normal distribution of asset log returns over 1-day horizon',
      'Linear portfolio valuation with constant position weights over the holding period',
    ],
    parameters: [
      {
        name: 'VaR Confidence Level',
        symbol: 'α',
        description: '1-tail confidence percentile',
        default_value: '95% (Z = 1.6449)',
        calibration_method: 'Basel Committee market risk standard',
      },
      {
        name: 'CVaR Confidence Level',
        symbol: 'β',
        description: 'Conditional tail expectation percentile',
        default_value: '99% (Z = 2.3263)',
        calibration_method: 'FRTB (Fundamental Review of the Trading Book) standard',
      },
    ],
    data_inputs: ['broker:positions', 'broker:account', 'tradfi:historical_covariance'],
    validation_gate:
      'Kupiec proportion-of-failures (POF) test: exceptions must match theoretical 5% frequency within binomial confidence bands.',
    falsifiability_condition:
      'Realized daily portfolio losses breach 95% VaR threshold more than 4 times in a 60-day window.',
  },
  half_kelly_position_sizing: {
    id: 'half_kelly_position_sizing',
    plain:
      'Decides how much to put into each bet. It uses a well-known formula for the best size, then deliberately bets half of that, because the formula assumes we know the odds exactly, and we never do.',
    name: 'Half-Kelly Optimal Capital Sizing',
    category: 'Quantitative Strategy',
    epistemic_tier: 'Deterministic Closed-Form',
    description:
      'Calculates fraction of portfolio equity to allocate to high-conviction trade setups while reducing drawdown variance by half.',
    formula_latex: 'K* = (p · b - q) / b,  K_alloc = min(K_max, 0.5 · K*)',
    assumptions: [
      'Logarithmic utility maximization (growth-optimal)',
      'Known win probability p and risk/reward payoff ratio b',
    ],
    parameters: [
      {
        name: 'Kelly Damping Factor',
        symbol: 'γ',
        description: 'Fraction of full Kelly criterion implemented to prevent over-betting',
        default_value: '0.50 (Half-Kelly)',
        calibration_method: 'Standard quantitative practice for fat-tailed asset distributions',
      },
      {
        name: 'Hard Position Clamp',
        symbol: 'K_max',
        description: 'Maximum allowable position size as a percentage of total equity',
        default_value: '15.0%',
        calibration_method: 'Platform risk governance hard rule',
      },
    ],
    data_inputs: [
      'quant_engine:win_rate',
      'quant_engine:risk_reward_ratio',
      'broker:account_equity',
    ],
    validation_gate:
      'Allocation rejected if calculated Kelly fraction <= 0 or expected value EV <= 0.',
    falsifiability_condition:
      'Realized strategy win rate drops below break-even threshold 1/(1+b).',
  },
};

export default function MethodologyPage() {
  const [selectedId, setSelectedId] = useState<string>('covered_call_optimization');
  const current =
    STATIC_METHODOLOGIES[selectedId] || STATIC_METHODOLOGIES['covered_call_optimization'];

  return (
    <div className="flex flex-col gap-6 p-2 max-w-7xl mx-auto text-ink">
      {/* Header */}
      <PageHeader
        title="How it works"
        description="No black boxes: every formula behind the platform, the assumptions it makes, how its settings were chosen, and what result would prove it wrong."
        related={[
          { href: '/performance#years', label: 'What drives signals' },
          { href: '/calls?view=signals', label: 'Trade signals' },
          { href: '/performance#years', label: 'Backtests' },
        ]}
      />

      {/* Main Layout */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {/* Sidebar Selector */}
        <div className="flex flex-col gap-2">
          <span className="text-xs font-bold text-ink-dim uppercase tracking-wider px-1">
            The methods
          </span>
          {Object.values(STATIC_METHODOLOGIES).map((m) => {
            const isSelected = m.id === selectedId;
            return (
              <button
                key={m.id}
                onClick={() => setSelectedId(m.id)}
                className={`flex flex-col items-start p-3 rounded-lg border text-left transition-all ${
                  isSelected
                    ? 'bg-accent/20 border-accent text-accent shadow-md shadow-accent/10'
                    : 'bg-raised/60 border-line text-ink-dim hover:border-line-strong hover:text-ink'
                }`}
              >
                <span className="text-xs font-bold text-ink">{m.name}</span>
                <span className="text-micro text-ink-dim mt-0.5">{m.category}</span>
              </button>
            );
          })}
        </div>

        {/* Detailed Methodology View */}
        <div className="md:col-span-3 flex flex-col gap-5 bg-raised/80 border border-line rounded-xl p-6">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-line pb-4">
            <div>
              <span className="px-2 py-0.5 text-micro font-semibold rounded bg-accent-dim text-accent border border-line-accent uppercase tracking-wide">
                {current.epistemic_tier}
              </span>
              <h2 className="text-xl font-bold text-ink mt-1.5">{current.name}</h2>
              <p className="text-xs text-ink-dim mt-1">{current.description}</p>
            </div>
          </div>

          {/* Plain words first: most readers stop here, and should be able to. */}
          <div className="rounded-lg border border-accent/30 bg-accent-dim px-4 py-3">
            <div className="text-micro font-semibold uppercase tracking-wider text-accent">
              In plain words
            </div>
            <p className="mt-1 text-sm text-ink">{current.plain}</p>
          </div>

          {/* Mathematical Formula Card */}
          <div className="flex flex-col gap-2 p-4 bg-page/90 border border-line rounded-lg">
            <span className="text-xs font-semibold text-accent flex items-center gap-1.5">
              <span>∑</span>
              <span>The formula (for specialists)</span>
            </span>
            <div className="text-sm text-positive bg-raised/90 p-3 rounded border border-line/80 overflow-x-auto">
              {current.formula_latex}
            </div>
          </div>

          {/* Theoretical Assumptions */}
          <div className="flex flex-col gap-2">
            <h3 className="text-xs font-bold uppercase tracking-wider text-ink-dim">
              What it assumes
            </h3>
            <ul className="list-disc list-inside text-xs text-ink-dim space-y-1 bg-page/40 p-3 rounded-lg border border-line/60">
              {current.assumptions.map((assump, i) => (
                <li key={i}>{assump}</li>
              ))}
            </ul>
          </div>

          {/* Parameter Calibration Grid */}
          <div className="flex flex-col gap-2">
            <h3 className="text-xs font-bold uppercase tracking-wider text-ink-dim">
              Its settings, and how each was chosen
            </h3>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs border-collapse border border-line">
                <thead>
                  <tr className="bg-page text-ink-dim border-b border-line">
                    <th className="p-2.5">Parameter</th>
                    <th className="p-2.5">Symbol</th>
                    <th className="p-2.5">Default Value</th>
                    <th className="p-2.5">Calibration Method</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-line">
                  {current.parameters.map((p, i) => (
                    <tr key={i} className="hover:bg-overlay/30">
                      <td className="p-2.5 font-semibold text-ink">{p.name}</td>
                      <td className="p-2.5 font-mono text-accent">{p.symbol}</td>
                      <td className="p-2.5 font-mono text-positive">{p.default_value}</td>
                      <td className="p-2.5 text-ink-dim">{p.calibration_method}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Validation Gate & Falsifiability */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
            <div className="p-3.5 bg-positive/10 border border-positive/30 rounded-lg">
              <span className="text-xs font-bold text-positive flex items-center gap-1">
                <span>
                  <IconShield className="inline-block shrink-0" />
                </span>
                <span>What it must pass before we use it</span>
              </span>
              <p className="text-xs text-positive/90 mt-1">{current.validation_gate}</p>
            </div>
            <div className="p-3.5 bg-negative/10 border border-negative/30 rounded-lg">
              <span className="text-xs font-bold text-negative flex items-center gap-1">
                <span>
                  <IconAlert className="inline-block shrink-0" />
                </span>
                <span>What would tell us it has stopped working</span>
              </span>
              <p className="text-xs text-negative/90 mt-1">{current.falsifiability_condition}</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
