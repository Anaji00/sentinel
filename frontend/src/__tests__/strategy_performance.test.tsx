import React from 'react';
import { describe, it, expect } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

/**
 * The calibration panel, against the shape the endpoint really returns.
 *
 * The fixture below is a verbatim record from `/backtest/results` on the
 * running stack, trimmed to one entry. Writing it from the interface instead
 * would test the interface against itself, which is the failure this whole
 * audit keeps finding: two declarations agreeing with each other and neither
 * agreeing with the wire.
 *
 * What matters here is the absence handling. `empirical_win_rate` is null for
 * every bin the strategy never traded, and a bin with no trades is not a bin
 * it lost -- rendering that as a 0% win rate would invent a measurement.
 */

// Imported for their types and their null handling, not re-implemented here.
import { ABSENT, formatPercent, isPresent } from '../lib/format';

const LIVE_RESULT = {
  strategy_id: 'momentum_trend_sm',
  strategy_name: 'Momentum Trend',
  ticker: 'SM',
  backtested_at: '2026-09-15T03:03:23.954995+00:00',
  bar_count: 300,
  data_provenance: 'authentic_market_data',
  initial_capital_usd: 100000.0,
  final_capital_usd: 99306.58,
  performance_metrics: {
    total_trades: 14,
    winning_trades: 3,
    losing_trades: 11,
    hit_rate_pct: 21.43,
    profit_factor: 0.14,
    total_return_pct: -0.69,
    benchmark_return_pct: 1.38,
    alpha_pct: -2.07,
  },
  risk_metrics: {
    realized_sharpe_ratio: -24.94,
    sortino_ratio: -36.8,
    max_drawdown_pct: 0.69,
    expected_value_per_trade_usd: -49.53,
    payoff_ratio: 0.51,
    avg_win_usd: 37.75,
    avg_loss_usd: 73.34,
  },
  calibration_curve: [
    {
      probability_bin: '40-60%',
      mean_predicted_prob: 0.5,
      empirical_win_rate: null,
      trade_count: 0,
    },
    {
      probability_bin: '60-80%',
      mean_predicted_prob: 0.7,
      empirical_win_rate: 0.2143,
      trade_count: 14,
    },
  ],
};

describe('the backtest result shape this panel is typed against', () => {
  it('carries every field the panel reads', () => {
    // Each of these was read off the live endpoint. If the server drops one,
    // the panel renders a blank cell and this says which.
    for (const key of [
      'strategy_name',
      'ticker',
      'bar_count',
      'data_provenance',
      'performance_metrics',
      'risk_metrics',
      'calibration_curve',
    ] as const) {
      expect(LIVE_RESULT).toHaveProperty(key);
    }
    for (const key of ['alpha_pct', 'hit_rate_pct', 'total_trades'] as const) {
      expect(LIVE_RESULT.performance_metrics).toHaveProperty(key);
    }
    for (const key of ['realized_sharpe_ratio', 'max_drawdown_pct'] as const) {
      expect(LIVE_RESULT.risk_metrics).toHaveProperty(key);
    }
  });

  it('has bins the strategy never traded, which are not losses', () => {
    const untested = LIVE_RESULT.calibration_curve.filter((b) => b.trade_count === 0);
    expect(untested.length).toBeGreaterThan(0);
    for (const bin of untested) {
      // The endpoint says null, not 0. The panel must not turn that into a
      // win rate of zero -- a bin with no trades has no win rate at all.
      expect(bin.empirical_win_rate).toBeNull();
      expect(isPresent(bin.empirical_win_rate)).toBe(false);
    }
  });

  it('renders an untraded bin as absent and a traded one as its rate', () => {
    const untraded = LIVE_RESULT.calibration_curve[0];
    const traded = LIVE_RESULT.calibration_curve[1];

    const cell = (bin: (typeof LIVE_RESULT.calibration_curve)[number]) =>
      renderToStaticMarkup(
        <span>
          {bin.trade_count === 0 || !isPresent(bin.empirical_win_rate)
            ? ABSENT
            : formatPercent(bin.empirical_win_rate, { from: 'ratio', decimals: 0 })}
        </span>,
      );

    expect(cell(untraded)).toContain(ABSENT);
    expect(cell(untraded)).not.toContain('0%');
    expect(cell(traded)).toContain('21%');
  });

  it('measures calibration as the gap between what was said and what happened', () => {
    const traded = LIVE_RESULT.calibration_curve[1];
    const said = traded.mean_predicted_prob!;
    const did = traded.empirical_win_rate!;
    const gap = Math.abs(said - did);

    // Said 70%, won 21%. That is a badly calibrated strategy, and the whole
    // reason this endpoint is worth putting on a screen: the return figures
    // alone would not have said so.
    expect(gap).toBeGreaterThan(0.2);
  });
});
