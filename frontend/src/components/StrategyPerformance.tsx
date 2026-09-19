'use client';

/**
 * What the strategies actually did, which nothing in the app has ever shown.
 *
 * `/backtest/results` holds 228 completed backtests -- every one carrying a
 * trade count, a hit rate, a realised Sharpe, a drawdown and a calibration
 * curve -- and no component in the frontend called it. The backtester has been
 * running, writing and serving results into a void.
 *
 * The calibration curve is the part worth putting on a screen. Every other
 * figure says how a strategy did; calibration says whether the strategy knew
 * what it was doing. A model that says "60% confident" and wins 60% of those
 * trades is calibrated. One that says 60% and wins 30% is not, and its returns
 * -- good or bad -- were luck. That distinction is the whole argument of this
 * codebase's audit, and it was sitting unread in a table.
 *
 * Bins with no trades render as absent rather than as a zero win rate. A bin
 * the strategy never entered is not a bin it lost.
 */

import React from 'react';
import useSWR from 'swr';
import { describeApiError, fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { EmptyState } from './ui/EmptyState';
import { ABSENT, formatNumber, formatPercent, isPresent } from '../lib/format';
import { IconAlert, IconCheck, IconDown, IconUp } from './ui/icons';
import { ExportButton } from './ui/ExportButton';
import type { CsvColumn } from '../lib/csv';
import { POLL } from './ui/DataProvider';
import { RunBacktest } from './RunBacktest';

interface PerformanceMetrics {
  total_trades: number;
  winning_trades: number;
  losing_trades: number;
  hit_rate_pct: number | null;
  profit_factor: number | null;
  total_return_pct: number | null;
  benchmark_return_pct: number | null;
  alpha_pct: number | null;
}

interface RiskMetrics {
  realized_sharpe_ratio: number | null;
  sortino_ratio: number | null;
  max_drawdown_pct: number | null;
  expected_value_per_trade_usd: number | null;
  payoff_ratio: number | null;
  avg_win_usd: number | null;
  avg_loss_usd: number | null;
}

interface CalibrationBin {
  probability_bin: string;
  mean_predicted_prob: number | null;
  /** Null when the strategy never took a trade in this confidence band. */
  empirical_win_rate: number | null;
  trade_count: number;
}

interface BacktestResult {
  strategy_id: string;
  strategy_name: string;
  ticker: string;
  backtested_at: string;
  bar_count: number;
  data_provenance: string;
  initial_capital_usd: number;
  final_capital_usd: number;
  performance_metrics: PerformanceMetrics;
  risk_metrics: RiskMetrics;
  calibration_curve?: CalibrationBin[];
}

/**
 * What leaves in the file.
 *
 * Every accessor returns the raw value or null -- no formatting, no zero
 * substitution. A strategy with no measured Sharpe exports an empty cell,
 * because a spreadsheet summing a column of "-24.94, 0, 0, 0" would be
 * averaging four strategies where only one was measured.
 *
 * The calibration curve is deliberately not flattened into columns here: it is
 * a variable-length series per row, and squashing it into `bin_1`..`bin_5`
 * would invent a fixed shape the endpoint does not promise.
 */
const CSV_COLUMNS: CsvColumn<BacktestResult>[] = [
  { label: 'strategy', value: (r) => r.strategy_name },
  { label: 'strategy_id', value: (r) => r.strategy_id },
  { label: 'ticker', value: (r) => r.ticker },
  { label: 'backtested_at', value: (r) => r.backtested_at },
  { label: 'bars_tested', value: (r) => r.bar_count },
  { label: 'data_provenance', value: (r) => r.data_provenance },
  { label: 'trades', value: (r) => r.performance_metrics.total_trades },
  { label: 'won', value: (r) => r.performance_metrics.winning_trades },
  { label: 'lost', value: (r) => r.performance_metrics.losing_trades },
  { label: 'hit_rate_pct', value: (r) => r.performance_metrics.hit_rate_pct },
  { label: 'profit_factor', value: (r) => r.performance_metrics.profit_factor },
  { label: 'total_return_pct', value: (r) => r.performance_metrics.total_return_pct },
  { label: 'benchmark_return_pct', value: (r) => r.performance_metrics.benchmark_return_pct },
  { label: 'alpha_pct', value: (r) => r.performance_metrics.alpha_pct },
  { label: 'realized_sharpe', value: (r) => r.risk_metrics.realized_sharpe_ratio },
  { label: 'sortino', value: (r) => r.risk_metrics.sortino_ratio },
  { label: 'max_drawdown_pct', value: (r) => r.risk_metrics.max_drawdown_pct },
  { label: 'payoff_ratio', value: (r) => r.risk_metrics.payoff_ratio },
  {
    label: 'expected_value_per_trade_usd',
    value: (r) => r.risk_metrics.expected_value_per_trade_usd,
  },
];

type SortKey = 'alpha' | 'sharpe' | 'trades' | 'drawdown';

const SORTS: { key: SortKey; label: string; of: (r: BacktestResult) => number | null }[] = [
  { key: 'alpha', label: 'Alpha', of: (r) => r.performance_metrics.alpha_pct },
  { key: 'sharpe', label: 'Sharpe', of: (r) => r.risk_metrics.realized_sharpe_ratio },
  { key: 'trades', label: 'Trades', of: (r) => r.performance_metrics.total_trades },
  { key: 'drawdown', label: 'Drawdown', of: (r) => r.risk_metrics.max_drawdown_pct },
];

/** A signed figure with the arrow and colour that match its sign. */
function Signed({ value, suffix = '%' }: { value: number | null | undefined; suffix?: string }) {
  if (!isPresent(value)) return <span className="text-ink-mute">{ABSENT}</span>;
  const tone = value > 0 ? 'text-positive' : value < 0 ? 'text-negative' : 'text-ink-mute';
  return (
    <span className={`inline-flex items-center gap-1 font-mono ${tone}`}>
      {value > 0 ? <IconUp /> : value < 0 ? <IconDown /> : null}
      {formatNumber(value, { decimals: 2 })}
      {suffix}
    </span>
  );
}

/**
 * How far a strategy's stated confidence was from what happened.
 *
 * Drawn as a bar per bin rather than a scatter against the diagonal: at five
 * bins a chart is harder to read than the numbers it encodes, and the figure
 * that matters is one subtraction -- said 60, did 30.
 */
function Calibration({ bins }: { bins: CalibrationBin[] }) {
  const measured = bins.filter((b) => b.trade_count > 0 && isPresent(b.empirical_win_rate));
  if (measured.length === 0) {
    return (
      <p className="text-micro text-ink-mute">
        No bin has a trade in it, so nothing here is calibrated or miscalibrated — it is untested.
      </p>
    );
  }
  return (
    <div className="space-y-1.5">
      {bins.map((bin) => {
        const said = bin.mean_predicted_prob;
        const did = bin.empirical_win_rate;
        const untested = bin.trade_count === 0 || !isPresent(did);
        const gap = !untested && isPresent(said) ? Math.abs(said - did!) : null;
        return (
          <div key={bin.probability_bin} className="flex items-center gap-2 text-micro">
            <span className="w-16 shrink-0 font-mono text-ink-dim">{bin.probability_bin}</span>
            <div className="relative h-2 flex-1 overflow-hidden rounded bg-inset">
              {!untested && (
                <>
                  <div
                    className="absolute inset-y-0 left-0 bg-accent/30"
                    style={{ width: `${Math.min(100, (said ?? 0) * 100)}%` }}
                  />
                  <div
                    className="absolute inset-y-0 left-0 border-r-2 border-positive"
                    style={{ width: `${Math.min(100, did! * 100)}%` }}
                  />
                </>
              )}
            </div>
            <span className="w-28 shrink-0 text-right font-mono">
              {untested ? (
                <span className="text-ink-mute">{ABSENT} no trades</span>
              ) : (
                <span className={gap !== null && gap > 0.2 ? 'text-caution' : 'text-ink-dim'}>
                  {formatPercent(did, { from: 'ratio', decimals: 0 })} of {bin.trade_count}
                </span>
              )}
            </span>
          </div>
        );
      })}
    </div>
  );
}

export default function StrategyPerformance() {
  const [sortKey, setSortKey] = React.useState<SortKey>('alpha');
  const [selected, setSelected] = React.useState<string | null>(null);

  const { data, error, isLoading } = useSWR<BacktestResult[]>('/backtest/results', fetcher, {
    refreshInterval: POLL.slow,
  });

  const results = Array.isArray(data) ? data : null;

  const sorted = React.useMemo(() => {
    if (!results) return [];
    const of = SORTS.find((s) => s.key === sortKey)!.of;
    // Ascending for drawdown -- less is better -- descending for the rest.
    const direction = sortKey === 'drawdown' ? 1 : -1;
    return [...results].sort((a, b) => {
      const av = of(a);
      const bv = of(b);
      // A strategy with no measurement sorts last either way, rather than
      // being treated as zero and landing in the middle of the table.
      if (!isPresent(av) && !isPresent(bv)) return 0;
      if (!isPresent(av)) return 1;
      if (!isPresent(bv)) return -1;
      return (av - bv) * direction;
    });
  }, [results, sortKey]);

  const open = sorted.find((r) => r.strategy_id + r.ticker === selected) ?? null;

  if (error) {
    return (
      <Card className="h-full">
        <EmptyState
          kind="error"
          title="Backtest results unavailable"
          detail={describeApiError(error) ?? undefined}
        />
      </Card>
    );
  }
  if (isLoading || !results) {
    return (
      <Card className="h-full">
        <EmptyState kind="loading" title="Reading stored backtests" />
      </Card>
    );
  }
  if (results.length === 0) {
    return (
      <Card className="h-full">
        <EmptyState
          kind="empty"
          title="No backtests stored"
          detail="The backtester has not written a result yet."
        />
      </Card>
    );
  }

  const authentic = results.filter((r) => r.data_provenance === 'authentic_market_data').length;

  return (
    <Card noPadding className="flex h-full flex-col overflow-hidden">
      <div className="panel-header shrink-0">
        <div className="min-w-0">
          <h2 className="panel-title">Strategy results</h2>
          <p className="panel-subtitle">
            {formatNumber(results.length, { decimals: 0 })} stored backtests,{' '}
            {authentic === results.length
              ? 'all on recorded market data'
              : `${authentic} on recorded market data`}
          </p>
        </div>
        <div className="flex items-center gap-1.5">
          {/* The results were readable and there was no way to ask for a new
              one: `POST /backtest/run` had no caller, so the 228 stored
              results were whatever a scheduled pass had left behind. */}
          <RunBacktest />
          {/* The whole result set, not the 120 rows the table caps at: the cap
              is a rendering budget, and exporting only what happens to be on
              screen would silently truncate the file. */}
          <ExportButton subject="strategy results" rows={sorted} columns={CSV_COLUMNS} />
          <span className="stat-label">Sort</span>
          {SORTS.map((s) => (
            <button
              key={s.key}
              onClick={() => setSortKey(s.key)}
              aria-pressed={sortKey === s.key}
              className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium transition-colors ${
                sortKey === s.key
                  ? 'border-line-accent bg-accent-dim text-accent'
                  : 'border-line text-ink-mute hover:text-ink-dim'
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <table className="w-full text-left text-xs">
          <thead className="sticky top-0 bg-inset">
            <tr className="border-b border-line">
              <th className="stat-label px-3 py-2">Strategy</th>
              <th className="stat-label px-3 py-2">Ticker</th>
              <th className="stat-label px-3 py-2 text-right">Alpha</th>
              <th className="stat-label px-3 py-2 text-right">Sharpe</th>
              <th className="stat-label px-3 py-2 text-right">Hit rate</th>
              <th className="stat-label px-3 py-2 text-right">Drawdown</th>
              <th className="stat-label px-3 py-2 text-right">Trades</th>
            </tr>
          </thead>
          <tbody>
            {sorted.slice(0, 120).map((r) => {
              const id = r.strategy_id + r.ticker;
              const perf = r.performance_metrics;
              // A hit rate over fewer than ten trades is not a rate, it is a
              // handful of outcomes; shown, but marked as thin.
              const thin = perf.total_trades < 10;
              return (
                <React.Fragment key={id}>
                  <tr
                    onClick={() => setSelected(selected === id ? null : id)}
                    className={`cursor-pointer border-b border-line/60 transition-colors hover:bg-overlay ${
                      selected === id ? 'bg-overlay' : ''
                    }`}
                  >
                    <td className="px-3 py-2 text-ink">{r.strategy_name}</td>
                    <td className="px-3 py-2 font-mono font-semibold text-accent">{r.ticker}</td>
                    <td className="px-3 py-2 text-right">
                      <Signed value={perf.alpha_pct} />
                    </td>
                    <td className="px-3 py-2 text-right font-mono text-ink-dim">
                      {formatNumber(r.risk_metrics.realized_sharpe_ratio, { decimals: 2 })}
                    </td>
                    <td className="px-3 py-2 text-right font-mono text-ink-dim">
                      {isPresent(perf.hit_rate_pct) ? (
                        <span className={thin ? 'text-ink-mute' : undefined}>
                          {formatNumber(perf.hit_rate_pct, { decimals: 1 })}%
                        </span>
                      ) : (
                        ABSENT
                      )}
                    </td>
                    <td className="px-3 py-2 text-right font-mono text-ink-dim">
                      {formatNumber(r.risk_metrics.max_drawdown_pct, { decimals: 2 })}%
                    </td>
                    <td className="px-3 py-2 text-right font-mono">
                      <span className={thin ? 'text-caution' : 'text-ink-dim'}>
                        {perf.total_trades}
                      </span>
                    </td>
                  </tr>
                  {selected === id && open && (
                    <tr className="border-b border-line bg-inset">
                      <td colSpan={7} className="px-3 py-3">
                        <div className="grid gap-4 md:grid-cols-2">
                          <div className="space-y-2">
                            <span className="stat-label flex items-center gap-1.5">
                              {perf.total_trades >= 10 ? <IconCheck /> : <IconAlert />}
                              Calibration — what it said against what happened
                            </span>
                            <Calibration bins={open.calibration_curve ?? []} />
                          </div>
                          <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-micro">
                            {(
                              [
                                [
                                  'Return',
                                  `${formatNumber(perf.total_return_pct, { decimals: 2 })}%`,
                                ],
                                [
                                  'Benchmark',
                                  `${formatNumber(perf.benchmark_return_pct, { decimals: 2 })}%`,
                                ],
                                [
                                  'Sortino',
                                  formatNumber(open.risk_metrics.sortino_ratio, { decimals: 2 }),
                                ],
                                [
                                  'Payoff ratio',
                                  formatNumber(open.risk_metrics.payoff_ratio, { decimals: 2 }),
                                ],
                                [
                                  'Profit factor',
                                  formatNumber(perf.profit_factor, { decimals: 2 }),
                                ],
                                ['Bars tested', formatNumber(open.bar_count, { decimals: 0 })],
                                ['Won', String(perf.winning_trades)],
                                ['Lost', String(perf.losing_trades)],
                              ] as const
                            ).map(([label, value]) => (
                              <React.Fragment key={label}>
                                <dt className="text-ink-mute">{label}</dt>
                                <dd className="text-right font-mono text-ink-dim">{value}</dd>
                              </React.Fragment>
                            ))}
                          </dl>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
        {sorted.length > 120 && (
          <p className="px-3 py-2 text-micro text-ink-mute">
            Showing the first 120 of {sorted.length} by{' '}
            {SORTS.find((s) => s.key === sortKey)!.label.toLowerCase()}.
          </p>
        )}
      </div>
    </Card>
  );
}
