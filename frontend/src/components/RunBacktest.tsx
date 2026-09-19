'use client';

/**
 * Asking for a backtest, rather than reading whatever one was left behind.
 *
 * `POST /backtest/run` takes a ticker, a strategy, a timeframe and a starting
 * capital, runs the strategy over real stored bars and writes the result. It
 * had no caller, so the 228 stored results the panel shows were whatever a
 * scheduled pass had happened to produce -- readable, and not askable.
 *
 * The three strategies and seven timeframes are the server's own defaults,
 * named in `RunBacktestRequest`. An unknown value is refused there, so offering
 * a free-text box would only move the rejection later.
 */

import React from 'react';
import { mutate as globalMutate } from 'swr';
import { apiClient, describeApiError } from '../lib/api';
import { useFeedback } from './ui/Feedback';
import { useDialog } from './ui/useDialog';
import { IconClose, IconGraph } from './ui/icons';

/** `strategy_type` in services/api_gateway/routes/backtest.py. */
const STRATEGIES = [
  { id: 'momentum_trend', label: 'Momentum trend' },
  { id: 'mean_reversion', label: 'Mean reversion' },
  { id: 'covered_call', label: 'Covered call' },
] as const;

/** `timeframe`, same file. */
const TIMEFRAMES = ['5m', '15m', '1h', '4h', '1d'] as const;

export function RunBacktest() {
  const { toast } = useFeedback();
  const [open, setOpen] = React.useState(false);
  const dialog = useDialog(open, () => setOpen(false), 'Run a backtest');

  const [ticker, setTicker] = React.useState('');
  const [strategy, setStrategy] = React.useState<string>('momentum_trend');
  const [timeframe, setTimeframe] = React.useState<string>('1h');
  const [busy, setBusy] = React.useState(false);

  const clean = ticker.trim().toUpperCase();
  const valid = clean.length > 0 && clean.length <= 10;

  const run = async () => {
    setBusy(true);
    try {
      const res = await apiClient.post('/backtest/run', {
        ticker: clean,
        strategy_type: strategy,
        timeframe,
      });
      const bars = (res.data as { bar_count?: number })?.bar_count;
      toast(
        'success',
        `Backtest complete for ${clean}`,
        // The bar count is the honest measure of what it ran on. A strategy
        // "tested" over forty bars is not a result, and the panel's own
        // thin-support marking depends on the reader knowing this.
        typeof bars === 'number' ? `${bars} bars tested.` : undefined,
      );
      await globalMutate('/backtest/results');
      setOpen(false);
    } catch (err) {
      toast('error', 'The backtest did not run.', describeApiError(err) ?? undefined);
    }
    setBusy(false);
  };

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        className="flex cursor-pointer items-center gap-1.5 rounded-md border border-line px-2 py-0.5 text-micro font-medium text-ink-dim transition-colors hover:border-line-strong hover:text-ink"
      >
        <IconGraph />
        Run
      </button>

      {open && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4"
          {...dialog.overlayProps}
        >
          <div
            className="w-full max-w-md space-y-4 rounded-2xl border border-line-strong bg-raised p-6 text-ink shadow-2xl"
            {...dialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-line pb-3">
              <h2 className="text-head font-semibold">Run a backtest</h2>
              <button
                onClick={() => setOpen(false)}
                aria-label="Close"
                className="cursor-pointer rounded p-1 text-ink-dim hover:text-ink"
              >
                <IconClose />
              </button>
            </div>

            <label className="block space-y-1">
              <span className="stat-label">Ticker</span>
              <input
                value={ticker}
                onChange={(e) => setTicker(e.target.value)}
                maxLength={10}
                placeholder="NVDA"
                className="w-full rounded-lg border border-line bg-page px-2.5 py-1.5 font-mono text-xs uppercase text-ink outline-none focus:border-line-accent"
              />
            </label>

            <div className="space-y-1">
              <span className="stat-label">Strategy</span>
              <div className="flex flex-wrap gap-1.5">
                {STRATEGIES.map((st) => (
                  <button
                    key={st.id}
                    onClick={() => setStrategy(st.id)}
                    aria-pressed={strategy === st.id}
                    className={`cursor-pointer rounded-md border px-2.5 py-1 text-micro font-medium transition-colors ${
                      strategy === st.id
                        ? 'border-line-accent bg-accent-dim text-accent'
                        : 'border-line text-ink-mute hover:text-ink-dim'
                    }`}
                  >
                    {st.label}
                  </button>
                ))}
              </div>
            </div>

            <div className="space-y-1">
              <span className="stat-label">Timeframe</span>
              <div className="flex flex-wrap gap-1.5">
                {TIMEFRAMES.map((t) => (
                  <button
                    key={t}
                    onClick={() => setTimeframe(t)}
                    aria-pressed={timeframe === t}
                    className={`cursor-pointer rounded-md border px-2 py-1 text-micro font-medium transition-colors ${
                      timeframe === t
                        ? 'border-line-accent bg-accent-dim text-accent'
                        : 'border-line text-ink-mute hover:text-ink-dim'
                    }`}
                  >
                    {t}
                  </button>
                ))}
              </div>
            </div>

            <div className="flex items-center justify-between border-t border-line pt-3">
              <span className="text-micro text-ink-mute">
                {/* It runs on stored bars, so a thin history produces a thin
                    result rather than an error. */}
                Runs over the bars this platform has stored for that symbol.
              </span>
              <button
                onClick={run}
                disabled={!valid || busy}
                className="rounded-lg border border-line-accent bg-accent-dim px-3 py-1.5 text-micro font-semibold text-accent transition-colors enabled:cursor-pointer enabled:hover:border-accent disabled:opacity-40"
              >
                {busy ? 'Running…' : 'Run'}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

export default RunBacktest;
