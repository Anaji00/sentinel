'use client';

/**
 * Three answers about one symbol, none of which had a caller.
 *
 *   `/radar/candles/{ticker}`        the stored bars, at a chosen timeframe
 *   `/filings/13f/consensus/{ticker}` which prominent filers hold it, and how
 *   `/radar/options/covered-calls`   whether an overlay is worth writing
 *
 * They were three separate unreachable routes and they are one question: what
 * does this platform know about this ticker. Splitting them across three
 * screens is part of why none of them got built.
 *
 * The covered-call endpoint is the interesting one to render honestly. It
 * answers `GATED_OR_INVALID` with the z-score that failed its own threshold,
 * which is a *result* -- the overlay is not worth writing right now -- and not
 * an error. A panel that showed a spinner or a red box there would be
 * reporting an outage where the platform had given a considered no.
 */

import React from 'react';
import useSWR from 'swr';
import { describeApiError, fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { EmptyState } from './ui/EmptyState';
import { ExportButton } from './ui/ExportButton';
import { ClockTime } from './ui/ClockTime';
import { POLL } from './ui/DataProvider';
import { IconBlocked, IconCheck, IconDown, IconSignal, IconUp } from './ui/icons';
import { ABSENT, formatCurrency, formatNumber, isPresent } from '../lib/format';
import type { CsvColumn } from '../lib/csv';

const TIMEFRAMES = ['5m', '15m', '1h', '4h', '1d'] as const;

interface Candle {
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  ticker: string;
}

interface CandleResponse {
  ticker: string;
  timeframe: string;
  count: number;
  source: string;
  candles: Candle[];
}

interface Consensus {
  ticker: string;
  institutional_buyers: string[];
  total_prominent_holders: number;
  consensus_sentiment: string;
  derived_from_filings: boolean;
}

interface CoveredCall {
  status?: string;
  message?: string;
  strategy?: string;
  ticker?: string;
  underlying_price?: number;
  strike?: number;
  premium?: number;
  annualized_yield_pct?: number;
  z_score?: number;
  expiry?: string;
}

const CANDLE_COLUMNS: CsvColumn<Candle>[] = [
  { label: 'ts', value: (c) => c.ts },
  { label: 'open', value: (c) => c.open },
  { label: 'high', value: (c) => c.high },
  { label: 'low', value: (c) => c.low },
  { label: 'close', value: (c) => c.close },
  { label: 'volume', value: (c) => c.volume },
];

/** Open to close, which is the only direction a single bar has. */
function Direction({ open, close }: { open: number; close: number }) {
  const move = close - open;
  const pct = open !== 0 ? (move / open) * 100 : null;
  const tone = move > 0 ? 'text-positive' : move < 0 ? 'text-negative' : 'text-ink-mute';
  return (
    <span className={`inline-flex items-center gap-1 font-mono tabular-nums ${tone}`}>
      {move > 0 ? <IconUp /> : move < 0 ? <IconDown /> : null}
      {isPresent(pct) ? `${formatNumber(pct, { decimals: 2, signed: true })}%` : ABSENT}
    </span>
  );
}

export default function TickerInspector() {
  const [input, setInput] = React.useState('');
  const [ticker, setTicker] = React.useState('');
  const [timeframe, setTimeframe] = React.useState<string>('1h');

  const q = ticker ? encodeURIComponent(ticker) : null;

  const candles = useSWR<CandleResponse>(
    q ? `/radar/candles/${q}?timeframe=${timeframe}&limit=24` : null,
    fetcher,
    { refreshInterval: POLL.standard },
  );
  const consensus = useSWR<Consensus>(q ? `/filings/13f/consensus/${q}` : null, fetcher, {
    // 13F holdings change quarterly.
    refreshInterval: POLL.rare,
  });
  const overlay = useSWR<CoveredCall>(
    q ? `/radar/options/covered-calls?ticker=${q}` : null,
    fetcher,
    { refreshInterval: POLL.slow },
  );

  const gated = overlay.data?.status === 'GATED_OR_INVALID';
  const rows = candles.data?.candles ?? [];

  return (
    <Card noPadding className="flex h-full flex-col overflow-hidden">
      <div className="panel-header shrink-0">
        <div className="min-w-0">
          <h2 className="panel-title flex items-center gap-1.5">
            <IconSignal />
            Ticker inspector
          </h2>
          <p className="panel-subtitle">
            Stored bars, institutional holdings and the covered-call gate, for one symbol
          </p>
        </div>
        {rows.length > 0 && (
          <ExportButton
            subject={`${ticker} ${timeframe} bars`}
            rows={rows}
            columns={CANDLE_COLUMNS}
          />
        )}
      </div>

      <div className="flex shrink-0 flex-wrap items-center gap-1.5 border-b border-line px-3.5 py-2.5">
        <label className="sr-only" htmlFor="inspect-ticker">
          Ticker
        </label>
        <input
          id="inspect-ticker"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') setTicker(input.trim().toUpperCase());
          }}
          maxLength={10}
          placeholder="NVDA"
          className="w-28 rounded-md border border-line bg-page px-2 py-1 font-mono text-micro uppercase text-ink outline-none focus:border-line-accent"
        />
        <button
          onClick={() => setTicker(input.trim().toUpperCase())}
          disabled={!input.trim()}
          className="rounded-md border border-line px-2 py-1 text-micro font-medium text-ink-dim transition-colors enabled:cursor-pointer enabled:hover:border-line-strong enabled:hover:text-ink disabled:opacity-40"
        >
          Inspect
        </button>
        <span className="stat-label ml-2">Bars</span>
        {TIMEFRAMES.map((t) => (
          <button
            key={t}
            onClick={() => setTimeframe(t)}
            aria-pressed={timeframe === t}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium transition-colors ${
              timeframe === t
                ? 'border-line-accent bg-accent-dim text-accent'
                : 'border-line text-ink-mute hover:text-ink-dim'
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {!ticker ? (
          <EmptyState
            kind="empty"
            title="No symbol selected"
            detail="Enter a ticker to see the bars this platform has stored for it, who holds it, and whether a covered-call overlay clears its own gate."
          />
        ) : (
          <div className="space-y-3 px-3.5 py-3">
            {/* ── holdings ───────────────────────────────────────────── */}
            <div className="space-y-1.5 rounded-lg border border-line bg-inset p-3">
              <span className="stat-label">Institutional consensus</span>
              {consensus.error ? (
                <p className="text-micro text-ink-mute">
                  {describeApiError(consensus.error) ?? 'Could not read 13F holdings.'}
                </p>
              ) : !consensus.data ? (
                <p className="text-micro text-ink-mute">Reading filings…</p>
              ) : consensus.data.total_prominent_holders === 0 ? (
                <p className="text-micro text-ink-mute">
                  No prominent filer in this platform&rsquo;s set holds {ticker}. That is a fact
                  about the set, not about the company.
                </p>
              ) : (
                <>
                  <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                    <span
                      className={`badge ${
                        consensus.data.consensus_sentiment === 'ACCUMULATING'
                          ? 'tone-positive'
                          : consensus.data.consensus_sentiment === 'DISTRIBUTING'
                            ? 'tone-negative'
                            : 'tone-muted'
                      }`}
                    >
                      {consensus.data.consensus_sentiment}
                    </span>
                    <span className="text-micro text-ink-mute">
                      {formatNumber(consensus.data.total_prominent_holders, { decimals: 0 })}{' '}
                      prominent{' '}
                      {consensus.data.total_prominent_holders === 1 ? 'holder' : 'holders'}
                      {/* Quarterly filings, and worth saying so: a 13F is a
                          snapshot up to 45 days stale by the time it is filed. */}
                      , from quarterly 13F filings
                    </span>
                  </div>
                  <p className="text-micro text-ink-dim">
                    {consensus.data.institutional_buyers.join(' · ')}
                  </p>
                </>
              )}
            </div>

            {/* ── the overlay gate ───────────────────────────────────── */}
            <div className="space-y-1.5 rounded-lg border border-line bg-inset p-3">
              <span className="stat-label flex items-center gap-1.5">
                {gated ? <IconBlocked /> : <IconCheck />}
                Covered-call overlay
              </span>
              {overlay.error ? (
                <p className="text-micro text-ink-mute">
                  {describeApiError(overlay.error) ?? 'Could not evaluate the overlay.'}
                </p>
              ) : !overlay.data ? (
                <p className="text-micro text-ink-mute">Evaluating…</p>
              ) : gated ? (
                // A considered no, not a failure. The endpoint returns the
                // z-score that missed its own threshold, so the reason is
                // quoted rather than summarised.
                <p className="text-micro text-ink-dim">{overlay.data.message}</p>
              ) : (
                <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-micro sm:grid-cols-4">
                  {(
                    [
                      ['Underlying', formatCurrency(overlay.data.underlying_price)],
                      ['Strike', formatCurrency(overlay.data.strike)],
                      ['Premium', formatCurrency(overlay.data.premium)],
                      [
                        'Annualised',
                        isPresent(overlay.data.annualized_yield_pct)
                          ? `${formatNumber(overlay.data.annualized_yield_pct, { decimals: 1 })}%`
                          : ABSENT,
                      ],
                    ] as const
                  ).map(([label, value]) => (
                    <React.Fragment key={label}>
                      <dt className="text-ink-mute">{label}</dt>
                      <dd className="text-right font-mono text-ink-dim">{value}</dd>
                    </React.Fragment>
                  ))}
                </dl>
              )}
            </div>

            {/* ── the bars ───────────────────────────────────────────── */}
            <div className="space-y-1.5">
              <div className="flex items-baseline justify-between gap-2">
                <span className="stat-label">Stored bars</span>
                {candles.data && (
                  <span className="text-micro text-ink-mute">
                    {formatNumber(candles.data.count, { decimals: 0 })} at {candles.data.timeframe}{' '}
                    {/* Where the bars came from. A cache read and a database
                        read can differ in freshness and the endpoint says
                        which it did. */}
                    from {candles.data.source}
                  </span>
                )}
              </div>
              {candles.error ? (
                <p className="text-micro text-ink-mute">
                  {describeApiError(candles.error) ?? 'Could not read bars.'}
                </p>
              ) : !candles.data ? (
                <p className="text-micro text-ink-mute">Reading bars…</p>
              ) : rows.length === 0 ? (
                <p className="text-micro text-ink-mute">
                  Nothing stored for {ticker} at {timeframe}. The platform tracks a fixed universe;
                  a symbol outside it has no bars rather than empty ones.
                </p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-left text-micro">
                    <thead>
                      <tr className="border-b border-line">
                        <th className="stat-label px-2 py-1">Time</th>
                        <th className="stat-label px-2 py-1 text-right">Open</th>
                        <th className="stat-label px-2 py-1 text-right">High</th>
                        <th className="stat-label px-2 py-1 text-right">Low</th>
                        <th className="stat-label px-2 py-1 text-right">Close</th>
                        <th className="stat-label px-2 py-1 text-right">Move</th>
                        <th className="stat-label px-2 py-1 text-right">Volume</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((c) => (
                        <tr key={c.ts} className="border-b border-line/60">
                          <td className="whitespace-nowrap px-2 py-1 font-mono text-ink-mute">
                            <ClockTime value={c.ts} seconds={false} />
                          </td>
                          <td className="px-2 py-1 text-right font-mono text-ink-dim">
                            {formatNumber(c.open)}
                          </td>
                          <td className="px-2 py-1 text-right font-mono text-ink-dim">
                            {formatNumber(c.high)}
                          </td>
                          <td className="px-2 py-1 text-right font-mono text-ink-dim">
                            {formatNumber(c.low)}
                          </td>
                          <td className="px-2 py-1 text-right font-mono text-ink">
                            {formatNumber(c.close)}
                          </td>
                          <td className="px-2 py-1 text-right">
                            <Direction open={c.open} close={c.close} />
                          </td>
                          <td className="px-2 py-1 text-right font-mono text-ink-mute">
                            {formatNumber(c.volume, { decimals: 0 })}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}
