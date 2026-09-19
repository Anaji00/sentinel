'use client';

/**
 * Which of this platform's signals actually predict anything.
 *
 * The system carries 57 hand-set weights and thresholds. Every one was chosen
 * by judgement, which is a reasonable way to start and no way to stay. The
 * endpoint measures each signal's lift over the base rate from resolved
 * outcomes, so a person can see which of them deserve their number -- and it
 * had no caller, so nobody ever saw.
 *
 * It also could not have been called. Joined against the events hypertable it
 * timed out at 120 seconds; measured on the deployment, the same window and the
 * same answer took 336ms once the trigger events were fetched by id instead.
 *
 * What it says on this deployment, over 777 resolved outcomes:
 *
 *   watched_entity   lift +0.257   931 samples, intervals disjoint
 *   high_anomaly     lift -0.097   931 samples, intervals disjoint
 *
 * The second one is the platform's own top-band anomaly score, and the sign is
 * negative: scoring in the top band makes confirmation *less* likely. That is
 * the finding this panel exists to make visible, so it is stated rather than
 * sorted quietly to the bottom of a table.
 *
 * Support is shown beside every figure, always. A lift of 0.4 on nine samples
 * and 0.04 on nine thousand are opposite findings that look alike in a column.
 */

import React from 'react';
import useSWR from 'swr';
import { describeApiError, fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { EmptyState } from './ui/EmptyState';
import { ExportButton } from './ui/ExportButton';
import { POLL } from './ui/DataProvider';
import { IconAlert, IconCheck, IconDown, IconTarget, IconUp } from './ui/icons';
import { ABSENT, formatNumber, formatPercent, isPresent } from '../lib/format';
import type { CsvColumn } from '../lib/csv';

interface Signal {
  signal: string;
  describes: string;
  measurable: boolean;
  reason?: string;
  rate_when_present?: number;
  rate_when_absent?: number;
  lift?: number;
  support?: number;
  support_present?: number;
  support_absent?: number;
  intervals_disjoint?: boolean;
  interval_present?: [number, number];
  interval_absent?: [number, number];
}

interface Attribution {
  available: boolean;
  reason?: string;
  lookback_days?: number;
  resolved_outcomes?: number;
  base_rate?: number;
  signals?: Signal[];
  carrying?: string[];
  inverted?: string[];
  not_distinguishing?: string[];
}

const WINDOWS = [30, 90, 180, 365] as const;

const CSV_COLUMNS: CsvColumn<Signal>[] = [
  { label: 'signal', value: (s) => s.signal },
  { label: 'describes', value: (s) => s.describes },
  { label: 'measurable', value: (s) => String(s.measurable) },
  { label: 'reason', value: (s) => s.reason ?? null },
  { label: 'lift', value: (s) => s.lift ?? null },
  { label: 'rate_when_present', value: (s) => s.rate_when_present ?? null },
  { label: 'rate_when_absent', value: (s) => s.rate_when_absent ?? null },
  { label: 'support', value: (s) => s.support ?? null },
  { label: 'support_present', value: (s) => s.support_present ?? null },
  { label: 'support_absent', value: (s) => s.support_absent ?? null },
  {
    label: 'intervals_disjoint',
    value: (s) => (s.measurable ? String(s.intervals_disjoint) : null),
  },
];

/**
 * The bar is centred on zero, because the sign is the finding.
 *
 * A signal with negative lift points the wrong way, and drawing it as a short
 * bar in the same direction as a weak positive one would hide the only thing
 * worth noticing about it.
 */
function LiftBar({ lift, disjoint }: { lift: number; disjoint: boolean | undefined }) {
  // Clamped at ±0.4: beyond that the bar stops being informative and the
  // number beside it carries the magnitude anyway.
  const width = Math.min(Math.abs(lift) / 0.4, 1) * 50;
  const positive = lift >= 0;
  return (
    <div className="relative h-2 w-full overflow-hidden rounded bg-inset">
      <div className="absolute inset-y-0 left-1/2 w-px bg-line-strong" />
      <div
        className={`absolute inset-y-0 ${positive ? 'left-1/2' : ''} ${
          // A lift whose intervals overlap is not distinguishable from zero,
          // so it is drawn muted rather than coloured as a result.
          !disjoint ? 'bg-ink-mute/40' : positive ? 'bg-positive' : 'bg-negative'
        }`}
        style={positive ? { width: `${width}%` } : { right: '50%', width: `${width}%` }}
      />
    </div>
  );
}

export default function SignalAttribution() {
  const [days, setDays] = React.useState<number>(90);

  const { data, error, isLoading } = useSWR<Attribution>(
    `/attribution/signals?lookback_days=${days}`,
    fetcher,
    // Outcomes resolve over days. Re-asking often would be a slow query for
    // an answer that cannot have changed.
    { refreshInterval: POLL.rare },
  );

  const signals = data?.signals ?? [];
  const measured = signals.filter((s) => s.measurable);
  const unmeasured = signals.filter((s) => !s.measurable);

  // Sorted by magnitude, not by sign: a strongly inverted signal is as
  // important as a strongly positive one and belongs at the top with it.
  const sorted = [...measured].sort((a, b) => Math.abs(b.lift ?? 0) - Math.abs(a.lift ?? 0));

  if (error) {
    return (
      <Card className="h-full">
        <EmptyState
          kind="error"
          title="Attribution unavailable"
          detail={describeApiError(error) ?? undefined}
        />
      </Card>
    );
  }

  return (
    <Card noPadding className="flex h-full flex-col overflow-hidden">
      <div className="panel-header shrink-0">
        <div className="min-w-0">
          <h2 className="panel-title flex items-center gap-1.5">
            <IconTarget />
            Signal attribution
          </h2>
          <p className="panel-subtitle">
            {data?.available
              ? `${formatNumber(data.resolved_outcomes, { decimals: 0 })} resolved outcomes · base rate ${formatPercent(data.base_rate, { from: 'ratio', decimals: 1 })}`
              : 'How much each signal moves the odds'}
          </p>
        </div>
        <div className="flex items-center gap-1.5">
          <ExportButton subject="signal attribution" rows={signals} columns={CSV_COLUMNS} />
          {WINDOWS.map((w) => (
            <button
              key={w}
              onClick={() => setDays(w)}
              aria-pressed={days === w}
              className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium transition-colors ${
                days === w
                  ? 'border-line-accent bg-accent-dim text-accent'
                  : 'border-line text-ink-mute hover:text-ink-dim'
              }`}
            >
              {w}d
            </button>
          ))}
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {isLoading && !data ? (
          <EmptyState kind="loading" title="Measuring against resolved outcomes" />
        ) : !data?.available ? (
          <EmptyState
            kind="empty"
            title="Not enough resolved outcomes yet"
            // The server's own sentence. It distinguishes "too little history"
            // from "measured and found nothing", which are opposite facts.
            detail={data?.reason}
          />
        ) : (
          <>
            {(data.inverted?.length ?? 0) > 0 && (
              <div className="border-b border-line bg-negative/5 px-3.5 py-2.5">
                <p className="flex items-start gap-1.5 text-micro tone-negative">
                  <IconAlert className="mt-0.5 shrink-0" />
                  <span>
                    <span className="font-semibold">
                      {data.inverted!.join(', ')} points the wrong way.
                    </span>{' '}
                    <span className="text-ink-dim">
                      Present, the outcome confirms less often than when it is absent, and the
                      confidence intervals do not overlap. Whatever weight this carries in the
                      scoring tree is being spent against the result.
                    </span>
                  </span>
                </p>
              </div>
            )}

            <table className="w-full text-left text-xs">
              <thead className="sticky top-0 bg-inset">
                <tr className="border-b border-line">
                  <th className="stat-label px-3 py-2">Signal</th>
                  <th className="stat-label px-3 py-2 w-40">Lift</th>
                  <th className="stat-label px-3 py-2 text-right">Present</th>
                  <th className="stat-label px-3 py-2 text-right">Absent</th>
                  <th className="stat-label px-3 py-2 text-right">Support</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((s) => {
                  const lift = s.lift ?? 0;
                  const tone = !s.intervals_disjoint
                    ? 'text-ink-mute'
                    : lift > 0
                      ? 'text-positive'
                      : 'text-negative';
                  return (
                    <tr key={s.signal} className="border-b border-line/60 hover:bg-overlay">
                      <td className="px-3 py-2">
                        <span className="block font-mono text-ink">{s.signal}</span>
                        <span className="block text-micro text-ink-mute">{s.describes}</span>
                      </td>
                      <td className="px-3 py-2">
                        <div className="flex items-center gap-2">
                          <LiftBar lift={lift} disjoint={s.intervals_disjoint} />
                          <span className={`shrink-0 font-mono tabular-nums ${tone}`}>
                            {lift > 0 ? <IconUp /> : lift < 0 ? <IconDown /> : null}
                            {formatNumber(lift, { decimals: 3, signed: true })}
                          </span>
                        </div>
                        {/* Whether the two intervals overlap is the difference
                            between a result and a coincidence, and it is a
                            yes-or-no the server already computed. */}
                        <span className="text-micro text-ink-mute">
                          {s.intervals_disjoint
                            ? 'intervals disjoint'
                            : 'intervals overlap — not distinguishable from zero'}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-ink-dim">
                        {isPresent(s.rate_when_present)
                          ? formatPercent(s.rate_when_present, { from: 'ratio', decimals: 1 })
                          : ABSENT}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-ink-dim">
                        {isPresent(s.rate_when_absent)
                          ? formatPercent(s.rate_when_absent, { from: 'ratio', decimals: 1 })
                          : ABSENT}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-ink-mute">
                        {formatNumber(s.support, { decimals: 0 })}
                        <span className="block text-micro">
                          {formatNumber(s.support_present, { decimals: 0 })} /{' '}
                          {formatNumber(s.support_absent, { decimals: 0 })}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>

            {unmeasured.length > 0 && (
              <div className="space-y-1.5 border-t border-line px-3.5 py-3">
                <span className="stat-label">Not measurable in this window</span>
                {unmeasured.map((s) => (
                  <p key={s.signal} className="text-micro text-ink-mute">
                    <span className="font-mono text-ink-dim">{s.signal}</span> — {s.reason}
                  </p>
                ))}
                <p className="text-micro text-ink-mute">
                  {/* The distinction the endpoint was written to preserve. */}
                  Too little history to judge is not the same as judged and found to be worth
                  nothing, and a table of zeros would make them look alike.
                </p>
              </div>
            )}

            {(data.carrying?.length ?? 0) > 0 && (
              <div className="border-t border-line px-3.5 py-2.5">
                <p className="flex items-center gap-1.5 text-micro tone-positive">
                  <IconCheck />
                  <span>
                    <span className="font-semibold">{data.carrying!.join(', ')}</span>{' '}
                    <span className="text-ink-dim">
                      {data.carrying!.length === 1 ? 'is' : 'are'} doing measurable work.
                    </span>
                  </span>
                </p>
              </div>
            )}
          </>
        )}
      </div>
    </Card>
  );
}
