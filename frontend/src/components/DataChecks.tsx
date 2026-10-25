'use client';

/**
 * Do the stored minute bars add up to each stock's day?
 * (/api/v1/radar/data-quality; shared/utils/bar_quality.py)
 *
 * For weeks the minute bars held one exchange's trades -- 2-6% of each stock's
 * volume -- and nothing on screen could have said so. This is that check.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface Worst {
  ticker: string;
  coverage: number;
  minutes: number;
}

export interface BarQuality {
  available: boolean;
  note?: string;
  day?: string | null;
  stocks?: number;
  median_coverage?: number | null;
  below_floor?: number;
  missing?: number;
  floor?: number;
  worst?: Worst[];
  ok?: boolean | null;
  checked_at?: string;
  crosscheck?: {
    day: string;
    vendor?: string;
    compared: number;
    close_mismatches: number;
    volume_mismatches: number;
    ok: boolean;
  } | null;
}

/** "94% of the day's volume" and a verdict in words. */
export function coverageLine(q: BarQuality): string {
  if (!q.available || q.median_coverage == null) return q.note ?? 'Not checked yet.';
  const pct = Math.round(q.median_coverage * 100);
  const verdict = q.ok ? 'Healthy' : 'Needs a look';
  return `${verdict}: the typical stock's minute bars add up to ${pct}% of its official daily volume (${q.day}).`;
}

export function DataChecks() {
  const { data } = useSWR<BarQuality>('/api/v1/radar/data-quality', fetcher, { refreshInterval: POLL.slow });
  return (
    <div className="panel flex flex-col min-h-0 overflow-hidden">
      <div className="panel-header shrink-0 !justify-start items-baseline">
        <span className="panel-title">Data checks</span>
        <span className="text-micro text-ink-mute">do minute bars add up to the day?</span>
      </div>
      <div className="flex-1 min-h-0 overflow-y-auto p-3 space-y-2 text-xs">
        {!data ? (
          <LoadingLines />
        ) : (
          <>
            <p className={data.ok ? 'text-positive' : data.ok === false ? 'text-caution' : 'text-ink-mute'}>
              {coverageLine(data)}
            </p>
            {data.available && (
              <p className="text-ink-mute">
                {data.stocks} stocks checked · {data.below_floor} below {Math.round((data.floor ?? 0) * 100)}% ·{' '}
                {data.missing} with no minute bars
              </p>
            )}
            <p className="text-ink-mute">
              {data.crosscheck
                ? `Second vendor (${data.crosscheck.vendor ?? 'Polygon'}), ${data.crosscheck.day}: ${data.crosscheck.compared} stocks compared, ${data.crosscheck.close_mismatches} closes and ${data.crosscheck.volume_mismatches} volumes disagree.`
                : 'Second-vendor check: the first daily comparison runs 15 minutes after the market collector starts.'}
            </p>
            {(data.worst ?? []).length > 0 && (
              <ul className="font-mono text-micro text-ink-dim">
                {(data.worst ?? []).slice(0, 8).map((w) => (
                  <li key={w.ticker} className="flex justify-between">
                    <span>{w.ticker}</span>
                    <span>
                      {Math.round(w.coverage * 100)}% · {w.minutes} min
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </>
        )}
      </div>
    </div>
  );
}

export default DataChecks;
