'use client';

/** The EIA's weekly petroleum stocks and how unusual this week is (/radar/eia-stocks). */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface Row {
  series: string;
  label: string;
  week_ending: string;
  level: number;
  change: number;
  change_z: number | null;
  vs_five_year_pct: number | null;
}

/** A week's change in words, millions of barrels. */
export function changeWords(r: Pick<Row, 'change' | 'change_z'>): string {
  const mb = (Math.abs(r.change) / 1000).toFixed(1);
  const dir = r.change < 0 ? 'drew' : 'built';
  const z = r.change_z == null ? null : Math.abs(r.change_z);
  const size = z == null ? '' : z >= 2 ? ', unusually large' : z >= 1 ? ', larger than usual' : ', a normal week';
  return `${dir} ${mb}M barrels${size}`;
}

export function OilStocks() {
  const { data } = useSWR<{ series: Row[]; source: string }>('/radar/eia-stocks', fetcher, { refreshInterval: POLL.slow });
  return (
    <section className="panel p-4 space-y-2 text-xs">
      <h2 className="text-sm font-semibold text-ink">Oil and fuel stockpiles (weekly)</h2>
      <p className="text-ink-dim">
        US inventories from the EIA each Wednesday. A big draw (falling stocks) tends to lift prices. Each change is
        compared with three years of weekly changes, and each level with the five-year average for the same week.
      </p>
      {!data || data.series.length === 0 ? (
        <LoadingLines />
      ) : (
        <ul className="grid grid-cols-1 sm:grid-cols-2 gap-2">
          {data.series.map((r) => (
            <li key={r.series} className="rounded-lg border border-line bg-inset p-2.5">
              <div className="text-ink font-medium">{r.label}</div>
              <div className={r.change < 0 ? 'text-positive' : 'text-caution'}>{changeWords(r)}</div>
              <div className="text-micro text-ink-mute">
                {(r.level / 1000).toFixed(1)}M barrels
                {r.vs_five_year_pct != null
                  ? ` · ${r.vs_five_year_pct > 0 ? '+' : ''}${r.vs_five_year_pct.toFixed(1)}% vs the five-year average`
                  : ''}
                {` · week to ${r.week_ending}`}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export default OilStocks;
