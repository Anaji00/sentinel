'use client';

/** Insiders' notices of proposed sales (Form 144; /filings/form144). */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface Notice {
  ticker: string | null;
  issuer_name: string | null;
  seller: string | null;
  relationship: string | null;
  shares: number | null;
  market_value: number | null;
  pct_outstanding: number | null;
  approx_sale_date: string | null;
  notice_date: string | null;
  accession: string;
}

/** "$1.2M" / "$89k". */
export function money(v: number | null): string {
  if (v == null) return '—';
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `$${Math.round(v / 1e3)}k`;
  return `$${Math.round(v)}`;
}

export function InsiderIntent() {
  const { data } = useSWR<{ notices: Notice[]; note?: string }>('/filings/form144?days=14&limit=60', fetcher, {
    refreshInterval: POLL.slow,
  });
  return (
    <section className="panel p-4 space-y-2 text-xs">
      <h2 className="text-sm font-semibold text-ink">Insiders planning to sell (Form 144)</h2>
      <p className="text-ink-dim">
        Directors, officers and large holders must file a notice before selling restricted stock. It arrives ahead of
        the Form 4 that reports the sale. Largest first, last two weeks.
      </p>
      {!data ? (
        <LoadingLines />
      ) : data.notices.length === 0 ? (
        <p className="text-ink-mute">No notices collected yet; they arrive every ten minutes as filed.</p>
      ) : (
        <ul className="divide-y divide-line max-h-96 overflow-y-auto">
          {data.notices.map((n) => (
            <li key={n.accession} className="py-1.5 flex flex-wrap items-baseline gap-x-2">
              <span className="symbol text-ink">{n.ticker ?? '—'}</span>
              <span className="text-ink-dim">{n.seller ?? 'Unnamed'}</span>
              {n.relationship && <span className="text-ink-mute">({n.relationship})</span>}
              <span className="ml-auto font-mono text-ink">{money(n.market_value)}</span>
              <span className="text-micro text-ink-mute">
                {n.pct_outstanding != null ? `${n.pct_outstanding.toFixed(2)}% of shares · ` : ''}
                sale around {n.approx_sale_date ?? '?'}
              </span>
            </li>
          ))}
        </ul>
      )}
      {data?.note && <p className="text-micro text-ink-mute">{data.note}</p>}
    </section>
  );
}

export default InsiderIntent;
