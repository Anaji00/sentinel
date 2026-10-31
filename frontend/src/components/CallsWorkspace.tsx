'use client';

/**
 * Calls: one workspace for "what should I do with which stock".
 *
 * Stock calls, Signal board and Trade signals were three pages answering the
 * same question from three endpoints. Here the unusual-activity list, the
 * model's call cards and the evidence for whichever stock is selected sit side
 * by side: picking a row updates the evidence instead of leaving the page.
 */

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { directionMark, formatNumber } from '../lib/format';
import { TickerEvidence } from './ConvergingEvidence';
import { StockLink } from './StockDrawer';
import TradeIdeas from './TradeIdeas';
import { POLL } from './ui/DataProvider';
import { EmptyState } from './ui/EmptyState';
import { LoadingLines } from './ui/Skeleton';

interface Count {
  bull: number;
  bear: number;
}

/** The fields of a Signal board row this list reads. */
export interface BoardRow {
  ticker: string;
  name: string | null;
  price: number | null;
  ret_1d: number | null;
  net: number;
  votes: number;
  proven: Count;
  supported: Count;
  published: Count;
  verdict?: string;
  confidence?: 'higher' | 'moderate' | 'low' | 'none';
}

const CONFIDENCE_RANK: Record<string, number> = { higher: 0, moderate: 1, low: 2, none: 3 };

export type Tier = 'all' | 'proven' | 'model';

/** Strongest leans first: confidence, then how lopsided the votes are. */
export function rankBoard(rows: BoardRow[], tier: Tier = 'all'): BoardRow[] {
  return rows
    .filter((r) => r.net !== 0)
    .filter((r) =>
      tier === 'proven'
        ? r.proven.bull + r.proven.bear > 0
        : tier === 'model'
          ? r.proven.bull + r.proven.bear + r.supported.bull + r.supported.bear > 0
          : true,
    )
    .sort(
      (a, b) =>
        (CONFIDENCE_RANK[a.confidence ?? 'none'] ?? 3) - (CONFIDENCE_RANK[b.confidence ?? 'none'] ?? 3) ||
        Math.abs(b.net) / Math.max(1, b.votes) - Math.abs(a.net) / Math.max(1, a.votes),
    );
}

function UnusualList({ selected, onSelect }: { selected: string | null; onSelect: (t: string) => void }) {
  const [tier, setTier] = useState<Tier>('all');
  const { data, error } = useSWR<{ board: BoardRow[] }>('/signals/matrix', fetcher, { refreshInterval: POLL.standard });
  const rows = useMemo(() => rankBoard(data?.board ?? [], tier).slice(0, 40), [data, tier]);
  return (
    <section aria-label="Unusual activity" className="flex min-h-0 flex-col overflow-hidden rounded-2xl border border-line bg-raised">
      <div className="flex items-center justify-between gap-2 border-b border-line px-3 py-2.5">
        <h2 className="text-sm font-semibold text-ink">Unusual activity</h2>
        <label className="sr-only" htmlFor="calls-tier">Evidence</label>
        <select
          id="calls-tier"
          value={tier}
          onChange={(e) => setTier(e.target.value as Tier)}
          className="rounded-md border border-line bg-inset px-2 py-1 text-xs text-ink-dim"
        >
          <option value="all">All evidence</option>
          <option value="model">Model or proven</option>
          <option value="proven">Proven only</option>
        </select>
      </div>
      {error ? (
        <EmptyState kind="error" title="Could not load the board" detail="The signal service did not answer." />
      ) : !data ? (
        <LoadingLines lines={6} label="Reading the board" />
      ) : rows.length === 0 ? (
        <EmptyState kind="empty" title="Nothing leans either way" detail="No stock has lopsided evidence at this tier." />
      ) : (
        <ul className="max-h-[640px] overflow-y-auto">
          {rows.map((r) => {
            const up = r.net > 0;
            const on = r.ticker === selected;
            return (
              <li key={r.ticker}>
                <button
                  type="button"
                  onClick={() => onSelect(r.ticker)}
                  aria-pressed={on}
                  className={`flex w-full items-center justify-between gap-3 border-b border-line/60 px-3 py-2.5 text-left transition-colors ${
                    on ? 'bg-accent-dim shadow-[inset_3px_0_0_var(--accent)]' : 'hover:bg-overlay/50'
                  }`}
                >
                  <span className="min-w-0">
                    <span className="block font-mono text-sm font-semibold text-ink">{r.ticker}</span>
                    <span className="block truncate text-micro text-ink-mute">{r.name ?? ''}</span>
                  </span>
                  <span className="shrink-0 text-right">
                    <span className={`block font-mono text-xs ${up ? 'text-positive' : 'text-negative'}`}>
                      {directionMark(r.net)} {formatNumber(Math.abs(r.net), { decimals: 0 })} of{' '}
                      {formatNumber(r.votes, { decimals: 0 })}
                    </span>
                    <span className="block text-micro text-ink-mute">{r.confidence ?? ''}</span>
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

export default function CallsWorkspace() {
  const [selected, setSelected] = useState<string | null>(null);
  const { data } = useSWR<{ board: BoardRow[] }>('/signals/matrix', fetcher, { refreshInterval: POLL.standard });
  const fallback = useMemo(() => rankBoard(data?.board ?? [])[0]?.ticker ?? null, [data]);
  const ticker = selected ?? fallback;
  return (
    <div className="grid grid-cols-1 items-start gap-4 xl:grid-cols-[minmax(240px,0.9fr)_minmax(0,1.8fr)_minmax(300px,1fr)]">
      <UnusualList selected={ticker} onSelect={setSelected} />
      <div className="min-w-0">
        <TradeIdeas />
      </div>
      <aside
        aria-label="Evidence"
        className="space-y-3 rounded-2xl border border-line-accent bg-raised p-4 xl:sticky xl:top-0"
      >
        {ticker ? (
          <>
            <div className="flex items-baseline justify-between gap-2">
              <h2 className="font-mono text-lead font-semibold text-ink">{ticker}</h2>
              <StockLink ticker={ticker} className="text-xs text-accent hover:underline">
                Open the stock
              </StockLink>
            </div>
            <TickerEvidence ticker={ticker} />
          </>
        ) : (
          <EmptyState kind="empty" title="Pick a stock" detail="Choose one from the list to see why it is moving." />
        )}
      </aside>
    </div>
  );
}
