'use client';

/**
 * The home page's personal row: the stocks this person follows, what today's
 * calls and evidence say about each, and their own town's surveillance record.
 * A reason to come back that is about them, not the market at large.
 */

import React, { useState } from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { formatNumber } from '../lib/format';
import { LABEL, type Recommendations } from '../lib/ideas';
import { toggleTicker, usePreferences, type MyTown } from '../lib/preferences';
import { ideaFor } from './TickerHero';
import { verdictClass, verdictSide } from './SignalBoard';
import { POLL } from './ui/DataProvider';
import { Skeleton } from './ui/Skeleton';

interface BoardRow {
  ticker: string;
  name: string | null;
  price: number | null;
  ret_1d: number | null;
  verdict?: string;
  confidence?: string;
}

interface AgencyCard {
  agency: string;
  state: string;
  city: string | null;
  intensity: number;
  technologies: { technology: string }[];
  vendors: string[];
}

/** How many followed stocks are on today's calls, in a sentence. */
export function callsLine(onCalls: number, followed: number): string {
  if (!followed) return 'Follow stocks from any stock page to see them here.';
  if (!onCalls)
    return followed === 1 ? "Your stock isn't on today's calls." : `None of your ${followed} stocks is on today's calls.`;
  return `${onCalls} of your ${followed} stock${followed === 1 ? ' is' : 's are'} on today's calls.`;
}

function AddStock({ onAdd }: { onAdd: (t: string) => void }) {
  const [v, setV] = useState('');
  return (
    <form
      className="flex gap-2"
      onSubmit={(e) => {
        e.preventDefault();
        const t = v.trim().toUpperCase();
        if (/^[A-Z]{1,5}(\.[A-Z])?$/.test(t)) onAdd(t);
        setV('');
      }}
    >
      <input
        value={v}
        onChange={(e) => setV(e.target.value)}
        placeholder="Add a ticker, e.g. NVDA"
        aria-label="Follow a stock"
        className="min-w-0 flex-1 rounded-md border border-line bg-page px-2.5 py-1.5 text-xs text-ink placeholder:text-ink-mute focus:border-accent outline-none"
      />
      <button type="submit" className="rounded-md border border-line px-2.5 py-1.5 text-xs text-accent hover:bg-overlay">
        Follow
      </button>
    </form>
  );
}

function MyStocks() {
  const { prefs, save } = usePreferences();
  const tickers = prefs?.tickers ?? [];
  const { data: rec } = useSWR<Recommendations>(tickers.length ? '/predictions/recommendations' : null, fetcher, {
    refreshInterval: POLL.standard,
  });
  const { data: board } = useSWR<{ board: BoardRow[] }>(tickers.length ? '/signals/matrix' : null, fetcher, {
    refreshInterval: POLL.standard,
  });
  const rows = tickers.map((t) => ({
    t,
    call: ideaFor(rec, t),
    row: board?.board.find((r) => r.ticker === t),
  }));
  const onCalls = rows.filter((r) => r.call).length;
  return (
    <section className="rounded-2xl border border-line bg-raised p-4 space-y-3">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="text-sm font-semibold text-ink">Your stocks</h2>
        <span className="text-micro text-ink-mute">{prefs ? callsLine(onCalls, tickers.length) : ''}</span>
      </div>
      {!prefs ? (
        <Skeleton height="6rem" />
      ) : (
        <>
          {rows.length > 0 && (
            <ul className="divide-y divide-line/60">
              {rows.map(({ t, call, row }) => (
                <li key={t} className="flex items-center justify-between gap-3 py-2">
                  <div className="min-w-0">
                    <Link href={`/ticker?symbol=${encodeURIComponent(t)}`} className="font-mono font-semibold text-ink hover:text-accent">
                      {t}
                    </Link>
                    <div className="truncate text-micro text-ink-mute">{row?.name ?? ''}</div>
                  </div>
                  <div className="flex shrink-0 items-center gap-2 text-micro">
                    {call ? (
                      <span
                        className={`rounded-full px-2 py-0.5 font-semibold ${
                          call.idea.direction > 0 ? 'bg-positive/15 text-positive' : 'bg-negative/15 text-negative'
                        }`}
                      >
                        {call.idea.direction > 0 ? 'BUY' : 'SHORT'} · {LABEL[call.horizon]?.split(' (')[0] ?? call.horizon}
                      </span>
                    ) : row?.verdict && verdictSide(row.verdict) ? (
                      <span className={`rounded-full border px-2 py-0.5 ${verdictClass(row.verdict, row.confidence)}`}>
                        {row.verdict.replace(/\s*\(.*\)\s*$/, '')}
                      </span>
                    ) : (
                      <span className="text-ink-mute">No call today</span>
                    )}
                    {row?.ret_1d != null && (
                      <span className={`font-mono ${row.ret_1d >= 0 ? 'text-positive' : 'text-negative'}`}>
                        {row.ret_1d >= 0 ? '+' : ''}
                        {formatNumber(row.ret_1d * 100, { decimals: 1 })}%
                      </span>
                    )}
                    <button
                      type="button"
                      onClick={() => void save({ tickers: toggleTicker(tickers, t) })}
                      aria-label={`Stop following ${t}`}
                      className="text-ink-mute hover:text-negative"
                    >
                      ×
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          )}
          <AddStock onAdd={(t) => void save({ tickers: toggleTicker(tickers.filter((x) => x !== t), t) })} />
        </>
      )}
    </section>
  );
}

function TownRecord({ town }: { town: MyTown }) {
  const { data, error } = useSWR<AgencyCard>(
    `/civic/agency?name=${encodeURIComponent(town.agency)}&state=${encodeURIComponent(town.state)}`,
    fetcher,
  );
  if (error) return <p className="text-xs text-ink-mute">That department&apos;s record could not be read.</p>;
  if (!data) return <Skeleton height="5rem" />;
  return (
    <div className="space-y-2">
      <div>
        <div className="text-sm text-ink">{data.agency}</div>
        <div className="text-micro text-ink-mute">{[data.city, data.state].filter(Boolean).join(', ')}</div>
      </div>
      <div className="flex flex-wrap gap-1.5">
        {data.technologies.length === 0 ? (
          <span className="text-micro text-ink-mute">No surveillance tools documented yet.</span>
        ) : (
          data.technologies.map((t) => (
            <span key={t.technology} className="rounded-full border border-civic/30 px-2 py-0.5 text-micro text-ink-dim">
              {t.technology}
            </span>
          ))
        )}
      </div>
      {data.vendors.length > 0 && (
        <p className="text-micro text-ink-mute">Supplied by {data.vendors.slice(0, 4).join(', ')}.</p>
      )}
    </div>
  );
}

function MyTownCard() {
  const { prefs } = usePreferences();
  const town = prefs?.town ?? null;
  return (
    <section className="rounded-2xl border border-civic/25 bg-raised p-4 space-y-3">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="text-sm font-semibold text-ink">Your town</h2>
        <Link href="/watch" className="text-micro text-civic hover:underline">
          {town ? 'Open the full record' : 'Find your town'}{' '}&rarr;
        </Link>
      </div>
      {!prefs ? (
        <Skeleton height="5rem" />
      ) : town ? (
        <TownRecord town={town} />
      ) : (
        <p className="text-xs text-ink-dim">
          Look up your police department on the surveillance map and choose &ldquo;Make this my town&rdquo;. Its tools,
          suppliers and any legal questions will show here.
        </p>
      )}
    </section>
  );
}

export function MyCorner() {
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <MyStocks />
      <MyTownCard />
    </div>
  );
}

export default MyCorner;
