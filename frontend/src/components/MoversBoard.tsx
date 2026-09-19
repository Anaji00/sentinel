'use client';

/**
 * The gainers and losers board, with the two joins that make it not a screener.
 *
 * Everything below this component has existed for a while and had no reader.
 * The radar sweep sees every symbol in the US equity universe every poll and
 * binds today's bar and yesterday's bar for each of them; a sorted set scored
 * by the day's move answers both directions off one structure; a per-ticker
 * standing carries the week, the month and the distance from the 50- and
 * 200-day. `/radar/movers` has served all of it, and nothing in this app
 * fetched it -- the string `/radar/movers` did not appear anywhere in
 * frontend/src.
 *
 * A ranked list of percentages is a commodity. What is not: this platform
 * holds the news and the sector for every name on that list. Both joins are
 * rendered here, and both render their own absence -- `news_events: null`
 * means the join could not run, which a reader acts on differently from zero.
 */

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { POLL } from './ui/DataProvider';

interface Mover {
  ticker: string;
  day_pct: number;
  gap_pct?: number | null;
  intraday_pct?: number | null;
  prev_close?: number | null;
  last_price?: number | null;
  change_pct_week?: number | null;
  dist_sma_50_pct?: number | null;
  dist_sma_200_pct?: number | null;
  ma_alignment?: string | null;
  sector?: string | null;
  news_events?: number | null;
  /** null when the size is not yet resolved, never 0 for "small". */
  market_cap_usd?: number | null;
}

interface MoversResponse {
  direction: 'gainers' | 'losers';
  count: number;
  movers: Mover[];
  as_of: string | null;
  news_window_hours?: number;
  /** null means the news join could not run at all -- not "no news". */
  movers_without_news_in_window?: string[] | null;
  sector_concentration?: Record<string, number>;
  /** The floor applied, in USD. 0 means the raw board. */
  min_market_cap_usd?: number;
  /** How far down the ranking the gate had to look. */
  candidates_walked?: number;
}

const pct = (v: number | null | undefined, digits = 2): string =>
  typeof v === 'number' && Number.isFinite(v) ? `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%` : '—';

const toneFor = (v: number | null | undefined): string => {
  if (typeof v !== 'number' || !Number.isFinite(v)) return 'text-ink-mute';
  if (v > 0) return 'text-emerald-400';
  if (v < 0) return 'text-rose-400';
  return 'text-ink-dim';
};

export default function MoversBoard() {
  const [direction, setDirection] = useState<'gainers' | 'losers'>('gainers');
  const { data, error, isLoading } = useSWR<MoversResponse>(
    `/api/v1/radar/movers?direction=${direction}&limit=20`,
    fetcher,
    { refreshInterval: POLL.slow },
  );

  const unexplained = useMemo(
    () => new Set(data?.movers_without_news_in_window ?? []),
    [data?.movers_without_news_in_window],
  );

  // Null and empty are different answers and the header says which it got.
  const newsJoinRan = data?.movers_without_news_in_window != null;

  const sectors = useMemo(() => {
    const entries = Object.entries(data?.sector_concentration ?? {});
    return entries.sort((a, b) => b[1] - a[1]).slice(0, 4);
  }, [data?.sector_concentration]);

  return (
    <div className="h-full flex flex-col bg-raised rounded-xl border border-cyan-500/20 overflow-hidden">
      <div className="flex items-center justify-between px-3.5 py-2 border-b border-cyan-500/15 shrink-0">
        <div className="flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-accent animate-pulse" />
          <span className="text-accent font-extrabold tracking-wider uppercase text-micro">
            Movers
          </span>
          {data?.as_of && (
            <span className="text-micro text-ink-mute">swept {data.as_of.slice(11, 19)}Z</span>
          )}
        </div>
        <div className="flex gap-1">
          {(['gainers', 'losers'] as const).map((d) => (
            <button
              key={d}
              onClick={() => setDirection(d)}
              className={`px-2 py-0.5 rounded text-micro uppercase tracking-wide transition-colors ${
                direction === d
                  ? 'bg-cyan-500/20 text-cyan-300 border border-cyan-400/40'
                  : 'text-ink-mute border border-transparent hover:text-ink-dim'
              }`}
            >
              {d}
            </button>
          ))}
        </div>
      </div>

      {sectors.length > 0 && (
        <div className="px-3.5 py-1.5 border-b border-cyan-500/10 flex flex-wrap gap-2 text-micro shrink-0">
          <span className="text-ink-mute uppercase tracking-wide">Concentration</span>
          {sectors.map(([sector, n]) => (
            <span key={sector} className="text-ink-dim">
              {sector} <span className="text-cyan-400 font-bold">{n}</span>
            </span>
          ))}
        </div>
      )}

      <div className="flex-1 overflow-y-auto">
        {isLoading && <div className="p-4 text-micro text-ink-mute">Reading the sweep…</div>}
        {error && (
          <div className="p-4 text-micro text-rose-400">
            Movers store unavailable. The radar publishes this on each sweep; an empty board before
            the open is a different answer and renders as one.
          </div>
        )}
        {data && data.movers.length === 0 && (
          <div className="p-4 text-micro text-ink-mute">
            {data.min_market_cap_usd
              ? `No company above $${(data.min_market_cap_usd / 1e9).toFixed(1)}B has moved
                 enough to rank${data.candidates_walked ? `, across ${data.candidates_walked}
                 candidates checked` : ''}. Ungated, this board is warrants and sub-dollar
                 tickers whose previous close was a rounding error.`
              : 'No moves on the board. Before the open this is the correct answer, not a failure.'}
          </div>
        )}

        {data?.movers.map((m) => (
          <div
            key={m.ticker}
            className="px-3.5 py-2 border-b border-white/5 hover:bg-white/[0.02] transition-colors"
          >
            <div className="flex items-baseline justify-between gap-2">
              <div className="flex items-baseline gap-2 min-w-0">
                <span className="text-ink font-bold text-xs">{m.ticker}</span>
                {m.market_cap_usd != null && (
                  <span className="text-micro text-ink-mute">
                    ${(m.market_cap_usd / 1e9).toFixed(1)}B
                  </span>
                )}
                {m.sector && m.sector !== 'UNKNOWN' && (
                  <span className="text-micro text-ink-mute truncate">{m.sector}</span>
                )}
                {newsJoinRan && unexplained.has(m.ticker) && (
                  <span
                    className="text-micro uppercase tracking-wider text-amber-400 border border-amber-500/30 rounded px-1"
                    title="No news event named this ticker in the window this platform collected. Not a claim about the world."
                  >
                    no news collected
                  </span>
                )}
              </div>
              <span className={`text-sm font-bold tabular-nums ${toneFor(m.day_pct)}`}>
                {pct(m.day_pct)}
              </span>
            </div>

            <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-micro text-ink-mute tabular-nums">
              <span>
                gap <span className={toneFor(m.gap_pct)}>{pct(m.gap_pct)}</span>
              </span>
              <span>
                intraday <span className={toneFor(m.intraday_pct)}>{pct(m.intraday_pct)}</span>
              </span>
              <span>
                week <span className={toneFor(m.change_pct_week)}>{pct(m.change_pct_week)}</span>
              </span>
              <span>
                vs 50d{' '}
                <span className={toneFor(m.dist_sma_50_pct)}>{pct(m.dist_sma_50_pct, 1)}</span>
              </span>
              <span>
                vs 200d{' '}
                <span className={toneFor(m.dist_sma_200_pct)}>{pct(m.dist_sma_200_pct, 1)}</span>
              </span>
              {m.ma_alignment && <span className="text-ink-mute">{m.ma_alignment}</span>}
              <span>
                news{''}
                <span className={m.news_events == null ? 'text-ink-mute' : 'text-ink-dim'}>
                  {m.news_events == null ? 'not joined' : m.news_events}
                </span>
              </span>
            </div>
          </div>
        ))}
      </div>

      <div className="px-3.5 py-1.5 border-t border-cyan-500/10 text-micro text-ink-mute shrink-0">
        {newsJoinRan
          ? `News join over ${data?.news_window_hours ?? 24}h of collected events.`
          : 'News join did not run — counts unavailable, which is not the same as no news.'}
      </div>
    </div>
  );
}
