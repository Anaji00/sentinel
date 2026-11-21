'use client';

/**
 * Today: the home page's first screen.
 *
 * The live console below it answers "is every feed flowing?". This answers
 * what a person opens the app for: how the model portfolio is doing against
 * the S&P 500, which stocks have the strongest evidence today, and who is
 * watching the public -- each a click from its full page.
 */

import React, { useSyncExternalStore } from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { failedWithout } from '../lib/dataHealth';
import { PanelError } from './ui/PanelError';
import { fetcher } from '../lib/api';
import { ABSENT, formatNumber } from '../lib/format';
import { TopCalls } from './TopCalls';
import { WhoIsWatching } from './WhoIsWatching';
import { usePreferences } from '../lib/preferences';
import { furthestBehind } from '../lib/stockRecord';
import { barLabel } from '../lib/candles';
import { LoadingLines } from './ui/Skeleton';

interface PortfolioData {
  summary: { sessions: number; return_pct?: number; spy_return_pct?: number; excess_pct?: number };
  nav: { day: string; nav: number; spy_nav: number }[];
  holdings: { long: { ticker: string }[]; short: { ticker: string }[] };
  state?: { next_rebalance_in_sessions: number | null };
}

interface BoardRow {
  ticker: string;
  name: string | null;
  verdict?: string;
  confidence?: 'higher' | 'moderate' | 'low' | 'none';
  lean: number;
  ret_1d: number | null;
}

/** "+1.23%" / "-0.40%"; absent when the figure is. */
export function signedPct(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return ABSENT;
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
}

/** A greeting for the reader's own time of day. */
export function greeting(hour: number): string {
  if (hour < 5) return 'Working late';
  if (hour < 12) return 'Good morning';
  if (hour < 18) return 'Good afternoon';
  return 'Good evening';
}

/** Points for a two-line sparkline (portfolio vs S&P), scaled to one box. */
export function sparkPoints(
  values: number[],
  lo: number,
  hi: number,
  w: number,
  h: number,
): string {
  const span = hi - lo || 1;
  return values
    .map(
      (v, i) =>
        `${((i / Math.max(1, values.length - 1)) * w).toFixed(1)},${(h - ((v - lo) / span) * h).toFixed(1)}`,
    )
    .join(' ');
}

const noSubscribe = () => () => {};
const clientDay = () => {
  const d = new Date();
  return `${greeting(d.getHours())}|${d.toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric' })}`;
};
const serverDay = () => 'Welcome back|';

/** The paper book against the S&P 500: one large figure, one chart with its lines named at their ends, one honest line of fine print. */
function BookLead() {
  const { data, error, mutate } = useSWR<PortfolioData>('/predictions/model-portfolio', fetcher);
  const s = data?.summary;
  const nav = data?.nav ?? [];
  const all = nav.flatMap((n) => [n.nav, n.spy_nav]);
  const lo = all.length ? Math.min(...all) : 0;
  const hi = all.length ? Math.max(...all) : 1;
  const excess = s?.excess_pct;
  const ahead = (excess ?? 0) >= 0;
  const W = 640;
  const PLOT = 520;
  const H = 120;
  const y = (v: number) => H - 6 - ((v - lo) / (hi - lo || 1)) * (H - 12);
  const pts = (key: 'nav' | 'spy_nav') => nav.map((n, i) => `${((i / Math.max(1, nav.length - 1)) * PLOT).toFixed(1)},${y(n[key]).toFixed(1)}`).join(' ');
  const sessions = s?.sessions ?? 0;
  const worst = furthestBehind(nav);
  const worstX = worst ? (worst.index / Math.max(1, nav.length - 1)) * PLOT : 0;
  return (
    <section aria-label="The paper book against the S&P 500" className="space-y-3">
      <p className="text-micro text-ink-mute">Model portfolio, practice money</p>
      {!data ? (
        failedWithout(error, data) ? (
          <PanelError inline what="the model portfolio" onRetry={() => void mutate()} />
        ) : (
          <LoadingLines />
        )
      ) : (
        <>
          <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
            <span className={`font-mono text-hero font-semibold leading-none tabular-nums ${ahead ? 'text-positive' : 'text-negative'}`}>
              {signedPct(excess)}
            </span>
            <span className="text-head leading-snug text-ink">{ahead ? 'ahead of' : 'behind'}{' '}the S&amp;P 500 since it began</span>
          </div>
          {nav.length > 1 && (
            <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`Portfolio ${signedPct(s?.return_pct)} and S&P 500 ${signedPct(s?.spy_return_pct)} since launch`} className="w-full max-w-3xl">
              <polyline points={pts('spy_nav')} fill="none" stroke="var(--text-muted)" strokeWidth="1.25" strokeDasharray="4 3" />
              <polyline points={pts('nav')} fill="none" stroke="var(--text-primary)" strokeWidth="1.75" />
              {worst && (
                <g>
                  <line x1={worstX} x2={worstX} y1={y(nav[worst.index].nav) + 4} y2={H - 2} stroke="var(--border-strong)" strokeDasharray="1 3" />
                  <circle cx={worstX} cy={y(nav[worst.index].nav)} r="3" fill="var(--text-primary)" stroke="var(--bg-base)" strokeWidth="1" />
                  <text x={Math.min(worstX, PLOT - 130)} y={H - 4} fontSize="11" className="fill-ink-mute">
                    Furthest behind: {Math.abs(worst.gapPct).toFixed(1)} points on {barLabel(`${worst.day.slice(0, 10)}T00:00:00Z`, '1d')}
                  </text>
                </g>
              )}
              <text x={PLOT + 8} y={Math.min(H - 4, Math.max(10, y(nav[nav.length - 1].nav) + (y(nav[nav.length - 1].nav) < y(nav[nav.length - 1].spy_nav) ? -4 : 12)))} fontSize="12" className="fill-ink">
                Book {signedPct(s?.return_pct)}
              </text>
              <text x={PLOT + 8} y={Math.min(H - 4, Math.max(10, y(nav[nav.length - 1].spy_nav) + (y(nav[nav.length - 1].spy_nav) < y(nav[nav.length - 1].nav) ? -4 : 12)))} fontSize="12" className="fill-ink-mute">
                S&amp;P 500 {signedPct(s?.spy_return_pct)}
              </text>
            </svg>
          )}
          <p className="max-w-prose text-micro text-ink-mute">
            {formatNumber(sessions, { decimals: 0 })} trading days in
            {sessions < 60 ? '. Too early to tell skill from luck.' : '.'}{' '}
            {data.holdings.long.length}{' '}buys and {data.holdings.short.length}{' '}bets against, on top of an S&amp;P 500 core
            {data.state?.next_rebalance_in_sessions != null ? `; next rebalance in ${data.state.next_rebalance_in_sessions} days` : ''}.{' '}
            <Link href="/performance" className="text-accent hover:underline">
              See every holding and trade
            </Link>
          </p>
        </>
      )}
    </section>
  );
}

/** Stocks with the most agreement across the evidence today: a plain ruled list, direction in words and an arrow. */
function EvidenceLedger() {
  const { data, error, mutate } = useSWR<{ board: BoardRow[] }>('/signals/matrix', fetcher);
  const rows = (data?.board ?? [])
    .filter((r) => r.confidence === 'higher' || r.confidence === 'moderate')
    .sort((a, b) => Math.abs(b.lean) - Math.abs(a.lean))
    .slice(0, 6);
  return (
    <section aria-label="Strongest evidence today" className="space-y-2 border-t border-line pt-4">
      <div className="flex items-baseline justify-between gap-3">
        <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Strongest evidence today</h2>
        <Link href="/calls" className="text-xs text-accent hover:underline">
          Open the signal board
        </Link>
      </div>
      {!data ? (
        failedWithout(error, data) ? (
          <PanelError inline what="today's evidence" onRetry={() => void mutate()} />
        ) : (
          <LoadingLines />
        )
      ) : rows.length === 0 ? (
        <p className="text-xs text-ink-mute">Nothing with enough evidence today. Quiet days are normal.</p>
      ) : (
        <ul>
          {rows.map((r) => (
            <li key={r.ticker} className="flex items-baseline justify-between gap-4 border-t border-line/60 py-1.5 text-xs first:border-t-0">
              <Link href={`?t=${encodeURIComponent(r.ticker)}`} className="min-w-0 truncate hover:text-accent">
                <span className="symbol text-ink">{r.ticker}</span> <span className="text-ink-mute">{r.name}</span>
              </Link>
              <span className={`shrink-0 font-mono ${r.lean >= 0 ? 'text-positive' : 'text-negative'}`}>{r.lean >= 0 ? '▲ leans up' : '▼ leans down'}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

const SHORTCUTS: [string, string, string][] = [
  ['/watch', 'Look up your town', 'Cameras, agencies and the rules they follow'],
  ['/calls', 'Look up a stock', 'Prices, fund holders and every signal'],
  ['/calls?view=signals', 'Trade signals', 'Only patterns proven on years of data'],
  ['/watch#protect', 'Protect yourself', 'A privacy plan in two minutes'],
];

import ChangesDigest from './ChangesDigest';
import WatchStrip from './WatchStrip';

export function TodayOverview() {
  // The reader's clock, read on the client only: the server renders the
  // neutral fallback and hydration swaps in the local greeting and date.
  const [hello, today] = useSyncExternalStore(noSubscribe, clientDay, serverDay).split('|');
  const focus = usePreferences().prefs?.focus ?? 'both';
  return (
    <div className="space-y-4">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-display font-semibold leading-tight text-ink">{hello}</h1>
          <p className="text-sm text-ink-mute">
            {today ? `${today} · ` : ''}Here&apos;s what matters today.
          </p>
        </div>
        <nav aria-label="Shortcuts" className="flex flex-wrap gap-2">
          {SHORTCUTS.map(([href, label, hint]) => (
            <Link
              key={href}
              href={href}
              title={hint}
              className="rounded-full border border-line bg-raised px-3 py-1.5 text-xs text-ink-dim hover:border-line-accent hover:text-ink"
            >
              {label}
            </Link>
          ))}
        </nav>
      </header>
      <div className="grid gap-x-12 gap-y-8 lg:grid-cols-[minmax(0,8fr)_minmax(18rem,4fr)]">
        <div className="min-w-0 space-y-8">
          <BookLead />
          <WatchStrip />
        </div>
        <aside className="min-w-0 lg:border-l lg:border-line lg:pl-8">
          <ChangesDigest />
        </aside>
      </div>
      {/* What leads follows what this person said they came for (the welcome's
          choice, saved on the account); "both" keeps the two halves at equal
          weight, stocks first. */}
      {focus === 'civic' ? (
        <>
          <WhoIsWatching />
          <TopCalls />
        </>
      ) : (
        <>
          <TopCalls />
          <WhoIsWatching />
        </>
      )}
      <EvidenceLedger />
    </div>
  );
}

export default TodayOverview;
