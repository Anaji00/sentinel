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
import { fetcher } from '../lib/api';
import { ABSENT, formatNumber } from '../lib/format';
import { PALETTE } from '../lib/palette';
import { TopCalls } from './TopCalls';
import { WhoIsWatching } from './WhoIsWatching';
import { MyCorner } from './MyCorner';
import { Welcome } from './Welcome';
import { usePreferences } from '../lib/preferences';
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

function Card({
  title,
  href,
  cta,
  children,
}: {
  title: string;
  href: string;
  cta: string;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-2xl border border-line bg-raised p-4 flex flex-col gap-3 min-h-[15rem]">
      <h2 className="text-sm font-semibold text-ink">{title}</h2>
      <div className="flex-1 min-h-0">{children}</div>
      <Link href={href} className="text-xs text-accent hover:underline self-start">
        {cta}{' '}&rarr;
      </Link>
    </section>
  );
}

function PortfolioCard() {
  const { data } = useSWR<PortfolioData>('/predictions/model-portfolio', fetcher);
  const s = data?.summary;
  const nav = data?.nav ?? [];
  const all = nav.flatMap((n) => [n.nav, n.spy_nav]);
  const lo = all.length ? Math.min(...all) : 0;
  const hi = all.length ? Math.max(...all) : 1;
  const ahead = (s?.excess_pct ?? 0) >= 0;
  return (
    <Card
      title="Your model portfolio vs the S&P 500"
      href="/performance"
      cta="See every holding and trade"
    >
      {!data ? (
        <LoadingLines />
      ) : (
        <div className="space-y-3">
          <div className="flex items-end gap-6">
            <div>
              <div
                className={`text-lead font-semibold font-mono ${ahead ? 'text-positive' : 'text-negative'}`}
              >
                {signedPct(s?.excess_pct)}
              </div>
              <div className="text-micro text-ink-mute">
                {ahead ? 'ahead of' : 'behind'} the index so far
              </div>
            </div>
            <div className="text-xs text-ink-dim space-y-0.5">
              <div>
                Portfolio <span className="font-mono text-ink">{signedPct(s?.return_pct)}</span>
              </div>
              <div>
                S&amp;P 500{' '}
                <span className="font-mono text-ink">{signedPct(s?.spy_return_pct)}</span>
              </div>
            </div>
          </div>
          {nav.length > 1 && (
            <svg
              viewBox="0 0 300 60"
              className="w-full h-16"
              preserveAspectRatio="none"
              aria-label="Portfolio and S&P 500 since launch"
            >
              <polyline
                points={sparkPoints(
                  nav.map((n) => n.spy_nav),
                  lo,
                  hi,
                  300,
                  60,
                )}
                fill="none"
                stroke={PALETTE.textMuted}
                strokeWidth="1.5"
              />
              <polyline
                points={sparkPoints(
                  nav.map((n) => n.nav),
                  lo,
                  hi,
                  300,
                  60,
                )}
                fill="none"
                stroke={PALETTE.accent}
                strokeWidth="2"
              />
            </svg>
          )}
          <p className="text-micro text-ink-mute">
            {formatNumber(s?.sessions ?? 0, { decimals: 0 })} trading days in ·{' '}
            {data.holdings.long.length} buys and {data.holdings.short.length}{' '}sells on top of an
            S&amp;P 500 core
            {data.state?.next_rebalance_in_sessions != null
              ? ` · next rebalance in ${data.state.next_rebalance_in_sessions} days`
              : ''}
          </p>
        </div>
      )}
    </Card>
  );
}

function SignalsCard() {
  const { data } = useSWR<{ board: BoardRow[] }>('/signals/matrix', fetcher);
  const rows = (data?.board ?? [])
    .filter((r) => r.confidence === 'higher' || r.confidence === 'moderate')
    .sort((a, b) => Math.abs(b.lean) - Math.abs(a.lean))
    .slice(0, 6);
  return (
    <Card title="Strongest evidence today" href="/calls" cta="Open the signal board">
      {!data ? (
        <LoadingLines />
      ) : rows.length === 0 ? (
        <p className="text-xs text-ink-mute">
          Nothing with enough evidence today. Quiet days are normal.
        </p>
      ) : (
        <ul className="space-y-1.5">
          {rows.map((r) => (
            <li key={r.ticker} className="flex items-center justify-between gap-3 text-xs">
              <Link
                href={`/ticker?symbol=${encodeURIComponent(r.ticker)}`}
                className="min-w-0 hover:text-accent"
              >
                <span className="symbol text-ink">{r.ticker}</span>{' '}
                <span className="text-ink-mute truncate">{r.name}</span>
              </Link>
              <span
                className={`shrink-0 rounded-full px-2 py-0.5 text-micro ${r.lean >= 0 ? 'bg-positive/10 text-positive' : 'bg-negative/10 text-negative'}`}
              >
                {r.lean >= 0 ? 'Leans up' : 'Leans down'}
              </span>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

const SHORTCUTS: [string, string, string][] = [
  ['/watch', 'Look up your town', 'Cameras, agencies and the rules they follow'],
  ['/ticker', 'Look up a stock', 'Prices, fund holders and every signal'],
  ['/calls?view=signals', 'Trade signals', 'Only patterns proven on years of data'],
  ['/watch#protect', 'Protect yourself', 'A privacy plan in two minutes'],
];

export function TodayOverview() {
  // The reader's clock, read on the client only: the server renders the
  // neutral fallback and hydration swaps in the local greeting and date.
  const [hello, today] = useSyncExternalStore(noSubscribe, clientDay, serverDay).split('|');
  const focus = usePreferences().prefs?.focus ?? 'both';
  return (
    <div className="space-y-4">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lead font-semibold text-ink">{hello}</h1>
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
      <Welcome />
      {/* What leads follows what this person said they came for (the welcome's
          choice, saved on the account); "both" keeps the two halves at equal
          weight, stocks first. */}
      {focus === 'civic' ? (
        <>
          <WhoIsWatching />
          <MyCorner />
          <TopCalls />
        </>
      ) : (
        <>
          <TopCalls />
          <MyCorner />
          <WhoIsWatching />
        </>
      )}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <PortfolioCard />
        <SignalsCard />
      </div>
    </div>
  );
}

export default TodayOverview;
