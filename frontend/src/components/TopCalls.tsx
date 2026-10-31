'use client';

/**
 * The home page's hero: the day's strongest calls as cards, the market's
 * backdrop in one line, and the way into the full board.
 */

import React from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatNumber } from '../lib/format';
import { LABEL, pickTopCalls, type Recommendations } from '../lib/ideas';
import { IdeaCard } from './IdeaCard';
import { POLL } from './ui/DataProvider';

const REGIME_WORD: Record<string, string> = {
  calm: 'Calm market',
  stressed: 'Stressed market',
  panic: 'Panic',
};

function Chip({
  children,
  tone = 'neutral',
}: {
  children: React.ReactNode;
  tone?: 'neutral' | 'good' | 'bad' | 'warn';
}) {
  const cls = {
    neutral: 'border-line text-ink-dim',
    good: 'border-positive/30 text-positive',
    bad: 'border-negative/30 text-negative',
    warn: 'border-caution/30 text-caution',
  }[tone];
  return <span className={`rounded-full border px-2.5 py-1 text-micro ${cls}`}>{children}</span>;
}

export function TopCalls({ count = 4 }: { count?: number }) {
  const { data, error } = useSWR<Recommendations>('/predictions/recommendations', fetcher, {
    refreshInterval: POLL.standard,
  });
  const calls = pickTopCalls(data, count);
  const p = data?.pulse;
  return (
    <section aria-labelledby="top-calls" className="space-y-3">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 id="top-calls" className="text-head font-semibold text-ink">
            Today&apos;s top calls
          </h2>
          <p className="text-xs text-ink-mute">
            The stocks our model most wants to buy, and to bet against, right now. Tap one to see
            why.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          {p?.spy_change_pct != null && (
            <Chip tone={p.spy_change_pct >= 0 ? 'good' : 'bad'}>
              S&amp;P 500 {p.spy_change_pct >= 0 ? '+' : ''}
              {formatNumber(p.spy_change_pct, { decimals: 2 })}% today
            </Chip>
          )}
          {p?.regime && (
            <Chip tone={p.regime === 'calm' ? 'neutral' : 'warn'}>
              {REGIME_WORD[p.regime] ?? p.regime}
            </Chip>
          )}
          {p && (
            <Chip tone={p.sizing_scale < 0.95 ? 'warn' : 'neutral'}>
              {p.sizing_scale < 0.95
                ? `Bets cut to ${formatNumber(p.sizing_scale * 100, { decimals: 0 })}%: markets are jumpy`
                : 'Normal bet sizes'}
            </Chip>
          )}
          <Link
            href="/calls"
            className="rounded-full bg-accent px-3 py-1 text-micro font-semibold text-page hover:opacity-90"
          >
            See all calls &rarr;
          </Link>
        </div>
      </div>
      {error ? (
        <p className="text-xs text-ink-mute">Calls could not be read.</p>
      ) : !data ? (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {Array.from({ length: count }, (_, n) => (
            <div key={n} className="h-56 rounded-2xl border border-line bg-inset animate-pulse" />
          ))}
        </div>
      ) : calls.length === 0 ? (
        <p className="rounded-2xl border border-dashed border-line p-4 text-xs text-ink-mute">
          {data.note ?? 'No calls yet: the model runs each evening after the close.'}
        </p>
      ) : (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {calls.map((c) => (
            <IdeaCard
              key={`${c.idea.ticker}-${c.horizon}`}
              idea={c.idea}
              days={c.days}
              validated={c.validated}
              horizonLabel={LABEL[c.horizon]?.split(' (')[0] ?? c.horizon}
              compact
            />
          ))}
        </div>
      )}
      {data?.as_of && (
        <p className="text-micro text-ink-mute">
          From the model as of {data.as_of ?? ABSENT}. Not financial advice.
        </p>
      )}
    </section>
  );
}

export default TopCalls;
