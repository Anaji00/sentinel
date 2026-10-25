'use client';

/**
 * The home page's civic band: who is watching the public, who works with
 * immigration enforcement, and who is paid for it -- with the reader's own
 * town one search away. The civic work had a single card in the third row of
 * home while its strongest facts sat two tabs deep on /watch.
 */

import React, { useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatCompact, formatNumber } from '../lib/format';
import { TickerChip } from './ui/TickerChip';
import { LoadingLines } from './ui/Skeleton';

interface CivicSummary {
  totals: { devices: number; agencies: number };
}

interface LegalTotals {
  totals?: { vehicles?: number | string | null; portals?: number; ice_287g_agencies?: number };
}

export interface MoneyTotal {
  kind: string;
  matched: string;
  awards: number;
  total: number;
}

interface FeedItem {
  kind: string;
  at: string | null;
  title: string;
  url: string | null;
}

const FEED_WORD: Record<string, string> = {
  news: 'News',
  finding: 'Legal finding',
  portal_dropped: 'Dropped Flock',
  portal_new: 'New Flock page',
  drone_waiver: 'Drone permit',
  federal_award: 'Federal money',
  deployment: 'New deployment',
  court_case: 'Court case',
  lobbying: 'Lobbying',
};

/** A count the API may send as a string (it overflows a 32-bit int). */
export function asCount(v: number | string | null | undefined): number | null {
  if (v == null || v === '') return null;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

/** The surveillance vendors paid most in federal contracts, largest first,
 * each with its share of the largest for the bar beside it. */
export function topVendors(totals: MoneyTotal[] | undefined, n = 4): (MoneyTotal & { share: number })[] {
  const contracts = (totals ?? []).filter((t) => t.kind === 'contract' && t.total > 0).sort((a, b) => b.total - a.total);
  const max = contracts[0]?.total ?? 0;
  return contracts.slice(0, n).map((t) => ({ ...t, share: max ? t.total / max : 0 }));
}

function Fact({ value, label, href }: { value: string; label: string; href: string }) {
  return (
    <Link
      href={href}
      className="group rounded-xl border border-civic/20 bg-page/40 px-4 py-3 hover:border-civic/50"
    >
      <div className="font-mono text-lead leading-none text-ink group-hover:text-civic">{value}</div>
      <div className="mt-1.5 text-micro leading-snug text-ink-dim">{label}</div>
    </Link>
  );
}

export function WhoIsWatching() {
  const router = useRouter();
  const [town, setTown] = useState('');
  const { data: summary } = useSWR<CivicSummary>('/civic/summary', fetcher);
  const { data: legal } = useSWR<LegalTotals>('/civic/legal?limit=1', fetcher);
  const { data: money } = useSWR<{ totals: MoneyTotal[] }>('/civic/money?limit=1', fetcher);
  const { data: feed } = useSWR<{ items: FeedItem[] }>('/civic/feed?days=7', fetcher);

  const vehicles = asCount(legal?.totals?.vehicles);
  const vendors = topVendors(money?.totals);
  const vendorTotal = (money?.totals ?? []).filter((t) => t.kind === 'contract').reduce((s, t) => s + t.total, 0);
  const latest = (feed?.items ?? []).slice(0, 4);

  const go = (e: React.FormEvent) => {
    e.preventDefault();
    const q = town.trim();
    router.push(q ? `/watch?town=${encodeURIComponent(q)}` : '/watch');
  };

  return (
    <section
      aria-labelledby="who-is-watching"
      className="rounded-2xl border border-civic/25 bg-gradient-to-br from-civic-dim via-raised to-raised p-4 sm:p-5 space-y-5"
    >
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="max-w-2xl space-y-1">
          <div className="text-micro font-semibold uppercase tracking-wider text-civic">Watching the watchers</div>
          <h2 id="who-is-watching" className="text-head font-semibold text-ink">
            Who&apos;s watching you
          </h2>
          <p className="text-sm text-ink-dim">
            Police plate readers, gunshot microphones, face recognition and the companies paid to run them, from public
            records. See what watches your town, whether it follows the law, and what you can do about it.
          </p>
        </div>
        <form onSubmit={go} className="flex w-full max-w-sm gap-2" role="search">
          <input
            type="search"
            value={town}
            onChange={(e) => setTown(e.target.value)}
            placeholder="Your town or police department"
            aria-label="Look up your town"
            className="min-w-0 flex-1 rounded-lg border border-line-strong bg-page px-3 py-2 text-sm text-ink placeholder:text-ink-mute focus:border-civic outline-none"
          />
          <button type="submit" className="rounded-lg bg-civic px-3 py-2 text-sm font-semibold text-page hover:opacity-90">
            Look up
          </button>
        </form>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
        <Fact
          value={vehicles != null ? formatCompact(vehicles, { decimals: 0 }) : ABSENT}
          label={`cars logged by plate readers in 30 days, counted only from the ${
            legal?.totals?.portals != null ? formatNumber(legal.totals.portals, { decimals: 0 }) : ''
          } police departments that publish the figure`}
          href="/watch#accountability"
        />
        <Fact
          value={summary ? formatNumber(summary.totals.devices, { decimals: 0 }) : ABSENT}
          label="plate readers and gunshot microphones mapped by volunteers"
          href="/watch"
        />
        <Fact
          value={legal?.totals?.ice_287g_agencies != null ? formatNumber(legal.totals.ice_287g_agencies, { decimals: 0 }) : ABSENT}
          label="local agencies signed up to help immigration enforcement (287(g))"
          href="/watch#accountability"
        />
        <Fact
          value={vendorTotal ? `$${formatCompact(vendorTotal, { decimals: 1 })}` : ABSENT}
          label="in federal contracts to surveillance companies"
          href="/watch#money"
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <div className="space-y-2">
          <h3 className="text-sm font-semibold text-ink">Who profits</h3>
          {vendors.length === 0 ? (
            <LoadingLines />
          ) : (
            <ul className="space-y-1.5">
              {vendors.map((v) => (
                <li key={v.matched} className="space-y-0.5">
                  <div className="flex items-baseline justify-between gap-2 text-xs">
                    <span className="text-ink">
                      {v.matched}
                      <TickerChip vendor={v.matched} />
                    </span>
                    <span className="font-mono text-ink-dim">
                      ${formatCompact(v.total, { decimals: 1 })}{' '}
                      <span className="text-ink-mute">· {formatNumber(v.awards, { decimals: 0 })} contracts</span>
                    </span>
                  </div>
                  <div className="h-1.5 rounded-full bg-inset overflow-hidden">
                    <div className="h-full rounded-full bg-civic/70" style={{ width: `${Math.max(2, v.share * 100)}%` }} />
                  </div>
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="space-y-2">
          <h3 className="text-sm font-semibold text-ink">This week</h3>
          {latest.length === 0 ? (
            <p className="text-micro text-ink-mute">{feed ? 'A quiet week.' : 'Reading…'}</p>
          ) : (
            <ul className="space-y-1.5">
              {latest.map((i, n) => (
                <li key={i.url ?? n} className="text-xs leading-snug">
                  <span className="text-micro text-civic">{FEED_WORD[i.kind] ?? i.kind}</span>{' '}
                  {i.url ? (
                    <a href={i.url} target="_blank" rel="noopener noreferrer" className="text-ink-dim hover:text-ink">
                      {i.title}
                    </a>
                  ) : (
                    <span className="text-ink-dim">{i.title}</span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>

      <nav aria-label="Civic watch" className="flex flex-wrap gap-2">
        {(
          [
            ['/watch', 'Map of cameras and agencies'],
            ['/watch#accountability', 'Possible rule-breaking'],
            ['/watch#money', 'Follow the money'],
            ['/watch#protect', 'Protect yourself'],
          ] as const
        ).map(([href, label]) => (
          <Link
            key={href}
            href={href}
            className="rounded-full border border-civic/30 px-3 py-1 text-xs text-ink-dim hover:border-civic hover:text-ink"
          >
            {label}
          </Link>
        ))}
      </nav>
    </section>
  );
}

export default WhoIsWatching;
