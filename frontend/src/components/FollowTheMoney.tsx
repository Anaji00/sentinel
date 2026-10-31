'use client';

/**
 * Follow the money: who owns the companies, whether rivals answer to the same
 * owners, which companies live on one customer, and what public money and
 * lobbying flow to the surveillance industry.
 *
 * Written for someone who has never read a 13F. Every number is one the
 * gateway measured; every section says where it comes from and what it
 * leaves out, because the point is a wake-up call that survives a fact check.
 */

import React, { useState } from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { formatCurrency, formatNumber } from '../lib/format';
import {
  BIG_THREE_IDS,
  ownershipNetwork,
  pctWidth,
  shortName,
  type CrossHolding,
  type HoldersResponse,
  type Interlock,
  type OwnershipOverview,
  type SupplyGroup,
} from '../lib/money';
import { POLL } from './ui/DataProvider';
import { EmptyState } from './ui/EmptyState';
import { LoadingLines } from './ui/Skeleton';

interface CivicMoney {
  totals: { kind: string; matched: string; awards: number; total: number }[];
}
interface Lobbying {
  by_vendor: { vendor: string; total: number }[];
}

const pct1 = (v: number | null | undefined) => (v == null ? '—' : `${formatNumber(v, { decimals: 1 })}%`);

function Section({
  id,
  kicker,
  title,
  children,
  source,
}: {
  id: string;
  kicker: string;
  title: string;
  children: React.ReactNode;
  source: string;
}) {
  return (
    <section id={id} aria-labelledby={`${id}-h`} className="space-y-5 border-t border-line pt-10">
      <div className="space-y-1.5">
        <span className="text-micro font-semibold uppercase tracking-[0.14em] text-civic">{kicker}</span>
        <h2 id={`${id}-h`} className="max-w-3xl text-2xl font-semibold leading-tight text-ink sm:text-3xl">
          {title}
        </h2>
      </div>
      {children}
      <p className="text-micro text-ink-mute">{source}</p>
    </section>
  );
}

function Hero({ o }: { o: OwnershipOverview }) {
  const c = o.concentration;
  const byId = new Map(o.largest_holder.map((h) => [h.filer_id, h.companies]));
  const vanguard = byId.get('VANGUARD') ?? 0;
  const blackrock = byId.get('BLACKROCK') ?? 0;
  const two = vanguard + blackrock;
  const share = c.companies ? Math.round((two / c.companies) * 100) : 0;
  return (
    <header className="space-y-8 pt-2">
      <div className="space-y-3">
        <span className="text-micro font-semibold uppercase tracking-[0.14em] text-civic">Follow the money</span>
        <h1 className="max-w-4xl text-4xl font-semibold leading-[1.05] tracking-tight text-ink sm:text-6xl">
          Two firms are the biggest owner of{' '}
          <span className="text-civic">{share}%</span> of the companies we track.
        </h1>
        <p className="max-w-2xl text-base text-ink-dim">
          Vanguard is the single largest shareholder of {vanguard} of {c.companies} companies, BlackRock of{' '}
          {blackrock} more. Their money is mostly yours: index funds in retirement accounts. They vote it.
        </p>
      </div>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        <div className="rounded-2xl border border-line bg-raised p-5">
          <div className="font-mono text-display font-semibold text-ink tabular">{two}</div>
          <div className="text-sm text-ink-dim">
            of {c.companies} companies where Vanguard or BlackRock is owner number one
          </div>
        </div>
        <div className="rounded-2xl border border-line bg-raised p-5">
          <div className="font-mono text-display font-semibold text-ink tabular">{pct1(c.median_big_three_pct)}</div>
          <div className="text-sm text-ink-dim">
            of a typical company held by just three firms: BlackRock, Vanguard and State Street
          </div>
        </div>
        <div className="rounded-2xl border border-line bg-raised p-5">
          <div className="font-mono text-display font-semibold text-ink tabular">{c.notable}</div>
          <div className="text-sm text-ink-dim">
            companies where those three hold {formatNumber(c.notable_threshold_pct ?? 15, { decimals: 0 })}% or more
          </div>
        </div>
      </div>
    </header>
  );
}

function OwnerLookup() {
  const [draft, setDraft] = useState('NVDA');
  const [ticker, setTicker] = useState('NVDA');
  const { data, error, isLoading } = useSWR<HoldersResponse>(
    ticker ? `/ownership/holders/${encodeURIComponent(ticker)}` : null,
    fetcher,
  );
  const holders = (data?.holders ?? []).filter((h) => (h.pct_outstanding ?? 0) > 0).slice(0, 10);
  const max = holders.length ? (holders[0].pct_outstanding ?? 1) : 1;
  return (
    <div className="rounded-2xl border border-line bg-raised p-5 space-y-4">
      <form
        className="flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          setTicker(draft.trim().toUpperCase());
        }}
      >
        <label htmlFor="owner-ticker" className="flex flex-col gap-1 text-xs text-ink-mute">
          A company you own, shop at or work for (name or ticker)
          <input
            id="owner-ticker"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            className="w-56 rounded-lg border border-line-strong bg-inset px-3 py-2 text-sm text-ink outline-none focus:border-accent"
            placeholder="Walmart, AAPL…"
          />
        </label>
        <button type="submit" className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-page hover:brightness-110">
          Who owns it
        </button>
      </form>
      {isLoading ? (
        <LoadingLines lines={4} label="Reading 13F filings" />
      ) : error ? (
        <EmptyState kind="error" title="Could not load owners" detail="The ownership service did not answer. Try again in a moment." />
      ) : holders.length === 0 ? (
        <EmptyState
          kind="empty"
          title={`No tracked owners for ${ticker}`}
          detail="We read the filings of 20 of the largest managers. This company is not in their reported holdings, or not in our coverage yet."
        />
      ) : (
        <div className="space-y-3">
          <p className="text-sm text-ink-dim">
            <span className="text-ink">{data!.name ?? data!.ticker}</span>
            {data!.name && data!.name !== data!.ticker && (
              <span className="font-mono text-ink-mute"> ({data!.ticker})</span>
            )}
            : the {data!.holders.length} tracked institutions that own it hold{' '}
            <span className="text-ink">{pct1(data!.tracked_institutions_pct)}</span> between them, and BlackRock,
            Vanguard and State Street alone <span className="text-civic">{pct1(data!.big_three_pct)}</span>.
            {data!.holders.length > holders.length && ` The ${holders.length} largest are shown.`}
          </p>
          <ul className="space-y-1.5">
            {holders.map((h) => {
              const big = BIG_THREE_IDS.has(h.filer_id);
              return (
                <li key={h.filer_id} className="grid grid-cols-[minmax(0,10rem)_minmax(0,1fr)_4rem] items-center gap-3 text-sm">
                  <span className={`truncate ${big ? 'text-ink' : 'text-ink-dim'}`}>{shortName(h.filer)}</span>
                  <span className="h-2.5 rounded-full bg-inset">
                    <span
                      className={`block h-full rounded-full ${big ? 'bg-civic' : 'bg-accent/60'}`}
                      style={{ width: pctWidth(h.pct_outstanding, max) }}
                    />
                  </span>
                  <span className="text-right font-mono text-ink tabular">{pct1(h.pct_outstanding)}</span>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}

function Rivals({ rivals }: { rivals: OwnershipOverview['rivals'] }) {
  if (!rivals.length) return <EmptyState kind="empty" title="No rival pairs yet" detail="Holder data for both sides of a pair has not arrived." />;
  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
      {rivals.slice(0, 8).map((r) => (
        <article key={`${r.a}-${r.b}`} className="rounded-2xl border border-line bg-raised p-5 space-y-3">
          <div className="space-y-0.5">
            <div className="text-base font-semibold text-ink">
              {r.a_name ?? r.a} <span className="font-normal text-ink-mute">vs</span> {r.b_name ?? r.b}
            </div>
            <div className="text-xs text-ink-mute">
              {r.label} · <span className="font-mono">{r.a}</span> and <span className="font-mono">{r.b}</span>
            </div>
          </div>
          <div className="font-mono text-lead font-semibold text-ink tabular">{pct1(r.common_pct)}</div>
          <p className="text-sm text-ink-dim">
            of each company is held by the same {r.shared_holders} institutions. The two compete for customers; their
            owners win either way.
          </p>
          <span className="block h-2 rounded-full bg-inset">
            <span className="block h-full rounded-full bg-civic" style={{ width: pctWidth(r.common_pct) }} />
          </span>
        </article>
      ))}
    </div>
  );
}

function Network({ rows }: { rows: CrossHolding[] }) {
  const size = 560;
  const { nodes, edges } = ownershipNetwork(rows, { size });
  if (!nodes.length) return <EmptyState kind="empty" title="No cross-holdings yet" detail="13F filings for the listed managers have not been read." />;
  const strongest = [...rows].sort((a, b) => b.pct - a.pct).slice(0, 5);
  return (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)]">
      <div className="overflow-x-auto rounded-2xl border border-line bg-raised p-3">
        <svg
          viewBox={`0 0 ${size} ${size}`}
          className="mx-auto block w-full max-w-[560px]"
          role="img"
          aria-label={`Who owns whom among ${nodes.length} financial institutions: ${edges.length} stakes`}
        >
          {edges.map((e, i) => (
            <line
              key={i}
              x1={e.from.x}
              y1={e.from.y}
              x2={e.to.x}
              y2={e.to.y}
              stroke={e.mutual ? 'var(--civic)' : 'var(--accent)'}
              strokeOpacity={e.mutual ? 0.85 : 0.4}
              strokeWidth={e.width}
            />
          ))}
          {nodes.map((n) => (
            <g key={n.id}>
              <circle cx={n.x} cy={n.y} r={n.bigThree ? 9 : 6} fill={n.bigThree ? 'var(--civic)' : 'var(--accent)'} stroke="var(--bg-raised)" strokeWidth={2} />
              <text
                x={n.labelX}
                y={n.labelY + (n.anchor === 'middle' ? (n.y > size / 2 ? 10 : -6) : 0)}
                textAnchor={n.anchor}
                fontSize={12}
                fill={n.bigThree ? 'var(--text-primary)' : 'var(--text-secondary)'}
              >
                {n.label}
              </text>
            </g>
          ))}
        </svg>
        <div className="flex flex-wrap gap-4 px-2 pb-1 text-micro text-ink-mute">
          <span className="flex items-center gap-1.5"><span className="h-0.5 w-5 bg-civic" />They own each other</span>
          <span className="flex items-center gap-1.5"><span className="h-0.5 w-5 bg-accent/60" />One owns the other</span>
          <span>Thicker line: bigger stake</span>
        </div>
      </div>
      <div className="space-y-3">
        <p className="text-sm text-ink-dim">
          The firms that own America&apos;s companies also own each other. These are the largest stakes one holds in
          another, from their own filings.
        </p>
        <ul className="space-y-2">
          {strongest.map((r) => (
            <li key={`${r.holder}-${r.held}`} className="rounded-xl border border-line bg-raised px-4 py-3 text-sm">
              <span className="text-ink">{shortName(r.holder)}</span> <span className="text-ink-mute">owns</span>{' '}
              <span className="font-mono text-civic">{pct1(r.pct)}</span> <span className="text-ink-mute">of</span>{' '}
              <span className="text-ink">{shortName(r.held)}</span>
              {r.reverse_pct != null && (
                <span className="text-ink-mute">
                  {' '}
                  and it owns <span className="font-mono text-ink">{pct1(r.reverse_pct)}</span> back
                </span>
              )}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

function Dependence({ groups }: { groups: SupplyGroup[] }) {
  const walmart = groups.find((g) => g.buyer === 'WMT');
  const rest = groups.filter((g) => g.buyer !== 'WMT').slice(0, 4);
  if (!groups.length) return <EmptyState kind="empty" title="No customer links yet" detail="Annual reports are read as they are filed." />;
  return (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
      {walmart && (
        <div className="rounded-2xl border border-line bg-raised p-5 space-y-4">
          <p className="text-sm text-ink-dim">
            Share of each company&apos;s sales that goes to <span className="text-ink">Walmart</span>, as each one
            reported it to the SEC.
          </p>
          <ul className="space-y-2">
            {walmart.suppliers.map((s) => (
              <li key={s.ticker} className="grid grid-cols-[minmax(0,9rem)_minmax(0,1fr)_3.5rem] items-center gap-3 text-sm">
                <span className="truncate text-ink" title={s.ticker}>
                  {s.name ?? s.ticker}
                </span>
                <span className="h-3 rounded bg-inset">
                  <span className="block h-full rounded bg-caution/70" style={{ width: pctWidth(s.pct, 35) }} />
                </span>
                <span className="text-right font-mono text-ink tabular">{pct1(s.pct)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="space-y-3">
        {rest.map((g) => (
          <div key={g.buyer} className="rounded-xl border border-line bg-raised px-4 py-3 space-y-1.5">
            <div className="text-sm text-ink">{g.buyer_name} buys</div>
            <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm">
              {g.suppliers.map((s) => (
                <span key={s.ticker} className="text-ink-dim">
                  <span className="font-mono text-caution">{pct1(s.pct)}</span> of <span className="text-ink">{s.name ?? s.ticker}</span>&apos;s sales
                </span>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function PublicMoney({ money, lobbying }: { money?: CivicMoney; lobbying?: Lobbying }) {
  const contracts = (money?.totals ?? []).filter((t) => t.kind === 'contract').sort((a, b) => b.total - a.total).slice(0, 6);
  const lobby = [...(lobbying?.by_vendor ?? [])].sort((a, b) => b.total - a.total).slice(0, 6);
  const maxC = contracts[0]?.total ?? 1;
  const maxL = lobby[0]?.total ?? 1;
  return (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
      <div className="rounded-2xl border border-line bg-raised p-5 space-y-3">
        <h3 className="text-sm font-semibold text-ink">Federal contracts to surveillance vendors</h3>
        {contracts.length === 0 ? (
          <p className="text-sm text-ink-mute">No awards matched yet.</p>
        ) : (
          <ul className="space-y-2">
            {contracts.map((t) => (
              <li key={t.matched} className="space-y-1">
                <div className="flex justify-between gap-3 text-sm">
                  <span className="text-ink">{t.matched}</span>
                  <span className="font-mono text-ink tabular">{formatCurrency(t.total)}</span>
                </div>
                <span className="block h-2 rounded bg-inset">
                  <span className="block h-full rounded bg-civic/80" style={{ width: pctWidth(t.total, maxC) }} />
                </span>
                <span className="text-micro text-ink-mute">{formatNumber(t.awards, { decimals: 0 })} awards</span>
              </li>
            ))}
          </ul>
        )}
      </div>
      <div className="rounded-2xl border border-line bg-raised p-5 space-y-3">
        <h3 className="text-sm font-semibold text-ink">Spent lobbying Congress</h3>
        {lobby.length === 0 ? (
          <p className="text-sm text-ink-mute">No lobbying filings matched yet.</p>
        ) : (
          <ul className="space-y-2">
            {lobby.map((v) => (
              <li key={v.vendor} className="space-y-1">
                <div className="flex justify-between gap-3 text-sm">
                  <span className="text-ink">{v.vendor}</span>
                  <span className="font-mono text-ink tabular">{formatCurrency(v.total)}</span>
                </div>
                <span className="block h-2 rounded bg-inset">
                  <span className="block h-full rounded bg-accent/70" style={{ width: pctWidth(v.total, maxL) }} />
                </span>
              </li>
            ))}
          </ul>
        )}
        <Link href="/watch#money" className="inline-block text-sm text-accent hover:underline">
          Every award and filing, on Surveillance Watch →
        </Link>
      </div>
    </div>
  );
}

function Interlocks({ rows }: { rows?: Interlock[] }) {
  const list = (rows ?? []).slice(0, 8);
  if (!list.length) return <EmptyState kind="empty" title="No interlocks yet" detail="These build up as insider filings arrive." />;
  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
      {list.map((r) => (
        <div key={r.person} className="rounded-xl border border-line bg-raised px-4 py-3 space-y-1.5">
          <div className="flex items-baseline justify-between gap-3">
            <span className="truncate text-sm text-ink">{r.person}</span>
            <span className="shrink-0 font-mono text-lead font-semibold text-civic tabular">{r.companies.length}</span>
          </div>
          <div className="text-micro text-ink-mute">
            {r.roles.slice(0, 2).join(' · ') || 'insider'} at: {r.companies.slice(0, 8).join(', ')}
            {r.companies.length > 8 ? ` and ${r.companies.length - 8} more` : ''}
          </div>
        </div>
      ))}
    </div>
  );
}

export default function FollowTheMoney() {
  const { data: overview, error } = useSWR<OwnershipOverview>('/ownership/overview', fetcher, { refreshInterval: POLL.rare });
  const { data: cross } = useSWR<CrossHolding[]>('/ownership/cross-holdings', fetcher, { refreshInterval: POLL.rare });
  const { data: supply } = useSWR<SupplyGroup[]>('/ownership/supply-chain', fetcher, { refreshInterval: POLL.rare });
  const { data: interlocks } = useSWR<Interlock[]>('/ownership/interlocks?limit=8', fetcher, { refreshInterval: POLL.rare });
  const { data: money } = useSWR<CivicMoney>('/civic/money', fetcher, { refreshInterval: POLL.rare });
  const { data: lobbying } = useSWR<Lobbying>('/civic/lobbying', fetcher, { refreshInterval: POLL.rare });

  if (error) {
    return <EmptyState kind="error" title="Could not load ownership data" detail="The ownership service did not answer. Try again in a moment." />;
  }
  if (!overview) return <LoadingLines lines={6} label="Reading who owns what" />;

  return (
    <div className="mx-auto max-w-6xl space-y-12 pb-16">
      <Hero o={overview} />

      <Section id="lookup" kicker="Look it up" title="Who owns the companies in your life?" source="Source: 13F filings of 20 of the largest managers, as filed (45-day lag). Long positions only, so the real figure is higher.">
        <OwnerLookup />
      </Section>

      <Section id="rivals" kicker="Rivals, same owners" title="Companies that compete are owned by the same firms." source="Common ownership: the stake the same institutions hold in both companies, summed over every shared holder.">
        <Rivals rivals={overview.rivals} />
      </Section>

      <Section id="web" kicker="The owners own each other" title="The firms that own the market own each other too." source="13F stakes between listed financial institutions. Purple lines are firms that each hold the other.">
        <Network rows={cross ?? overview.mutual} />
      </Section>

      <Section id="customers" kicker="One customer, many companies" title="Some of the brands you know live on Walmart's orders." source="Customers each company names in its annual report (10-K); the SEC requires any above 10% of revenue. Combined figures are left out.">
        <Dependence groups={supply ?? []} />
      </Section>

      <Section id="public" kicker="Your taxes, their business" title="Public money and lobbying behind the surveillance industry." source="USAspending awards matched by recipient name; lda.gov lobbying filings. Spending through trade associations is not included.">
        <PublicMoney money={money} lobbying={lobbying} />
      </Section>

      <Section id="boards" kicker="One investor, many boardrooms" title="The same names keep turning up as insiders." source="SEC Form 4: one person or fund reporting as an insider (director, officer or 10% owner) at several companies. Builds up as filings arrive.">
        <Interlocks rows={interlocks} />
      </Section>
    </div>
  );
}
