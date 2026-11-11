'use client';

/**
 * Commodities: oil, gas, metals, grains, coffee, cocoa, sugar, livestock and lumber, from /commodities/futures
 * (daily continuous futures, back to 2015 for most). Each row has its price, how it moved over a day, a week,
 * a month and a year, and the stocks it helps or hurts. A change is shown only where the stored history reaches
 * back that far, and a series that has stopped updating says so.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { formatNumber } from '../lib/format';
import { Button } from './ui/Button';
import { POLL } from './ui/DataProvider';
import { EmptyState } from './ui/EmptyState';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';
import { Sparkline } from './ui/Sparkline';
import { StockLink } from './StockDrawer';
import { Updated } from './ui/Updated';

interface Item {
  key: string;
  name: string;
  latest: number;
  as_of: string;
  history_days: number;
  stale: boolean;
  change_day: number | null;
  change_week: number | null;
  change_month: number | null;
  change_year: number | null;
  history: { day: string; value: number }[];
  gain_when_up: string[];
  pay_when_up: string[];
}

interface Category {
  key: string;
  title: string;
  items: Item[];
}

interface Board {
  as_of: string | null;
  stale: boolean;
  categories: Category[];
  note: string;
}

const price = (v: number) => `$${formatNumber(v, { decimals: v >= 1000 ? 0 : 2 })}`;

function Change({ label, pct }: { label: string; pct: number | null }) {
  return (
    <div className="w-16 text-right" title={pct == null ? `Not enough stored history for a ${label} change` : `Over the last ${label}`}>
      {pct == null ? (
        <span className="text-micro text-ink-mute">n/a</span>
      ) : (
        <span className={`font-mono text-xs ${pct >= 0 ? 'text-positive' : 'text-negative'}`}>
          {pct >= 0 ? '+' : ''}
          {formatNumber(pct, { decimals: 1 })}%
        </span>
      )}
    </div>
  );
}

function Row({ i }: { i: Item }) {
  const [open, setOpen] = React.useState(false);
  const stocks = i.gain_when_up.length + i.pay_when_up.length;
  return (
    <li className="py-2">
      <div className="flex items-center gap-3">
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm text-ink">{i.name}</div>
          <div className="text-micro text-ink-mute">
            {i.stale ? `no update since ${i.as_of}` : i.history_days < 30 ? `only ${i.history_days} days of history` : i.key}
          </div>
        </div>
        <Sparkline values={i.history.map((h) => h.value)} width={72} height={24} label={`${i.name}, last four months`} />
        <div className="w-20 text-right font-mono text-sm text-ink">{price(i.latest)}</div>
        <div className="hidden items-center gap-1 sm:flex">
          <Change label="day" pct={i.change_day} />
          <Change label="week" pct={i.change_week} />
          <Change label="month" pct={i.change_month} />
          <Change label="year" pct={i.change_year} />
        </div>
        {stocks > 0 && (
          <Button variant="outline" size="sm" pressed={open} onClick={() => setOpen((v) => !v)} className="shrink-0">
            {open ? 'Hide stocks' : 'Stocks'}
          </Button>
        )}
      </div>
      {open && (
        <div className="mt-2 space-y-1.5 pl-1 text-micro text-ink-mute">
          {[
            ['Gain when it rises', i.gain_when_up],
            ['Pay more when it rises', i.pay_when_up],
          ].map(([label, list]) =>
            (list as string[]).length > 0 ? (
              <div key={label as string} className="flex flex-wrap items-center gap-1.5">
                <span>{label as string}</span>
                {(list as string[]).map((t) => (
                  <StockLink key={t} ticker={t} className="rounded-full border border-line px-2 py-0.5 font-mono text-ink-dim hover:border-line-accent hover:text-ink">
                    {t}
                  </StockLink>
                ))}
              </div>
            ) : null,
          )}
        </div>
      )}
    </li>
  );
}

export default function CommodityBoard() {
  const { data, error, mutate } = useSWR<Board>('/commodities/futures', fetcher, { refreshInterval: POLL.rare });

  if (failedWithout(error, data)) return <PanelError what="commodity prices" onRetry={() => void mutate()} />;
  if (!data) return <LoadingLines lines={6} label="Loading commodity prices" />;
  if (data.categories.length === 0) {
    return <EmptyState kind="empty" title="No commodity prices stored yet" detail="The collector records futures prices once a day." />;
  }

  return (
    <div className="space-y-5">
      <p role="note" className={`text-sm ${data.stale ? 'text-caution' : 'text-ink-dim'}`}>
        {data.stale ? `The newest price is from ${data.as_of}, so the collector may have stopped. ` : ''}
        {data.note} <Updated source="/commodities/futures" pollMs={300_000} />
      </p>
      <div className="grid gap-5 xl:grid-cols-2">
        {data.categories.map((c) => (
          <section key={c.key} aria-label={c.title} className="space-y-2 rounded-xl border border-line bg-raised p-4">
            <div className="flex items-baseline justify-between gap-3">
              <h2 className="text-head font-semibold text-ink">{c.title}</h2>
              <div className="hidden gap-1 text-micro text-ink-mute sm:flex" aria-hidden>
                {['day', 'week', 'month', 'year'].map((l) => (
                  <span key={l} className="w-16 text-right">
                    {l}
                  </span>
                ))}
              </div>
            </div>
            <ul className="divide-y divide-line">
              {c.items.map((i) => (
                <Row key={i.key} i={i} />
              ))}
            </ul>
          </section>
        ))}
      </div>
      <p className="text-micro text-ink-mute">Prices are in each contract&apos;s own US-dollar unit (per barrel, ounce, bushel, pound or tonne). Latest prices are from {data.as_of}.</p>
    </div>
  );
}
