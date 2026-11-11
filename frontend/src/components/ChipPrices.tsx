'use client';

/**
 * Memory-chip and GPU prices: DRAM, NAND flash and GDDR spot (DRAMeXchange) and GPU rental rates (RunPod),
 * from /commodities/chips. Each section says which stocks gain or pay when its prices rise, and the page
 * says plainly how short the record is: collection began on 22 October, which is too little to call a trend.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { EmptyState } from './ui/EmptyState';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';
import { Sparkline } from './ui/Sparkline';
import { StockLink } from './StockDrawer';
import { Updated } from './ui/Updated';

interface PriceItem {
  name: string;
  variant: string | null;
  latest: number;
  as_of: string;
  since: string;
  change_pct: number | null;
  unit: string;
  history: { day: string; value: number }[];
}

interface Section {
  key: string;
  title: string;
  note: string;
  items: PriceItem[];
  gain_when_up: string[];
  pay_when_up: string[];
}

interface ChipPricesResponse {
  as_of: string | null;
  since: string | null;
  record_days: number;
  stale: boolean;
  sections: Section[];
  note: string;
}

const money = (v: number, unit: string) => `$${formatNumber(v, { decimals: v >= 100 ? 0 : 2 })}${unit.includes('hour') ? '/hr' : ''}`;

function Change({ pct }: { pct: number | null }) {
  if (pct == null) return <span className="text-micro text-ink-mute">first day</span>;
  const up = pct >= 0;
  return (
    <span className={`rounded-full px-2 py-0.5 text-micro font-semibold ${up ? 'bg-positive/15 text-positive' : 'bg-negative/15 text-negative'}`}>
      {up ? '+' : ''}
      {formatNumber(pct, { decimals: 1 })}%
    </span>
  );
}

function StockChips({ label, tickers }: { label: string; tickers: string[] }) {
  if (tickers.length === 0) return null;
  return (
    <div className="flex flex-wrap items-center gap-1.5 text-micro text-ink-mute">
      <span>{label}</span>
      {tickers.map((t) => (
        <StockLink key={t} ticker={t} className="rounded-full border border-line px-2 py-0.5 font-mono text-ink-dim hover:border-line-accent hover:text-ink">
          {t}
        </StockLink>
      ))}
    </div>
  );
}

export default function ChipPrices() {
  const { data, error, mutate } = useSWR<ChipPricesResponse>('/commodities/chips', fetcher, { refreshInterval: POLL.rare });

  if (failedWithout(error, data)) return <PanelError what="chip and GPU prices" onRetry={() => void mutate()} />;
  if (!data) return <LoadingLines lines={6} label="Loading chip prices" />;
  if (data.sections.length === 0) {
    return <EmptyState kind="empty" title="No chip prices stored yet" detail="The collector records memory spot prices and GPU rental rates once a day." />;
  }

  return (
    <div className="space-y-5">
      <p role="note" className={`text-sm ${data.stale ? 'text-caution' : 'text-ink-dim'}`}>
        {data.stale ? `The newest price is from ${data.as_of}, so the collector may have stopped. ` : ''}
        {data.note} <Updated source="/commodities/chips" pollMs={300_000} />
      </p>
      <div className="grid gap-5 xl:grid-cols-2">
        {data.sections.map((s) => (
          <section key={s.key} aria-label={s.title} className="space-y-3 rounded-md border border-line bg-raised p-4">
            <div>
              <h2 className="text-head font-semibold text-ink">{s.title}</h2>
              <p className="text-xs text-ink-mute">{s.note}</p>
            </div>
            <ul className="divide-y divide-line">
              {s.items.map((i) => (
                <li key={`${i.name}-${i.variant}`} className="flex items-center gap-3 py-2">
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-sm text-ink">{i.name}</div>
                    {i.variant && <div className="text-micro text-ink-mute">{i.variant}</div>}
                  </div>
                  <Sparkline values={i.history.map((h) => h.value)} width={72} height={24} label={`${i.name}${i.variant ? `, ${i.variant}` : ''}, price history`} />
                  <div className="w-20 text-right font-mono text-sm text-ink">{money(i.latest, i.unit)}</div>
                  <div className="w-16 text-right">
                    <Change pct={i.change_pct} />
                  </div>
                </li>
              ))}
            </ul>
            <div className="space-y-1.5">
              <StockChips label="Gain when it rises" tickers={s.gain_when_up} />
              <StockChips label="Pay more when it rises" tickers={s.pay_when_up} />
            </div>
          </section>
        ))}
      </div>
      <p className="text-micro text-ink-mute">Change is since each price&apos;s first stored day. Latest prices are from {data.as_of}.</p>
    </div>
  );
}
