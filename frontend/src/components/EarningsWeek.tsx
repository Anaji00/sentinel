'use client';

/**
 * Companies reporting results in the next seven days, soonest first and larger companies first within a day. Only dates
 * the company or its data vendor has listed appear here; dates guessed from the last report stay on the stock page,
 * marked as expected.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { barLabel } from '../lib/candles';
import { formatCurrency } from '../lib/format';
import { groupByDate, hourWords, type EarningsCalendar } from '../lib/digest';
import { bigMoney } from '../lib/snapshot';
import { StockLink } from './StockDrawer';
import { POLL } from './ui/DataProvider';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

const day = (iso: string) => barLabel(`${iso.slice(0, 10)}T00:00:00Z`, '1d');

export default function EarningsWeek() {
  const { data, error } = useSWR<EarningsCalendar>('/calendar/earnings?days=7&min_cap=10000000000&limit=60', fetcher, { refreshInterval: POLL.rare, revalidateOnFocus: false });
  const days = groupByDate(data?.rows);
  return (
    <section aria-label="Companies reporting this week" className="space-y-2 rounded-md border border-line bg-raised p-4">
      <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Reporting results this week</h2>
      {failedWithout(error, data) ? (
        <PanelError what="the earnings calendar" inline />
      ) : !data ? (
        <LoadingLines lines={3} label="Loading the earnings calendar" />
      ) : days.length === 0 ? (
        <p role="status" className="text-xs text-ink-mute">No listed reporting dates in the next seven days for companies worth $10 billion or more.</p>
      ) : (
        <div className="grid gap-x-8 gap-y-3 md:grid-cols-2">
          {days.map((d) => (
            <div key={d.date} className="space-y-1">
              <h3 className="text-xs font-semibold text-ink">{day(d.date)}</h3>
              <ul className="space-y-1">
                {d.rows.slice(0, 8).map((r) => (
                  <li key={`${r.ticker}-${r.date}`} className="flex items-baseline justify-between gap-2 text-xs">
                    <span className="min-w-0 truncate">
                      <StockLink ticker={r.ticker} className="font-mono font-semibold text-ink hover:text-accent" />
                      <span className="text-ink-mute"> {hourWords(r.hour)}</span>
                    </span>
                    <span className="shrink-0 font-mono text-micro tabular-nums text-ink-mute" title="Expected profit per share and company size">
                      {r.eps_estimate != null ? `${formatCurrency(r.eps_estimate, { decimals: 2 })} a share` : ''}
                      {bigMoney(r.market_cap) ? ` · ${bigMoney(r.market_cap)}` : ''}
                    </span>
                  </li>
                ))}
              </ul>
              {d.rows.length > 8 && <p className="text-micro text-ink-mute">and {d.rows.length - 8} more</p>}
            </div>
          ))}
        </div>
      )}
      <p className="text-micro text-ink-mute">Listed dates only, for companies worth $10 billion or more. The profit figure is the vendor&apos;s expectation, not a result.</p>
    </section>
  );
}
