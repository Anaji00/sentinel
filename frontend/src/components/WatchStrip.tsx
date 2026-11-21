'use client';

/**
 * Your followed stocks in one block: price and move, when they next report, the model's rank and anything held back or
 * flagged. One request for all of them. With none followed it says how to follow one, instead of showing a ranked
 * list of "top picks".
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { barLabel } from '../lib/candles';
import { formatCurrency } from '../lib/format';
import { rankWords } from '../lib/ideas';
import { signedPct } from '../lib/scan';
import { earningsWords, stripFlags, stripTickers, type Strip } from '../lib/strip';
import { usePreferences } from '../lib/preferences';
import { StockLink } from './StockDrawer';
import { closesKey, type Closes } from '../lib/spark';
import { InlineSpark } from './ui/InlineSpark';
import { POLL } from './ui/DataProvider';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

const day = (iso: string) => barLabel(`${iso.slice(0, 10)}T00:00:00Z`, '1d');

export default function WatchStrip() {
  const { prefs } = usePreferences();
  const tickers = stripTickers(prefs?.tickers);
  const key = tickers.length ? `/stocks/strip?tickers=${encodeURIComponent(tickers.join(','))}` : null;
  const { data, error } = useSWR<Strip>(key, fetcher, { refreshInterval: POLL.standard, revalidateOnFocus: false });
  const spark = useSWR<Closes>(closesKey(tickers), fetcher, { revalidateOnFocus: false, shouldRetryOnError: false });
  return (
    <section aria-label="Your stocks" className="space-y-2 border-t border-line pt-4">
      <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Your stocks</h2>
      {!prefs ? (
        <LoadingLines lines={2} label="Loading your stocks" />
      ) : tickers.length === 0 ? (
        <p role="status" className="text-xs text-ink-mute">You are not following any stocks yet. Open a stock and press Follow, and it will appear here with its price, its next report date and anything that needs a look.</p>
      ) : failedWithout(error, data) ? (
        <PanelError what="your stocks" inline />
      ) : !data ? (
        <LoadingLines lines={3} label="Loading your stocks" />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[560px] text-xs">
            <thead className="text-ink-mute">
              <tr className="border-b border-line text-left">
                <th className="py-1.5 font-normal">Stock</th>
                <th className="py-1.5 text-right font-normal">Price</th>
                <th className="py-1.5 text-right font-normal">Change</th>
                <th className="py-1.5 pl-4 text-right font-normal" title="Daily closes over the last 60 sessions">60 days</th>
                <th className="py-1.5 text-right font-normal">Model view</th>
                <th className="py-1.5 text-right font-normal">Next earnings</th>
                <th className="py-1.5 pl-4 text-left font-normal">Worth a look</th>
              </tr>
            </thead>
            <tbody className="font-mono tabular-nums">
              {data.rows.map((r) => (
                <tr key={r.ticker} className="border-b border-line/60 hover:bg-overlay">
                  <td className="py-1.5 text-left font-sans">
                    <StockLink ticker={r.ticker} className="font-mono font-semibold text-ink hover:text-accent" />
                  </td>
                  <td className="py-1.5 text-right">{r.price != null ? formatCurrency(r.price, { decimals: 2 }) : '—'}</td>
                  <td className={`py-1.5 text-right ${r.change_pct == null ? 'text-ink-mute' : r.change_pct >= 0 ? 'text-positive' : 'text-negative'}`}>
                    {r.change_pct == null ? '—' : `${r.change_pct >= 0 ? '▲' : '▼'} ${signedPct(r.change_pct)}`}
                  </td>
                  <td className="py-1.5 pl-4 text-right">
                    <InlineSpark closes={spark.data?.[r.ticker]} />
                  </td>
                  <td className="py-1.5 text-right font-sans text-ink-dim">{r.model_percentile != null ? rankWords(r.model_percentile) : '—'}</td>
                  <td className="py-1.5 text-right font-sans text-ink-dim">{earningsWords(r.next_earnings, day).replace('Earnings ', '') || '—'}</td>
                  <td className="py-1.5 pl-4 text-left font-sans text-ink-dim">{stripFlags(r).join(' · ') || ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="pt-2 text-micro text-ink-mute">
            Change is since the last completed close outside market hours. {data.rows[0]?.model_as_of ? `Model view is as of ${day(data.rows[0].model_as_of)}.` : ''} An earnings date marked expected is a guess one quarter after the last report.
          </p>
        </div>
      )}
    </section>
  );
}
