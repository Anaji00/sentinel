'use client';

/**
 * What the traded paper book holds: names on each side, how many of them were measured, its exposure to the stock
 * market itself, and the sectors it leans on. The book's published Sharpe includes some of that market exposure, so the
 * panel says so beside it. Averages always show how many names stand behind them.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { barLabel } from '../lib/candles';
import { netBetaWords, sectorWords, sideWords, type B1Exposure as B1 } from '../lib/b1';
import { POLL } from './ui/DataProvider';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

const day = (iso: string) => barLabel(`${iso.slice(0, 10)}T00:00:00Z`, '1d');

export default function B1Exposure() {
  const { data, error, mutate } = useSWR<B1>('/portfolio/b1-exposure', fetcher, { refreshInterval: POLL.rare, revalidateOnFocus: false });
  const net = netBetaWords(data?.net_beta);
  return (
    <section aria-label="What the traded book holds" className="space-y-2 border-t border-line pt-4 text-xs">
      <h2 className="text-head font-semibold text-ink">What the traded book holds</h2>
      {failedWithout(error, data) ? (
        <PanelError what="the book's make-up" onRetry={() => void mutate()} />
      ) : !data ? (
        <LoadingLines lines={3} label="Loading the book's make-up" />
      ) : (
        <>
          <p className="text-ink-dim">{sideWords('Bought', data.long)}</p>
          <p className="text-ink-dim">{sideWords('Bet against', data.short)}</p>
          {net && (
            <p className="text-ink">
              Exposure to the stock market itself: <span className="font-mono">{net}</span>. The book&apos;s published Sharpe estimate includes some of this, so it is not pure stock-picking skill.
            </p>
          )}
          {(data.top_sectors.long.length > 0 || data.top_sectors.short.length > 0) && (
            <p className="text-ink-dim">
              Biggest sectors, bought: {sectorWords(data.top_sectors.long) || 'none'}. Bet against: {sectorWords(data.top_sectors.short) || 'none'}.
            </p>
          )}
          <p className="text-micro text-ink-mute">
            {data.rebalanced_on ? `Rebuilt on ${day(data.rebalanced_on)}. ` : ''}Estimates from how the stocks have moved before, not forecasts.
          </p>
        </>
      )}
    </section>
  );
}
