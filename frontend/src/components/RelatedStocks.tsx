'use client';

/**
 * In the stock drawer: which stocks have moved with this one, and which have a
 * similar business. Each is a link that swaps the drawer to that stock.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { formatNumber } from '../lib/format';
import { emptyRelated, relatedKey, shortDate, type RelatedStocks as Related } from '../lib/related';
import { StockLink } from './StockDrawer';
import { POLL } from './ui/DataProvider';
import { EmptyState } from './ui/EmptyState';
import { LoadingLines } from './ui/Skeleton';

const row = 'flex items-baseline justify-between gap-3 text-sm';

export default function RelatedStocks({ ticker }: { ticker: string }) {
  const { data, error, isLoading } = useSWR<Related>(relatedKey(ticker), fetcher, { refreshInterval: POLL.rare });

  return (
    <section className="space-y-3 rounded-xl border border-line bg-inset p-4">
      <h3 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Stocks that move with it</h3>
      {isLoading ? (
        <LoadingLines lines={3} label="Finding related stocks" />
      ) : error || !data ? (
        <EmptyState kind="error" title="Could not load related stocks" detail="Try again in a moment." />
      ) : data.empty_reason ? (
        <EmptyState kind="empty" {...emptyRelated(data.empty_reason, data.window_sessions)} />
      ) : (
        <>
          {data.moves_with.length > 0 && (
            <ul className="space-y-1.5">
              {data.moves_with.map((p) => (
                <li key={p.ticker} className={row}>
                  <span className="min-w-0 truncate">
                    <StockLink ticker={p.ticker} className="font-mono font-semibold text-ink hover:text-accent" />{' '}
                    <span className="text-ink-mute">{p.name !== p.ticker ? p.name : ''}</span>
                  </span>
                  <span
                    className="shrink-0 text-right text-micro text-ink-dim"
                    title={`Correlation ${formatNumber(p.correlation, { decimals: 2 })} over ${p.sessions} trading days`}
                  >
                    {p.closeness}
                  </span>
                </li>
              ))}
            </ul>
          )}
          {data.similar_business.length > 0 && (
            <div className="space-y-1.5 border-t border-line pt-3">
              <div className="text-micro text-ink-mute">Similar business, by what each says it does</div>
              <ul className="space-y-1.5">
                {data.similar_business.map((p) => (
                  <li key={p.ticker} className={row}>
                    <span className="min-w-0 truncate">
                      <StockLink ticker={p.ticker} className="font-mono font-semibold text-ink hover:text-accent" />{' '}
                      <span className="text-ink-mute">{p.name !== p.ticker ? p.name : ''}</span>
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
          <p className="text-micro text-ink-mute">
            Last {data.window_sessions} trading days, updated {shortDate(data.as_of) || 'recently'}. {data.note}
          </p>
          {data.stale && (
            <p role="status" className="text-micro text-caution">
              This is older than it should be: the nightly update has not run for a few days.
            </p>
          )}
        </>
      )}
    </section>
  );
}
