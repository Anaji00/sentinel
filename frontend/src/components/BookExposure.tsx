'use client';

/**
 * What a standard shock would do to the paper book, from /portfolio/exposure: the stock market down 5%,
 * yields up 25 basis points, oil, gold, the dollar. A move beyond the limit is flagged. These are model
 * estimates from the book's measured sensitivities, not forecasts, and the panel says so.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { formatNumber } from '../lib/format';
import { instrumentWords, sortExposure, type ExposureReport } from '../lib/bookViews';
import { POLL } from './ui/DataProvider';
import { ClockTime } from './ui/ClockTime';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';
import { Updated } from './ui/Updated';

export default function BookExposure() {
  const { data, error, mutate } = useSWR<ExposureReport>('/portfolio/exposure', fetcher, { refreshInterval: POLL.rare });
  const rows = React.useMemo(() => sortExposure(data?.rows ?? []), [data]);
  const limit = (data?.limit ?? 0) * 100;

  return (
    <section aria-label="What a market shock would do to the book" className="space-y-3 rounded-md border border-line bg-raised p-5 text-xs">
      <div>
        <h2 className="text-head font-semibold text-ink">If markets move</h2>
        <p className="text-ink-mute">What a standard shock would do to the paper book, from how its holdings have behaved. Estimates, not forecasts.{' '}
          <Updated source="/portfolio/exposure" />
        </p>
      </div>
      {failedWithout(error, data) ? (
        <PanelError what="the book's exposure" onRetry={() => void mutate()} />
      ) : !data ? (
        <LoadingLines lines={4} label="Loading the book's exposure" />
      ) : rows.length === 0 ? (
        <p className="text-ink-mute">The exposure report has not been built yet. It runs after each rebalance.</p>
      ) : (
        <>
          <ul className="divide-y divide-line">
            {rows.map((r) => {
              const move = r.book_move * 100;
              return (
                <li key={`${r.instrument}-${r.shock}`} className="flex items-center gap-3 py-2 text-sm">
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-ink">{instrumentWords(r.instrument)}</div>
                    <div className="text-micro text-ink-mute">
                      shock of {r.shock}
                      {r.names ? `, ${r.names} holdings react` : ''}
                    </div>
                  </div>
                  {r.flag && <span className="rounded-full bg-caution/15 px-2 py-0.5 text-micro font-semibold text-caution">over the limit</span>}
                  <div className={`w-20 text-right font-mono ${move >= 0 ? 'text-positive' : 'text-negative'}`}>
                    {move >= 0 ? '+' : ''}
                    {formatNumber(move, { decimals: 2 })}%
                  </div>
                </li>
              );
            })}
          </ul>
          <p className="text-micro text-ink-mute">
            The limit is {formatNumber(limit, { decimals: 1 })}% of the book for one shock.
            {data.market_beta != null ? ` Overall the book moves about ${formatNumber(data.market_beta, { decimals: 2 })} for every 1 the market moves.` : ''}
            {data.generated_at ? (
              <>
                {' '}Computed <ClockTime value={data.generated_at} withDate seconds={false} />.
              </>
            ) : null}
          </p>
        </>
      )}
    </section>
  );
}
