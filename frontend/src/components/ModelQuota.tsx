'use client';

/**
 * How much of each hosted model's free allowance has been used, so a slow or missing answer can be traced to a limit
 * rather than guessed at. Most used first, as the route returns them.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { formatNumber } from '../lib/format';
import { quotaLine, quotaTone, type QuotaResponse } from '../lib/heldBack';
import { POLL } from './ui/DataProvider';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

export default function ModelQuota() {
  const { data, error } = useSWR<QuotaResponse>('/health/model-quota', fetcher, { refreshInterval: POLL.standard });

  return (
    <section aria-label="Free model allowance" className="space-y-2 rounded-md border border-line bg-inset p-4">
      <h3 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Free model allowance</h3>
      {failedWithout(error, data) ? (
        <PanelError what="the model allowance" inline />
      ) : !data ? (
        <LoadingLines lines={3} label="Loading the model allowance" />
      ) : data.models.length === 0 ? (
        <p role="status" className="text-xs text-ink-mute">
          {data.note ?? 'No models are tracked.'}
        </p>
      ) : (
        <ul className="space-y-2">
          {data.models.map((m) => (
            <li key={`${m.provider}/${m.model}`} className="space-y-0.5 text-xs">
              <div className="flex items-baseline justify-between gap-2">
                <span className="min-w-0 truncate font-mono text-ink">{m.model}</span>
                <span className={`shrink-0 text-micro font-semibold ${quotaTone(m)}`}>{m.status}</span>
              </div>
              <p className="text-micro text-ink-dim">
                {quotaLine(m)}
                {m.share_of_limit_used != null && m.status !== 'no key set' && <> {formatNumber(m.share_of_limit_used * 100, { decimals: 0 })}% of its strictest limit used.</>}
              </p>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
