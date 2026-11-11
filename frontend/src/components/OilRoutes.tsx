'use client';

/**
 * Unusual readings around oil routes, from /chokepoints/flows: ship counts, tankers and ships going dark at
 * each watched site, each against that site's own recent history. Only readings well away from their usual
 * level are listed. The history is about a month, which the panel says, because a z-score on a month of data
 * is a pointer to look, not proof that anything happened.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { formatNumber } from '../lib/format';
import { metricWords, siteWords, unusualOil, unusualWords, type OilReport } from '../lib/bookViews';
import { POLL } from './ui/DataProvider';
import { ClockTime } from './ui/ClockTime';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';
import { Term } from './ui/Term';
import { Updated } from './ui/Updated';

export default function OilRoutes() {
  const { data, error, mutate } = useSWR<OilReport>('/chokepoints/flows', fetcher, { refreshInterval: POLL.rare });
  const unusual = React.useMemo(() => unusualOil(data), [data]);
  const watched = data?.series?.length ?? 0;

  return (
    <section aria-label="Unusual activity on oil routes" className="space-y-3 rounded-md border border-line bg-raised p-5 text-xs">
      <div>
        <h2 className="text-head font-semibold text-ink">Oil routes: what looks unusual</h2>
        <p className="text-ink-mute">
          Ship and tanker measures at watched sites, each compared with its own last month or so. <Updated source="/chokepoints/flows" />
        </p>
      </div>
      {failedWithout(error, data) ? (
        <PanelError what="the oil route measures" onRetry={() => void mutate()} />
      ) : !data ? (
        <LoadingLines lines={4} label="Loading oil route measures" />
      ) : watched === 0 ? (
        <p className="text-ink-mute">No oil route measures have been computed yet.</p>
      ) : unusual.length === 0 ? (
        <p className="text-ink-dim">Nothing stands out: all {watched} measures are near their usual levels.</p>
      ) : (
        <ul className="divide-y divide-line">
          {unusual.map((s) => (
            <li key={`${s.site}-${s.metric}`} className="flex items-center gap-3 py-2 text-sm">
              <div className="min-w-0 flex-1">
                <div className="truncate text-ink">{siteWords(s.site)}</div>
                <div className="text-micro text-ink-mute">
                  {metricWords(s.metric)}: {formatNumber(s.value, { decimals: s.value < 1 ? 4 : 0 })}, {unusualWords(s.z)}
                </div>
              </div>
              <span className={`rounded-full px-2 py-0.5 font-mono text-micro ${(s.z ?? 0) >= 0 ? 'bg-caution/15 text-caution' : 'bg-info/15 text-info'}`}>
                {(s.z ?? 0) >= 0 ? '+' : ''}
                {formatNumber(s.z ?? 0, { decimals: 1 })}σ
              </span>
            </li>
          ))}
        </ul>
      )}
      {data && watched > 0 && (
        <p className="text-micro text-ink-mute">
          <Term word="sigma">σ</Term> counts how many typical day-to-day swings a reading is from its own average. Only about {Math.max(...(data.series ?? []).map((s) => s.history_days))} days of history exist, so treat these as pointers to look, not proof. Updated{' '}
          {data.generated_at ? <ClockTime value={data.generated_at} withDate seconds={false} /> : 'recently'}.
        </p>
      )}
    </section>
  );
}
