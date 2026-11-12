'use client';

/**
 * On a stock: signals that were raised for it but held back before reaching the traded book, and why. An empty list
 * means nothing was held back, which is not the same as the stock being clear.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { formatNumber } from '../lib/format';
import { groupHeldBack, expectedMoveWords, heldBackKey, type HeldBackResponse } from '../lib/heldBack';
import { POLL } from './ui/DataProvider';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

export default function HeldBack({ ticker }: { ticker: string }) {
  const { data, error } = useSWR<HeldBackResponse>(heldBackKey(ticker), fetcher, { refreshInterval: POLL.rare });
  const groups = React.useMemo(() => groupHeldBack(data?.held_back), [data]);

  return (
    <section aria-label={`Signals held back on ${ticker}`} className="space-y-2 rounded-md border border-line bg-inset p-4">
      <h3 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Held back, last 45 days</h3>
      {failedWithout(error, data) ? (
        <PanelError what="the held-back signals" inline />
      ) : !data ? (
        <LoadingLines lines={2} label={`Loading signals held back on ${ticker}`} />
      ) : groups.length === 0 ? (
        <p role="status" className="text-xs text-ink-mute">
          {data.note ?? `Nothing was held back on ${ticker}.`} That does not mean the stock is clear: only signals of a proven type are listed.
        </p>
      ) : (
        <ul className="space-y-2">
          {groups.map((g) => (
            <li key={g.key} className="space-y-0.5 text-xs">
              <p className="text-ink">{g.signal}</p>
              <p className="text-micro text-ink-dim">
                {expectedMoveWords(g.expectedMove)} over the {g.windows.join(' and the ')}, but held back by the {g.heldBackBy}. {g.explanation}
                {g.measured != null && g.required != null && (
                  <>
                    {' '}
                    Traded ${formatNumber(g.measured, { decimals: 1 })} million a day; the bar is ${formatNumber(g.required, { decimals: 1 })} million.
                  </>
                )}
              </p>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
