'use client';

/**
 * The facts behind a stock's call, from the case file (/case/{ticker}): each with its id, its source
 * and when it was true. Not every stock has one: it is built for the book, for stocks with alerts and for
 * strong headline reads, so a missing file is said plainly rather than shown as an empty list.
 */

import React from 'react';
import useSWR from 'swr';
import { ApiError, fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { groupFacts, sourceWords, type CaseFile } from '../lib/caseFacts';
import { ClockTime } from './ui/ClockTime';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

const isDate = (s: string | null | undefined): s is string => Boolean(s && /^\d{4}-\d{2}-\d{2}/.test(s));

export default function SourceFacts({ ticker }: { ticker: string }) {
  const { data, error, mutate } = useSWR<CaseFile>(ticker ? `/case/${encodeURIComponent(ticker)}` : null, fetcher, {
    revalidateOnFocus: false,
    shouldRetryOnError: false,
  });
  const missing = error instanceof ApiError && error.isMissing;
  const groups = React.useMemo(() => groupFacts(data), [data]);

  return (
    <section aria-label={`Facts behind ${ticker}`} className="space-y-2 rounded-xl border border-line p-3">
      <h3 className="stat-label">Facts behind the call</h3>
      {missing ? (
        <p className="text-sm text-ink-mute">
          No case file is built for {ticker}. They are made for the model&apos;s picks, for stocks with alerts, and for strong headline reads.
        </p>
      ) : failedWithout(error, data) ? (
        <PanelError what={`the facts for ${ticker}`} onRetry={() => void mutate()} inline />
      ) : !data ? (
        <LoadingLines lines={3} label="Loading the facts" />
      ) : groups.length === 0 ? (
        <p className="text-sm text-ink-mute">The case file for {ticker} holds no facts yet.</p>
      ) : (
        <div className="space-y-3">
          {groups.map((g) => (
            <div key={g.kind} className="space-y-1">
              <div className="text-micro uppercase tracking-[0.08em] text-ink-mute">{g.label}</div>
              <ul className="space-y-1.5">
                {g.facts.map((f) => (
                  <li key={f.id} className="grid grid-cols-[2rem_minmax(0,1fr)] gap-x-2 text-sm">
                    <span className="pt-0.5 font-mono text-micro text-ink-mute">{f.id}</span>
                    <span className="min-w-0">
                      <span className="text-ink">{f.text}</span>
                      <span className="block text-micro text-ink-mute">
                        From {sourceWords(f.source)}
                        {isDate(f.as_of) ? (
                          <>
                            , as of <ClockTime value={f.as_of} withDate seconds={false} />
                          </>
                        ) : f.as_of ? (
                          `, as of ${f.as_of}`
                        ) : (
                          ''
                        )}
                        .
                      </span>
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          ))}
          {data.built_at && (
            <p className="text-micro text-ink-mute">
              Built <ClockTime value={data.built_at} withDate seconds={false} />. The ids are what the bull and bear case cites.
            </p>
          )}
        </div>
      )}
    </section>
  );
}
