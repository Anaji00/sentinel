'use client';

/**
 * In the stock drawer: the bull case and the bear case, written by the local AI
 * model when someone asks. Nothing runs until the button is pressed, because the
 * model is the scarcest thing this platform has. Each point shows the facts it
 * rests on, and the page says plainly that a small model wrote it.
 */

import React from 'react';
import { Button } from './ui/Button';
import useSWR from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { canAsk, thesisKey, thesisRefresh, type ThesisPoint, type ThesisState } from '../lib/thesis';
import { ClockTime } from './ui/ClockTime';
import { LoadingLines } from './ui/Skeleton';

function Side({ title, tone, points }: { title: string; tone: string; points: ThesisPoint[] }) {
  return (
    <div className="space-y-2">
      <h4 className={`text-sm font-semibold ${tone}`}>{title}</h4>
      <ul className="space-y-2.5">
        {points.map((p) => (
          <li key={p.point} className="text-sm leading-relaxed text-ink">
            {p.point}
            <details className="mt-1 text-micro text-ink-mute">
              <summary className="cursor-pointer select-none hover:text-ink-dim">
                Based on {p.sources.length} {p.sources.length === 1 ? 'fact' : 'facts'}
              </summary>
              <ul className="mt-1 list-disc space-y-0.5 pl-4">
                {p.sources.map((s) => (
                  <li key={s.id}>{s.text}</li>
                ))}
              </ul>
            </details>
          </li>
        ))}
      </ul>
    </div>
  );
}

export default function ThesisPanel({ ticker }: { ticker: string }) {
  const { data, error, isLoading, mutate } = useSWR<ThesisState>(thesisKey(ticker), fetcher, {
    refreshInterval: thesisRefresh,
  });
  const [asking, setAsking] = React.useState(false);
  const [problem, setProblem] = React.useState<string | null>(null);

  const ask = async () => {
    setAsking(true);
    setProblem(null);
    try {
      const res = await apiClient.post<ThesisState>(thesisKey(ticker));
      await mutate(res.data, { revalidate: false });
    } catch (e) {
      setProblem(describeApiError(e) ?? 'Could not start this. Try again in a moment.');
    } finally {
      setAsking(false);
    }
  };

  return (
    <section aria-label="Bull and bear case" className="space-y-3 rounded-xl border border-line bg-inset p-4">
      <h3 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Bull case and bear case</h3>

      {isLoading ? (
        <LoadingLines lines={2} label="Checking for a saved bull and bear case" />
      ) : error ? (
        <p role="alert" className="text-sm text-caution">Could not check for one. Try again in a moment.</p>
      ) : data?.status === 'pending' ? (
        <p role="status" className="flex items-center gap-2 text-sm text-ink-dim">
          <span aria-hidden className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-line-strong border-t-accent" />
          {data.message || 'Writing it now. This usually takes one to two minutes.'}
        </p>
      ) : data?.status === 'ready' ? (
        <div className="space-y-4">
          <Side title="The case for" tone="text-positive" points={data.bull} />
          <Side title="The case against" tone="text-negative" points={data.bear} />
          {data.would_change_it && (
            <p className="text-sm text-ink-dim">
              <span className="font-semibold text-ink">What would change this: </span>
              {data.would_change_it}
            </p>
          )}
          <p className="text-micro text-ink-mute">
            {data.disclaimer} Written <ClockTime value={data.generated_at} withDate />; kept for six hours.
          </p>
        </div>
      ) : (
        <p className="text-sm text-ink-dim">
          A small AI model running on this machine can set out the best reasons for and against {ticker}, using only
          what this platform knows about it. It takes a minute or two.
        </p>
      )}

      {(data?.status === 'failed' || data?.status === 'busy' || data?.status === 'limited') && data.message && (
        <p role="status" className="text-sm text-caution">{data.message}</p>
      )}
      {problem && (
        <p role="alert" className="text-sm text-caution">{problem}</p>
      )}
      {!isLoading && !error && (canAsk(data) || problem) && (
        <Button variant="primary" size="md"
          type="button"
          onClick={() => void ask()}
          disabled={asking}
        >
          {asking ? 'Starting…' : data?.status === 'failed' || data?.status === 'busy' ? 'Try again' : 'Write the bull and bear case'}
        </Button>
      )}
    </section>
  );
}
