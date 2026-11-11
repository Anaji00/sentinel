'use client';

/**
 * "What if": name a development ("a ceasefire in Ukraine"), optionally with stocks to tie it to, and the
 * platform's local model writes the outcomes, their likelihoods and what would confirm or rule out each
 * (POST /scenarios/request, then GET /scenarios/request/{id}). A scenario is a hypothesis the model wrote, not
 * a forecast, and the panel says so. The feature is part of Pro when payments are on; a 402 shows that plainly.
 */

import React from 'react';
import useSWR from 'swr';
import { apiClient, fetcher } from '../lib/api';
import { isFinished, parseRequest, problem, statusWords, TOPIC_MAX, type RequestState } from '../lib/whatIf';
import { Button } from './ui/Button';
import { Input } from './ui/Input';

function submitError(err: unknown): string {
  const e = err as { response?: { status?: number; data?: { detail?: { message?: string } | string } } };
  const status = e?.response?.status;
  if (status === 402) return 'What-if scenarios are part of Sentinel Pro.';
  if (status === 429) return 'Too many requests are waiting. Try again in a minute.';
  if (status === 403) return 'Sign in to ask a what-if.';
  if (status === 422) return 'That request was not accepted. Check the wording and the symbols.';
  return 'The request could not be sent. Try again.';
}

export default function WhatIf() {
  const [topic, setTopic] = React.useState('');
  const [tickers, setTickers] = React.useState('');
  const [requestId, setRequestId] = React.useState<string | null>(null);
  const [sending, setSending] = React.useState(false);
  const [failure, setFailure] = React.useState<string | null>(null);

  const parsed = React.useMemo(() => parseRequest(topic, tickers), [topic, tickers]);
  const blocked = problem(parsed);

  const { data: state, error: pollError } = useSWR<RequestState>(requestId ? `/scenarios/request/${encodeURIComponent(requestId)}` : null, fetcher, {
    refreshInterval: (latest) => (isFinished(latest) ? 0 : 4000),
    revalidateOnFocus: false,
  });

  const send = async (e: React.FormEvent) => {
    e.preventDefault();
    if (blocked || sending) return;
    setSending(true);
    setFailure(null);
    setRequestId(null);
    try {
      const res = await apiClient.post<{ request_id: string }>('/scenarios/request', { topic: parsed.topic, tickers: parsed.tickers });
      setRequestId(res.data.request_id);
    } catch (err) {
      setFailure(submitError(err));
    } finally {
      setSending(false);
    }
  };

  return (
    <section aria-label="Ask a what-if" className="space-y-3 rounded-md border border-line bg-raised p-4">
      <div>
        <h2 className="text-head font-semibold text-ink">What if…</h2>
        <p className="text-xs text-ink-mute">
          Name a development and the platform writes the possible outcomes, how likely each looks, and what would confirm or rule it out.
          The model is small and runs here, so treat the result as a way to think, not a forecast.
        </p>
      </div>
      <form onSubmit={send} className="space-y-2.5">
        <Input
          label="The development"
          placeholder="A ceasefire in Ukraine, a strike at the Port of Los Angeles…"
          value={topic}
          maxLength={TOPIC_MAX}
          onChange={(e) => setTopic(e.target.value)}
        />
        <Input
          label="Stocks to tie it to (optional)"
          placeholder="XOM, LMT"
          value={tickers}
          spellCheck={false}
          onChange={(e) => setTickers(e.target.value)}
          error={tickers && parsed.invalid.length ? `Not a stock symbol: ${parsed.invalid.join(', ')}.` : null}
        />
        <div className="flex flex-wrap items-center gap-3">
          <Button type="submit" variant="primary" size="md" loading={sending} disabled={Boolean(blocked) || sending}>
            Write the scenario
          </Button>
          {blocked && topic && !parsed.invalid.length && <span className="text-micro text-ink-mute">{blocked}</span>}
        </div>
      </form>

      {failure && (
        <p role="alert" className="text-sm text-caution">
          {failure}
        </p>
      )}
      {requestId && (
        <div role="status" aria-live="polite" className="space-y-1 rounded-lg border border-line px-3 py-2 text-sm">
          {pollError && !state ? (
            <p className="text-caution">The status could not be read. The request may still be running.</p>
          ) : (
            <>
              <p className="text-ink">{statusWords(state)}</p>
              {state?.status === 'done' && (
                <p className="text-ink-dim">
                  {state.headline ? <span className="text-ink">{state.headline}. </span> : null}
                  It now appears in the list below{state.scenario_id != null ? ` as scenario ${state.scenario_id}` : ''}.
                </p>
              )}
            </>
          )}
        </div>
      )}
    </section>
  );
}
