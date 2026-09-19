'use client';

/**
 * Replacing the tracked equity universe, in one deliberate write.
 *
 * `PUT /watchlists/equities/sync` is an atomic replacement of up to fifty
 * tickers, gated to ADMIN. There are add and remove routes beside it that the
 * account panel reads a count from, and no way to set the whole list -- so
 * changing the universe meant a sequence of single-symbol calls, each leaving
 * the platform in a state nobody chose.
 *
 * This is a replacement, not an edit. That is the whole risk of the control:
 * submitting a shorter list silently stops tracking everything omitted, and a
 * radar that stops watching a symbol produces no events for it rather than an
 * error. So the diff is computed and named before anything is written, and
 * what is being dropped is stated first.
 */

import React from 'react';
import useSWR, { mutate as globalMutate } from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { useFeedback } from './ui/Feedback';
import { POLL } from './ui/DataProvider';
import { IconAlert } from './ui/icons';
import { formatNumber } from '../lib/format';

const LIST_KEY = '/watchlists/equities';
/** `MAX_WATCHLIST_LIMIT` in services/api_gateway/routes/watchlists.py. */
const MAX_TICKERS = 50;

interface WatchlistResponse {
  tickers?: string[];
  total_active?: number;
}

export function WatchlistSync() {
  const { toast, confirm } = useFeedback();
  const { data } = useSWR<WatchlistResponse>(LIST_KEY, fetcher, { refreshInterval: POLL.slow });
  const current = React.useMemo(() => data?.tickers ?? [], [data]);

  const [draft, setDraft] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);

  // Until the operator types, the box shows what is actually tracked. Starting
  // empty would make "submit" mean "stop tracking everything", which is not
  // what an empty box looks like it means.
  const text = draft ?? current.join(', ');

  const parsed = React.useMemo(
    () =>
      Array.from(
        new Set(
          text
            .split(/[\s,]+/)
            .map((t) => t.trim().toUpperCase())
            .filter(Boolean),
        ),
      ),
    [text],
  );

  const added = parsed.filter((t) => !current.includes(t));
  const dropped = current.filter((t) => !parsed.includes(t));
  const changed = added.length > 0 || dropped.length > 0;
  const tooMany = parsed.length > MAX_TICKERS;

  const submit = async () => {
    const ok = await confirm({
      title: 'Replace the tracked universe?',
      // Dropped first. It is the half that silently stops producing events.
      body:
        (dropped.length > 0
          ? `This stops tracking ${dropped.length} ${
              dropped.length === 1 ? 'symbol' : 'symbols'
            }: ${dropped.join(', ')}. The radar produces no events for a symbol it is not watching, which looks the same as a quiet symbol. `
          : '') +
        (added.length > 0 ? `It starts tracking ${added.join(', ')}. ` : '') +
        `The list becomes exactly these ${parsed.length}.`,
      confirmLabel: `Replace with ${parsed.length}`,
    });
    if (!ok) return;

    setBusy(true);
    try {
      await apiClient.put('/watchlists/equities/sync', { tickers: parsed });
      toast('success', 'Watchlist replaced', `${parsed.length} symbols now tracked.`);
      setDraft(null);
      await globalMutate(LIST_KEY);
    } catch (err) {
      toast('error', 'The watchlist was not changed.', describeApiError(err) ?? undefined);
    }
    setBusy(false);
  };

  return (
    <div className="space-y-2 px-3.5 py-3">
      <label className="sr-only" htmlFor="watchlist-tickers">
        Tracked tickers
      </label>
      <textarea
        id="watchlist-tickers"
        value={text}
        onChange={(e) => setDraft(e.target.value)}
        rows={3}
        placeholder="NVDA, AAPL, MSFT"
        className="w-full resize-y rounded-md border border-line bg-page px-2 py-1.5 font-mono text-micro uppercase text-ink outline-none focus:border-line-accent"
      />

      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="text-micro text-ink-mute">
          {tooMany ? (
            <span className="tone-caution">
              {formatNumber(parsed.length, { decimals: 0 })} symbols; the server keeps the first{' '}
              {MAX_TICKERS}.
            </span>
          ) : !changed ? (
            `${formatNumber(parsed.length, { decimals: 0 })} tracked, unchanged`
          ) : (
            <>
              {dropped.length > 0 && (
                <span className="tone-caution">
                  <IconAlert /> dropping {dropped.length}
                </span>
              )}
              {dropped.length > 0 && added.length > 0 && ' · '}
              {added.length > 0 && <span className="tone-positive">adding {added.length}</span>}
            </>
          )}
        </span>
        <div className="flex items-center gap-1.5">
          {draft !== null && (
            <button
              onClick={() => setDraft(null)}
              className="cursor-pointer rounded-md border border-line px-2 py-0.5 text-micro text-ink-mute hover:text-ink-dim"
            >
              Reset
            </button>
          )}
          <button
            onClick={submit}
            disabled={!changed || busy || parsed.length === 0}
            className="rounded-md border border-line-accent bg-accent-dim px-2.5 py-0.5 text-micro font-semibold text-accent transition-colors enabled:cursor-pointer enabled:hover:border-accent disabled:opacity-40"
          >
            {busy ? 'Replacing…' : 'Replace'}
          </button>
        </div>
      </div>
    </div>
  );
}

export default WatchlistSync;
