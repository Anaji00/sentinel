'use client';

/**
 * What else has looked like this.
 *
 * The platform has embedded every enriched event since it was built --
 * 493,878 vectors at 768 dimensions when this was written -- and nothing has
 * ever queried them. The route's own docstring says the index was write-only,
 * and it stayed write-only after the route was added, because two things were
 * wrong with it at once: it named a collection that has never existed, and the
 * client library it imported is not installed in the gateway image. Both
 * failures reported the same sentence about Qdrant being unreachable.
 *
 * "Show me historically similar situations" is a different question from "show
 * me correlated tickers". Correlation finds what moves together; similarity
 * finds precedent. This is the first surface in the product that asks the
 * second one.
 *
 * Similarity is not significance. A 0.99 neighbour of a routine market anomaly
 * is another routine market anomaly, and the panel says the score rather than
 * implying the match means something -- the floor the server applies is shown
 * too, because "nothing above 0.55" and "nothing indexed" are different
 * answers.
 */

import React from 'react';
import useSWR from 'swr';
import { describeApiError, fetcher } from '../lib/api';
import { EmptyState } from './ui/EmptyState';
import { ClockTime } from './ui/ClockTime';
import { IconRadar } from './ui/icons';
import { formatNumber } from '../lib/format';

interface SimilarHit {
  event_id: string;
  type: string | null;
  domain: string | null;
  region: string | null;
  occurred_at: string | null;
  anomaly_score: number | null;
  similarity: number;
}

interface SimilarResponse {
  query_event_id: string;
  min_similarity: number;
  count: number;
  results: SimilarHit[];
}

interface SimilarEventsProps {
  eventId: string;
  /** The domain of the event being asked about, for the cross-domain toggle. */
  domain?: string | null;
  onOpen?: (eventId: string) => void;
}

export function SimilarEvents({ eventId, domain, onOpen }: SimilarEventsProps) {
  // Precedent from another domain is the more interesting answer and the
  // rarer one, so it is offered rather than assumed: a maritime event whose
  // nearest neighbours are all maritime tells you the corpus is consistent,
  // not that anything is going on.
  const [crossDomainOnly, setCrossDomainOnly] = React.useState(false);

  const query = new URLSearchParams({ limit: '6' });
  if (crossDomainOnly && domain) query.set('exclude_domain', domain);

  const { data, error, isLoading } = useSWR<SimilarResponse>(
    `/search/similar/${encodeURIComponent(eventId)}?${query.toString()}`,
    fetcher,
  );

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="stat-label flex items-center gap-1.5">
          <IconRadar />
          Similar precedent
        </span>
        {domain && (
          <button
            onClick={() => setCrossDomainOnly((v) => !v)}
            aria-pressed={crossDomainOnly}
            className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium transition-colors ${
              crossDomainOnly
                ? 'border-line-accent bg-accent-dim text-accent'
                : 'border-line text-ink-mute hover:text-ink-dim'
            }`}
          >
            Other domains only
          </button>
        )}
      </div>

      {error ? (
        // A 404 here means this event has no vector, which is a fact about the
        // event rather than a failure -- only enriched events are indexed.
        <p className="text-micro text-ink-mute">
          {describeApiError(error) === 'Endpoint not found'
            ? 'This event has no embedding indexed. Only enriched events are retrievable.'
            : (describeApiError(error) ?? 'Could not reach the index.')}
        </p>
      ) : isLoading && !data ? (
        <EmptyState kind="loading" title="Searching the corpus" />
      ) : (data?.results.length ?? 0) === 0 ? (
        <p className="text-micro text-ink-mute">
          {/* The corpus size is not quoted here. It was, as a literal, which is
              the exact habit this audit exists to remove -- and it would go
              stale the moment the indexer ran again. The state strip reads it
              from `/search/status`, which is where a served figure belongs. */}
          Nothing in the indexed corpus scores above{' '}
          {formatNumber(data?.min_similarity, { decimals: 2 })}
          {crossDomainOnly ? ' outside this domain' : ''}. That is an answer, not an outage.
        </p>
      ) : (
        <ul className="space-y-1">
          {data!.results.map((hit) => (
            <li key={hit.event_id}>
              <button
                onClick={() => onOpen?.(hit.event_id)}
                disabled={!onOpen}
                className="flex w-full items-baseline justify-between gap-2 rounded px-1.5 py-1 text-left text-micro transition-colors enabled:cursor-pointer enabled:hover:bg-overlay"
              >
                <span className="flex min-w-0 items-baseline gap-2">
                  <span className="font-mono tabular-nums text-accent">
                    {formatNumber(hit.similarity, { decimals: 3 })}
                  </span>
                  <span className="truncate text-ink-dim">{hit.type ?? 'unknown type'}</span>
                  {hit.domain && <span className="shrink-0 text-ink-mute">{hit.domain}</span>}
                </span>
                <ClockTime
                  value={hit.occurred_at}
                  seconds={false}
                  className="shrink-0 text-ink-mute"
                />
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default SimilarEvents;
