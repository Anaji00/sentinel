'use client';

/**
 * The three surfaces that need a human and had nowhere for one to stand.
 *
 * Forty-one of ninety-four gateway routes had no caller anywhere in this app.
 * Most of that is fine -- a Stripe webhook has no business being called by a
 * browser. What was not fine is the shape of what was missing: every endpoint
 * whose whole purpose is a person deciding something.
 *
 *   /attribution/entities/merge-candidates + /alias + /reject-merge
 *       An entity-resolution review workflow. It exists precisely because a
 *       machine cannot tell whether "Maersk Line" and "A.P. Moller-Maersk" are
 *       one entity, and there was nowhere for the person to say.
 *   /feedback/rules
 *       Which rule firings were worth anything. `needs_review` is computed,
 *       stored, and was read by nothing.
 *   /dlq/summary + /events + /replay
 *       A failed-event backlog that could be neither seen nor drained.
 *
 * Each panel renders its own absence. A DLQ with nothing outstanding is a
 * finding; so is a merge list with no candidates. Neither is a blank box.
 */

import React, { useState } from 'react';
import useSWR, { mutate } from 'swr';
import { fetcher } from '../lib/api';
import { formatPercent } from '../lib/format';
import { EmptyState } from './ui/EmptyState';
import { useFeedback } from './ui/Feedback';
import { POLL } from './ui/DataProvider';
import { IconChevronDown } from './ui/icons';
import { ClockTime } from './ui/ClockTime';
import { ABSENT } from '../lib/format';
import { WatchlistSync } from './WatchlistSync';

interface DlqTopic {
  topic: string;
  total: number;
  outstanding: number;
}
interface DlqSummary {
  topics: DlqTopic[];
  total: number;
  outstanding: number;
}

interface MergeCandidate {
  pair_id: string;
  a: string;
  b: string;
  a_seen_as: string;
  b_seen_as: string;
  similarity: number;
}
interface MergeResponse {
  scanned: number;
  candidates: MergeCandidate[];
  reason?: string;
}

interface RuleFeedback {
  rule_id: string;
  total: number;
  negative: number;
  negative_share: number;
  needs_review: boolean;
}
interface FeedbackResponse {
  rules: RuleFeedback[];
}

const Panel: React.FC<{ title: string; note?: string; children: React.ReactNode }> = ({
  title,
  note,
  children,
}) => (
  <div className="flex flex-col min-h-0 bg-raised rounded-xl border border-amber-500/20 overflow-hidden">
    <div className="px-3.5 py-2 border-b border-amber-500/15 shrink-0 flex items-baseline gap-2">
      <span className="text-amber-300 font-extrabold tracking-wider uppercase text-micro">
        {title}
      </span>
      {note && <span className="text-micro text-ink-mute">{note}</span>}
    </div>
    <div className="flex-1 min-h-0 overflow-y-auto">{children}</div>
  </div>
);

/**
 * Loading, empty and error, for one SWR result.
 *
 * The three panels below each rendered their heading and then nothing while a
 * request was in flight -- measured at 66 characters for the whole page with
 * the backend unreachable. The docstring above claims every panel "renders its
 * own absence"; this is what makes that true rather than aspirational.
 */
/** One row of `/dlq/events`: a message that failed, and what killed it. */
interface FailedEvent {
  id: number;
  failed_at: string;
  topic: string;
  error: string;
  retry_count: number;
  permanently_failed: boolean;
  resolved: boolean;
  replay_count: number;
  last_replay_error: string | null;
}

interface FailedEventsResponse {
  count: number;
  limit: number;
  offset: number;
  events: FailedEvent[];
}

/** `/feedback/log`: the reasons analysts gave, most recent first. */
interface FeedbackLogEntry {
  correlation_id: string | null;
  rule_id: string | null;
  verdict: string;
  reason: string | null;
  created_at: string;
}

/** `/feedback/interaction/bands`: what readers did with what was surfaced. */
interface InteractionBands {
  total_surfaced: number;
  bands: Array<{
    band: string;
    surfaced: number;
    opened: number;
    acted: number;
    dismissed?: number;
    open_rate?: number | null;
    act_rate?: number | null;
  }>;
}

/** `/attribution/entities/resolve`: a spelling, and what it names. */
interface Resolution {
  input: string;
  canonical: string | null;
  structural_fold: string | null;
}

function Status<T>({
  q,
  children,
}: {
  q: { data?: T; error?: unknown; isLoading: boolean };
  children: (data: T) => React.ReactNode;
}) {
  if (q.error) {
    return (
      <EmptyState
        kind="error"
        title="Could not reach the gateway"
        detail="This panel reads from the API gateway. Unreachable is not the same as empty, and this is the first."
      />
    );
  }
  if (q.isLoading || q.data === undefined) {
    return <EmptyState kind="loading" title="Loading…" />;
  }
  return <>{children(q.data)}</>;
}

export default function OperationsConsole() {
  const [busy, setBusy] = useState<string | null>(null);
  const { confirm, toast } = useFeedback();

  const dlq = useSWR<DlqSummary>('/api/v1/dlq/summary', fetcher, { refreshInterval: POLL.slow });

  // The failures themselves, for whichever topic the operator opened. Fetched
  // on demand rather than alongside the summary: the rows carry full exception
  // text and there is no reason to pull them for topics nobody is looking at.
  const [openTopic, setOpenTopic] = useState<string | null>(null);
  const failures = useSWR<FailedEventsResponse>(
    openTopic
      ? `/api/v1/dlq/events?topic=${encodeURIComponent(openTopic)}&outstanding_only=true&limit=20`
      : null,
    fetcher,
  );
  const merges = useSWR<MergeResponse>(
    '/api/v1/attribution/entities/merge-candidates?limit=25',
    fetcher,
  );
  const feedback = useSWR<FeedbackResponse>('/api/v1/feedback/rules?limit=25', fetcher);

  // The verdicts themselves, not only their aggregate. A rule at 60% negative
  // tells an operator to look; the reasons tell them what at.
  const feedbackLog = useSWR<{ entries: FeedbackLogEntry[] }>(
    '/api/v1/feedback/log?limit=20',
    fetcher,
    { refreshInterval: POLL.slow },
  );

  // What readers did with what the platform surfaced, by score band. The
  // platform measures its own relevance here and nothing displayed it.
  const bands = useSWR<InteractionBands>('/api/v1/feedback/interaction/bands', fetcher, {
    refreshInterval: POLL.slow,
  });

  // A spelling, resolved to the subject it names. The merge-candidate panel
  // beside this one proposes folds; this answers the question directly.
  const [resolveInput, setResolveInput] = useState('');
  const [resolveQuery, setResolveQuery] = useState('');
  const resolution = useSWR<Resolution>(
    resolveQuery
      ? `/api/v1/attribution/entities/resolve?name=${encodeURIComponent(resolveQuery)}`
      : null,
    fetcher,
  );

  // Every write here is an irreversible decision by a person, so each one names
  // what it will do before it does it -- and says what happened afterwards.
  //
  // This used `window.confirm` and then reported nothing at all. A silent POST
  // is the UI version of a swallowed exception: a replay the gateway refused
  // looked exactly like one that worked.
  const decide = async (
    path: string,
    body: unknown,
    ask: { title: string; body: string; confirmLabel: string; destructive?: boolean },
    done: string,
    revalidate: string,
  ) => {
    if (!(await confirm(ask))) return;
    setBusy(path);
    try {
      // Spelled in full at each call site rather than built from a variable.
      // The contract check reads these literally -- it is how a component that
      // fetches a path the Next.js origin does not route gets caught before it
      // silently reaches a 404 page instead of the gateway.
      const res = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        // The status, because "it failed" and "you are not allowed to do that"
        // are different things to a person holding a dead-letter backlog.
        toast(
          'error',
          'The gateway refused that',
          `HTTP ${res.status} from ${path.replace('/api/proxy', '')}`,
        );
        return;
      }
      toast('success', done);
      await mutate(revalidate);
    } catch (err) {
      toast('error', 'Could not reach the gateway', err instanceof Error ? err.message : undefined);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="h-full w-full grid grid-cols-1 lg:grid-cols-3 gap-3 min-h-0">
      <Panel
        title="Dead letters"
        note={dlq.data ? `${dlq.data.outstanding} outstanding of ${dlq.data.total}` : undefined}
      >
        <Status q={dlq}>
          {(data) =>
            data.topics.length === 0 ? (
              <EmptyState
                kind="empty"
                title="No failed events"
                detail="The good state, and worth telling apart from an unreachable table."
              />
            ) : (
              <>
                {data.topics.map((t) => (
                  <div key={t.topic} className="px-3.5 py-2 border-b border-white/5">
                    <div className="flex items-baseline justify-between gap-2">
                      {/* The topic name is the control. Replay was the only
                          thing an operator could do here, and it was offered
                          without the exception that says whether replaying
                          could possibly work. */}
                      <button
                        onClick={() => setOpenTopic(openTopic === t.topic ? null : t.topic)}
                        aria-expanded={openTopic === t.topic}
                        className="flex min-w-0 cursor-pointer items-center gap-1 text-micro text-ink hover:text-accent"
                      >
                        <IconChevronDown
                          className={`shrink-0 transition-transform ${
                            openTopic === t.topic ? '' : '-rotate-90'
                          }`}
                        />
                        <span className="truncate">{t.topic}</span>
                      </button>
                      <span
                        className={`text-micro tabular-nums font-bold ${
                          t.outstanding > 0 ? 'text-rose-400' : 'text-emerald-400'
                        }`}
                      >
                        {t.outstanding}
                      </span>
                    </div>
                    <div className="mt-1 flex items-center justify-between">
                      <span className="text-micro text-ink-mute">{t.total} total</span>
                      {t.outstanding > 0 && (
                        <button
                          disabled={busy !== null}
                          onClick={() =>
                            decide(
                              '/api/proxy/api/v1/dlq/replay',
                              { topic: t.topic, limit: 100 },
                              {
                                title: 'Replay failed events?',
                                body: `Up to 100 outstanding events from ${t.topic} will be re-published to the pipeline. They will be enriched and correlated again, which can produce duplicate downstream findings.`,
                                confirmLabel: 'Replay 100',
                              },
                              `Replay queued for ${t.topic}`,
                              '/api/v1/dlq/summary',
                            )
                          }
                          className="text-micro uppercase tracking-wide px-2 py-0.5 rounded border border-amber-500/40 text-amber-300 hover:bg-amber-500/10 disabled:opacity-40"
                        >
                          Replay
                        </button>
                      )}
                    </div>

                    {openTopic === t.topic && (
                      <div className="mt-2 space-y-1.5 border-l border-line pl-2">
                        {failures.isLoading && !failures.data ? (
                          <p className="text-micro text-ink-mute">Reading the failures…</p>
                        ) : failures.error ? (
                          <p className="text-micro tone-caution">
                            Could not read the failed rows for this topic.
                          </p>
                        ) : (failures.data?.events.length ?? 0) === 0 ? (
                          <p className="text-micro text-ink-mute">
                            Nothing outstanding on this topic right now.
                          </p>
                        ) : (
                          failures.data!.events.map((e) => (
                            <div key={e.id} className="space-y-0.5">
                              <div className="flex items-baseline justify-between gap-2 text-micro">
                                <ClockTime value={e.failed_at} className="text-ink-mute" />
                                <span className="text-ink-mute">
                                  {e.retry_count} {e.retry_count === 1 ? 'retry' : 'retries'}
                                  {e.permanently_failed && (
                                    <span className="tone-negative"> · given up</span>
                                  )}
                                </span>
                              </div>
                              {/* The first line, which is the exception and the
                                  reason. The stack below it is for a log, not
                                  for a console -- the full text is on the
                                  title so it can still be read or copied. */}
                              <p
                                className="truncate font-mono text-micro tone-caution"
                                title={e.error}
                              >
                                {e.error.split('\n')[0]}
                              </p>
                              {e.last_replay_error && (
                                <p
                                  className="truncate font-mono text-micro tone-negative"
                                  title={e.last_replay_error}
                                >
                                  replay failed: {e.last_replay_error.split('\n')[0]}
                                </p>
                              )}
                            </div>
                          ))
                        )}
                      </div>
                    )}
                  </div>
                ))}
              </>
            )
          }
        </Status>
      </Panel>

      <Panel
        title="Entity merges"
        note={merges.data ? `${merges.data.scanned} names scanned` : undefined}
      >
        <Status q={merges}>
          {(data) =>
            data.reason ? (
              <EmptyState kind="empty" title="Queue unavailable" detail={data.reason} />
            ) : data.candidates.length === 0 ? (
              <EmptyState
                kind="empty"
                title="Nothing to review"
                detail="No pairs above the similarity floor — the resolver folded everything it could without a human, which is what this queue exists to produce."
              />
            ) : (
              <>
                {data.candidates.map((c) => (
                  <div key={c.pair_id} className="px-3.5 py-2 border-b border-white/5">
                    <div className="text-micro text-ink leading-snug">
                      <div className="truncate">{c.a}</div>
                      <div className="text-ink-mute">vs</div>
                      <div className="truncate">{c.b}</div>
                    </div>
                    <div className="mt-1 flex items-center justify-between gap-2">
                      <span className="text-micro text-ink-mute tabular-nums">
                        {formatPercent(c.similarity, { decimals: 1, from: 'ratio' })} similar
                      </span>
                      <span className="flex gap-1">
                        <button
                          disabled={busy !== null}
                          onClick={() =>
                            decide(
                              '/api/proxy/api/v1/attribution/entities/alias',
                              { alias: c.b, canonical: c.a },
                              {
                                title: 'Merge these entities?',
                                body: `Every future event naming"${c.b}"will resolve to"${c.a}". Past events keep the name they arrived with; this changes what happens next, not what already happened.`,
                                confirmLabel: 'They are the same',
                              },
                              `"${c.b}"now resolves to"${c.a}"`,
                              '/api/v1/attribution/entities/merge-candidates?limit=25',
                            )
                          }
                          className="text-micro uppercase px-2 py-0.5 rounded border border-emerald-500/40 text-emerald-300 hover:bg-emerald-500/10 disabled:opacity-40"
                        >
                          Same
                        </button>
                        <button
                          disabled={busy !== null}
                          onClick={() =>
                            decide(
                              '/api/proxy/api/v1/attribution/entities/reject-merge',
                              { pair_id: c.pair_id },
                              {
                                title: 'Keep these separate?',
                                body: 'The pair stops being suggested. The resolver will go on treating them as two entities.',
                                confirmLabel: 'They are different',
                              },
                              'Pair recorded as distinct',
                              '/api/v1/attribution/entities/merge-candidates?limit=25',
                            )
                          }
                          className="text-micro uppercase px-2 py-0.5 rounded border border-line-strong text-ink-dim hover:bg-white/5 disabled:opacity-40"
                        >
                          Different
                        </button>
                      </span>
                    </div>
                  </div>
                ))}
              </>
            )
          }
        </Status>
      </Panel>

      <Panel title="Tracked universe" note="the equities the radar watches">
        <WatchlistSync />
      </Panel>

      <Panel title="Entity resolution" note="what a spelling actually names">
        <div className="space-y-2 px-3.5 py-3">
          <div className="flex items-center gap-1.5">
            <label className="sr-only" htmlFor="resolve-name">
              Name to resolve
            </label>
            <input
              id="resolve-name"
              value={resolveInput}
              onChange={(e) => setResolveInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') setResolveQuery(resolveInput.trim());
              }}
              placeholder="NVIDIA Corporation"
              className="min-w-0 flex-1 rounded-md border border-line bg-page px-2 py-1 text-micro text-ink outline-none focus:border-line-accent"
            />
            <button
              onClick={() => setResolveQuery(resolveInput.trim())}
              disabled={!resolveInput.trim()}
              className="rounded-md border border-line px-2 py-1 text-micro font-medium text-ink-dim transition-colors enabled:cursor-pointer enabled:hover:border-line-strong enabled:hover:text-ink disabled:opacity-40"
            >
              Resolve
            </button>
          </div>
          {resolveQuery && (
            <Status q={resolution}>
              {(r) => (
                <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-micro">
                  <dt className="text-ink-mute">Asked</dt>
                  <dd className="font-mono text-ink-dim">{r.input}</dd>
                  <dt className="text-ink-mute">Canonical</dt>
                  <dd className="font-mono text-accent">{r.canonical || ABSENT}</dd>
                  <dt className="text-ink-mute">Structural fold</dt>
                  <dd className="font-mono text-ink-dim">{r.structural_fold || ABSENT}</dd>
                </dl>
              )}
            </Status>
          )}
          {!resolveQuery && (
            <p className="text-micro text-ink-mute">
              The same company arrives spelled a dozen ways. This is what the platform folds a given
              spelling to before anything else reads it.
            </p>
          )}
        </div>
      </Panel>

      <Panel
        title="What readers did"
        note={bands.data ? `${bands.data.total_surfaced} surfaced` : undefined}
      >
        <Status q={bands}>
          {(data) =>
            data.bands.length === 0 ? (
              <EmptyState
                kind="empty"
                title="Nothing surfaced yet"
                detail="The platform measures its own relevance from what readers open and act on. With nothing surfaced there is nothing to measure — which is not the same as a relevance of zero."
              />
            ) : (
              <>
                {data.bands.map((b) => (
                  <div key={b.band} className="border-b border-white/5 px-3.5 py-2">
                    <div className="flex items-baseline justify-between gap-2 text-micro">
                      <span className="font-mono text-ink">{b.band}</span>
                      <span className="text-ink-mute tabular-nums">{b.surfaced} surfaced</span>
                    </div>
                    <div className="mt-0.5 flex items-center gap-3 text-micro text-ink-mute">
                      <span>
                        opened <span className="text-ink-dim tabular-nums">{b.opened}</span>
                      </span>
                      <span>
                        acted <span className="text-ink-dim tabular-nums">{b.acted}</span>
                      </span>
                    </div>
                  </div>
                ))}
              </>
            )
          }
        </Status>
      </Panel>

      <Panel title="Verdict log" note="the reasons, most recent first">
        <Status q={feedbackLog}>
          {(data) =>
            data.entries.length === 0 ? (
              <EmptyState
                kind="empty"
                title="No verdicts recorded"
                detail="Nothing has been judged yet. The scorecard beside this is computed from these entries, so it stays empty until someone marks a correlation."
              />
            ) : (
              <>
                {data.entries.map((e, i) => (
                  <div
                    key={`${e.correlation_id}-${i}`}
                    className="border-b border-white/5 px-3.5 py-2"
                  >
                    <div className="flex items-baseline justify-between gap-2 text-micro">
                      <span className="truncate font-mono text-ink">
                        {e.rule_id || 'unattributed'}
                      </span>
                      <span className={e.verdict === 'useful' ? 'tone-positive' : 'tone-caution'}>
                        {e.verdict.replace('_', ' ')}
                      </span>
                    </div>
                    {e.reason && <p className="mt-0.5 text-micro text-ink-dim">{e.reason}</p>}
                    <ClockTime
                      value={e.created_at}
                      seconds={false}
                      className="text-micro text-ink-mute"
                    />
                  </div>
                ))}
              </>
            )
          }
        </Status>
      </Panel>

      <Panel title="Rule feedback" note="analyst verdicts per rule">
        <Status q={feedback}>
          {(data) =>
            data.rules.length === 0 ? (
              <EmptyState
                kind="empty"
                title="No verdicts yet"
                detail="needs_review is computed from the balance of them, so it stays false until a rule has enough feedback to judge — which is not the same as a rule judged well."
              />
            ) : (
              <>
                {data.rules.map((r) => (
                  <div key={r.rule_id} className="px-3.5 py-2 border-b border-white/5">
                    <div className="flex items-baseline justify-between gap-2">
                      <span className="text-ink text-micro truncate">{r.rule_id}</span>
                      {r.needs_review && (
                        <span className="text-micro uppercase tracking-wider text-rose-400 border border-rose-500/30 rounded px-1 shrink-0">
                          review
                        </span>
                      )}
                    </div>
                    <div className="mt-1 text-micro text-ink-mute tabular-nums">
                      {r.negative} negative of {r.total} ·{' '}
                      {formatPercent(r.negative_share, { decimals: 0, from: 'ratio' })}
                    </div>
                  </div>
                ))}
              </>
            )
          }
        </Status>
      </Panel>
    </div>
  );
}
