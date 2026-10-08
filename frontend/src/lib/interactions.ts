import { apiClient } from './api';

/**
 * What the reader did with an alert.
 *
 * The consequence gap: the platform could say whether a scenario confirmed, and
 * nothing else. That measures the platform agreeing with itself. What score
 * bands actually precede a person doing something was unanswerable, because no
 * analyst action was recorded anywhere.
 *
 * `surfaced` is the denominator and is the part that is easy to forget: "the
 * 0.9 band was opened 40 times" is meaningless until you know whether it was
 * shown 50 times or 40,000.
 *
 * Fire-and-forget by design. This is instrumentation; a failed write must never
 * be visible to the reader, and must never block the render that triggered it.
 */
export type InteractionAction = 'surfaced' | 'opened' | 'dismissed' | 'acted_on';

// Once per alert per session per action. Without this, `surfaced` fires on
// every re-render and every SWR revalidation, and the denominator becomes a
// count of React renders rather than of alerts a person could have read.
const emitted = new Set<string>();

// A page surfaces dozens of rows at once; one request per row was ~90 POSTs per
// load. Interactions are buffered briefly and sent as one batch.
const FLUSH_DELAY_MS = 1500;
const MAX_BATCH = 500;

interface InteractionBody {
  action: InteractionAction;
  score: number;
  correlation_id: string | null;
  rule_id: string | null;
}

let pending: { key: string; body: InteractionBody }[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;

function flush(): void {
  flushTimer = null;
  const batch = pending.slice(0, MAX_BATCH);
  pending = pending.slice(MAX_BATCH);
  if (pending.length > 0) flushTimer = setTimeout(flush, FLUSH_DELAY_MS);
  if (batch.length === 0) return;
  void apiClient
    .post('/feedback/interaction/batch', { items: batch.map((p) => p.body) })
    .catch(() => {
      // Instrumentation never surfaces. Allow a retry on the next render
      // rather than silently recording nothing for the rest of the session.
      for (const p of batch) emitted.delete(p.key);
    });
}

export function recordInteraction(
  action: InteractionAction,
  score: number | null | undefined,
  ids: { correlationId?: string | null; ruleId?: string | null } = {},
): void {
  const numeric = typeof score === 'number' && Number.isFinite(score) ? score : null;
  if (numeric === null) return; // An interaction with no score says nothing about a band.

  const key = `${action}:${ids.correlationId || ids.ruleId || numeric.toFixed(4)}`;
  if (emitted.has(key)) return;
  emitted.add(key);

  pending.push({
    key,
    body: {
      action,
      score: Math.max(0, Math.min(1, numeric)),
      correlation_id: ids.correlationId || null,
      rule_id: ids.ruleId || null,
    },
  });
  if (flushTimer === null) flushTimer = setTimeout(flush, FLUSH_DELAY_MS);
}
