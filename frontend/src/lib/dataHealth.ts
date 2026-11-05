/**
 * Which data sources are failing right now, kept in one place.
 *
 * A panel that asks for data and gets an error used to look like a panel that
 * asked and found nothing: a list drawn from `data ?? []` is empty either way.
 * DataProvider records every failed request here and clears it on the next
 * success, so the shared spinner and a strip at the top of the page can say so.
 *
 * Failures count only after they last a few seconds, so a single dropped request
 * that the next poll repairs never flashes a warning. 401 and 404 are not
 * recorded: an expired session has its own redirect, and a 404 often means
 * "nothing collected yet" rather than "broken".
 */

export interface Failure {
  since: number;
  count: number;
}

export type Failures = Readonly<Record<string, Failure>>;

/** How long a source has to keep failing before anyone is told. */
export const GRACE_MS = 6_000;

const EMPTY: Failures = Object.freeze({});
let failures: Failures = EMPTY;
const listeners = new Set<() => void>();

function emit(next: Failures): void {
  failures = next;
  listeners.forEach((l) => l());
}

export function recordFailure(key: string, now: number = Date.now()): void {
  const prior = failures[key];
  emit({ ...failures, [key]: { since: prior?.since ?? now, count: (prior?.count ?? 0) + 1 } });
}

export function recordSuccess(key: string): void {
  if (!(key in failures)) return;
  const rest: Record<string, Failure> = { ...failures };
  delete rest[key];
  emit(Object.keys(rest).length ? rest : EMPTY);
}

export const snapshot = (): Failures => failures;
export const serverSnapshot = (): Failures => EMPTY;

export function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** How many sources have been failing for at least `graceMs`. */
export function failingFor(all: Failures, now: number, graceMs: number = GRACE_MS): number {
  return Object.values(all).filter((f) => now - f.since >= graceMs).length;
}

/**
 * What the shared spinner should say. Failure wins over slowness: a source that is erroring is not
 * "taking longer than usual", and the person needs the retry either way.
 */
export type SpinnerState = 'loading' | 'slow' | 'failed';

export function spinnerState(failing: number, patienceSpent: boolean): SpinnerState {
  if (failing > 0) return 'failed';
  return patienceSpent ? 'slow' : 'loading';
}

/** True when a request failed and there is nothing older to show: the case for a panel's own error. */
export function failedWithout(error: unknown, data: unknown): boolean {
  return Boolean(error) && (data === undefined || data === null);
}

/** Test hook: forget everything. */
export function resetDataHealth(): void {
  emit(EMPTY);
}
