/**
 * When each data source last answered successfully, so a panel can say "Updated 2 min ago".
 *
 * DataProvider records every successful request here. A panel asks for its source's time; with none recorded
 * (the first request has not finished) it shows nothing, never a made-up time.
 */

const updated = new Map<string, number>();
const listeners = new Set<() => void>();

export function recordUpdate(key: string, now: number = Date.now()): void {
  updated.set(key, now);
  listeners.forEach((l) => l());
}

export const updatedAt = (key: string): number | null => updated.get(key) ?? null;
export const noUpdate = (): number | null => null;

export function subscribeUpdates(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Test hook: forget everything. */
export function resetFreshness(): void {
  updated.clear();
  listeners.forEach((l) => l());
}

/** "just now", "42 s ago", "2 min ago", "3 h ago", "2 days ago". A future time (clock skew) reads as just now. */
export function agoWords(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 10) return 'just now';
  if (s < 60) return `${s} s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min ago`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h} h ago`;
  return `${Math.floor(h / 24)} days ago`;
}

/** True when a panel has not refreshed for much longer than its own polling interval. */
export function isOverdue(ms: number, pollMs: number): boolean {
  return ms > Math.max(pollMs * 3, 60_000);
}
