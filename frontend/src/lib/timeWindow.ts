/**
 * The Intel console's time window: keep only what was reported within the
 * last N hours. Shared by the feed and the map so one slider narrows both.
 */

/** Events reported within `hours` of `now`; no window (null or undefined) keeps everything. */
export function withinHours<T extends { occurred_at?: string | null }>(
  events: T[],
  hours: number | null | undefined,
  now: number = Date.now(),
): T[] {
  if (hours == null || !Number.isFinite(hours) || hours <= 0) return events;
  const since = now - hours * 3_600_000;
  return events.filter((e) => {
    const t = e.occurred_at ? Date.parse(e.occurred_at) : NaN;
    // An undated report cannot be placed in a window; it stays rather than vanishing.
    return Number.isNaN(t) || t >= since;
  });
}
