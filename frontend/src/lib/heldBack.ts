/**
 * Two plain-language views over backend routes: the signals held back on a stock (`/signals/held-back/{ticker}`) and the
 * free-tier model quota (`/health/model-quota`). Both arrive already in plain sentences, so this only groups and words.
 */

export interface HeldBackRow {
  signal: string;
  expected_move: 'rise' | 'fall' | 'none';
  window: string;
  held_back_by: string;
  explanation: string;
  measured?: number | null;
  required?: number | null;
  when: string;
}

export interface HeldBackResponse {
  ticker: string;
  count: number;
  note: string | null;
  held_back: HeldBackRow[];
}

export interface HeldBackGroup {
  key: string;
  signal: string;
  expectedMove: HeldBackRow['expected_move'];
  heldBackBy: string;
  explanation: string;
  windows: string[];
  measured: number | null;
  required: number | null;
  when: string;
}

export const heldBackKey = (ticker: string): string => `/signals/held-back/${encodeURIComponent(ticker)}?limit=50`;

/** The route repeats a row per window; one line per signal is easier to read, with the windows listed on it. */
export function groupHeldBack(rows: HeldBackRow[] | undefined): HeldBackGroup[] {
  const out = new Map<string, HeldBackGroup>();
  for (const r of rows ?? []) {
    const key = `${r.signal}|${r.expected_move}|${r.held_back_by}`;
    const g = out.get(key);
    if (g) {
      if (!g.windows.includes(r.window)) g.windows.push(r.window);
      if (r.when > g.when) g.when = r.when;
    } else {
      out.set(key, {
        key,
        signal: r.signal,
        expectedMove: r.expected_move,
        heldBackBy: r.held_back_by,
        explanation: r.explanation,
        windows: [r.window],
        measured: r.measured ?? null,
        required: r.required ?? null,
        when: r.when,
      });
    }
  }
  return [...out.values()].sort((a, b) => (a.when < b.when ? 1 : -1));
}

export function expectedMoveWords(m: HeldBackRow['expected_move']): string {
  return m === 'rise' ? 'Would have expected a rise' : m === 'fall' ? 'Would have expected a fall' : 'No direction expected';
}

export interface QuotaModel {
  model: string;
  provider: string;
  status: 'ok' | 'near limit' | 'at limit' | 'cooling down' | 'no key set';
  cooling_down_seconds: number | null;
  requests_this_minute: number | null;
  requests_per_minute_limit: number | null;
  requests_today: number | null;
  requests_per_day_limit: number | null;
  share_of_limit_used: number | null;
}

export interface QuotaResponse {
  count: number;
  note: string | null;
  models: QuotaModel[];
}

/** One plain line for a model's state. A limit of null means the provider sets none. */
export function quotaLine(m: QuotaModel): string {
  if (m.status === 'no key set') return 'No key is set, so this model is not in use.';
  if (m.status === 'cooling down') {
    const s = m.cooling_down_seconds ?? 0;
    return `Paused by the provider; trying again in ${s >= 120 ? `${Math.round(s / 60)} minutes` : `${s} seconds`}.`;
  }
  const day =
    m.requests_per_day_limit == null
      ? `${m.requests_today ?? 0} requests today, no daily limit`
      : `${m.requests_today ?? 0} of ${m.requests_per_day_limit} requests today`;
  if (m.status === 'at limit') return `At its limit: ${day}.`;
  if (m.status === 'near limit') return `Close to its limit: ${day}.`;
  return `${day}.`;
}

export function quotaTone(m: QuotaModel): string {
  return m.status === 'ok' ? 'text-positive' : m.status === 'near limit' ? 'text-caution' : m.status === 'no key set' ? 'text-ink-mute' : 'text-negative';
}
