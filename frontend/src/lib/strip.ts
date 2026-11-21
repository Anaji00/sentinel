/**
 * The followed-stocks strip (`/stocks/strip`): one row per stock with its price, move, next earnings, the model's rank
 * and any flags. Missing values stay missing; an estimated earnings date is called expected.
 */

export interface StripRow {
  ticker: string;
  price: number | null;
  change_pct: number | null;
  session: string | null;
  delayed: boolean | null;
  as_of: string | null;
  next_earnings: { date: string | null; estimated: boolean | null } | null;
  model_percentile: number | null;
  model_as_of: string | null;
  held_back_count: number | null;
  regime: { label: string | null; action: string | null; alert_at: string | null } | null;
}

export interface Strip {
  as_of: string | null;
  rows: StripRow[];
}

/** "Earnings Nov 17" or "Earnings expected Feb 3"; empty when there is no date. */
export function earningsWords(e: StripRow['next_earnings'], day: (iso: string) => string): string {
  if (!e?.date) return '';
  return `${e.estimated ? 'Earnings expected' : 'Earnings'} ${day(e.date)}`;
}

/** Flags worth a second look on a row, as short plain phrases. */
export function stripFlags(r: StripRow): string[] {
  const out: string[] = [];
  if (r.regime?.label) out.push(r.regime.label);
  if (r.held_back_count != null && r.held_back_count > 0) out.push(`${r.held_back_count} signals held back`);
  if (r.delayed) out.push('price delayed');
  return out;
}

/** The followed tickers as a request list: at most 50, upper-case, no repeats, empty entries dropped. */
export function stripTickers(followed: readonly string[] | undefined): string[] {
  const seen = new Set<string>();
  for (const t of followed ?? []) {
    const u = t.trim().toUpperCase();
    if (u) seen.add(u);
    if (seen.size >= 50) break;
  }
  return [...seen];
}
