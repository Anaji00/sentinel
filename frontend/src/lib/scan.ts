/**
 * Plain wording for the Scan screen: the market's breadth in a sentence, sector tiles by size of move, and the
 * screener's columns. Shapes follow `/market/breadth`, `/market/heatmap` and `/stocks/screener`; a missing figure is
 * left out, never shown as zero.
 */

/**
 * Seconds to wait when the server answers 503 with Retry-After on a route it is still building for the first time, or
 * null for any other error. That answer means "warming", not "failed": the page shows its loading state and tries again.
 */
export function warmingSeconds(err: unknown): number | null {
  const r = (err as { response?: { status?: number; headers?: Record<string, unknown> } } | null)?.response;
  if (!r || r.status !== 503) return null;
  const raw = r.headers?.['retry-after'] ?? r.headers?.['Retry-After'];
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.min(120, Math.max(5, n)) : null;
}

/**
 * True for any 503 from a route the server builds on first request. The Retry-After header can be lost on the way
 * through the proxy, and a live check showed the panels reading "could not load" on a 503, so any 503 here means the
 * server is still building it, not that it failed.
 */
export function isBuilding(err: unknown): boolean {
  return (err as { response?: { status?: number } } | null)?.response?.status === 503;
}

/** Seconds to wait before asking again: the server's Retry-After if it came through, else 20; null when it is not a 503. */
export function buildingWait(err: unknown): number | null {
  return warmingSeconds(err) ?? (isBuilding(err) ? 20 : null);
}

export interface BreadthDay {
  day: string;
  n: number | null;
  advancers: number | null;
  decliners: number | null;
  unchanged: number | null;
  share_up: number | null;
  new_52w_highs: number | null;
  new_52w_lows: number | null;
  share_above_50d: number | null;
  share_above_200d: number | null;
}

export interface Breadth {
  as_of: string | null;
  universe: number | null;
  series: BreadthDay[];
  dropped_thin: string[];
  note: string | null;
}

/** One sentence on how many stocks took part in the day's move. Null with no usable day. */
export function weatherSentence(d: BreadthDay | null | undefined): string | null {
  const up = d?.share_up;
  if (up == null || !Number.isFinite(up)) return null;
  if (up >= 0.7) return 'Most stocks rose: a broad rally.';
  if (up >= 0.55) return 'More stocks rose than fell.';
  if (up > 0.45) return 'About as many rose as fell: a mixed day.';
  if (up > 0.3) return 'More stocks fell than rose.';
  return 'Most stocks fell: a broad sell-off.';
}

export interface Figure {
  label: string;
  value: string;
  title: string;
}

/** The three figures beside the sentence. */
export function weatherFigures(d: BreadthDay | null | undefined): Figure[] {
  if (!d) return [];
  const out: Figure[] = [];
  if (d.share_up != null) out.push({ label: 'Stocks up', value: `${Math.round(d.share_up * 100)}%`, title: `${d.advancers ?? '?'} rose, ${d.decliners ?? '?'} fell, among ${d.n ?? '?'} stocks worth $1 billion or more.` });
  if (d.new_52w_highs != null && d.new_52w_lows != null) out.push({ label: 'New yearly highs / lows', value: `${d.new_52w_highs} / ${d.new_52w_lows}`, title: 'Stocks closing at their highest or lowest price in 52 weeks.' });
  if (d.share_above_50d != null) out.push({ label: 'Above their 50-day average', value: `${Math.round(d.share_above_50d * 100)}%`, title: 'The share of stocks trading above their average price of the last 50 sessions.' });
  return out;
}

export interface HeatGroup {
  key: string;
  label: string;
  n: number;
  n_priced: number | null;
  change_pct: number | null;
  change_pct_median: number | null;
  breadth_up_share: number | null;
  volume_vs_20d_median: number | null;
  top: { ticker: string; change_pct: number }[];
  bottom: { ticker: string; change_pct: number }[];
}

export interface Heatmap {
  as_of: string | null;
  session: string | null;
  by: string;
  universe: number | null;
  groups: HeatGroup[];
  note: string | null;
}

/** A tile's colour step from -3 to 3 in fixed bands (0.25, 1 and 2 percent), so tiles compare across days. */
export function moveStep(pct: number | null | undefined): number {
  if (pct == null || !Number.isFinite(pct)) return 0;
  const a = Math.abs(pct);
  const step = a < 0.25 ? 0 : a < 1 ? 1 : a < 2 ? 2 : 3;
  return step === 0 ? 0 : pct < 0 ? -step : step;
}

export const signedPct = (v: number, digits = 2): string => `${v >= 0 ? '+' : '−'}${Math.abs(v).toFixed(digits)}%`;

/** "Energy rose 1.61%, with 76% of its stocks up", or null when the group has no statistics. */
export function groupSentence(g: HeatGroup): string | null {
  if (g.change_pct == null) return null;
  const share = g.breadth_up_share != null ? `, with ${Math.round(g.breadth_up_share * 100)}% of its stocks up` : '';
  return `${g.label} ${g.change_pct >= 0 ? 'rose' : 'fell'} ${Math.abs(g.change_pct).toFixed(2)}%${share}.`;
}

/** What the move is measured over, from the response's session word. */
export function sessionNote(session: string | null | undefined): string {
  return session === 'regular' ? 'Live moves during the regular session.' : 'Moves are the last completed session, close to close.';
}

export interface ScreenerRow {
  ticker: string;
  price: number | null;
  change_pct: number | null;
  volume_vs_20d: number | null;
  model_percentile: number | null;
  model_as_of: string | null;
  next_event: { kind: string; date: string; estimated?: boolean | null } | null;
  market_cap: number | null;
  sector?: string | null;
  industry?: string | null;
}

export interface Screener {
  as_of: string | null;
  model_as_of: string | null;
  horizon: string | null;
  universe: number | null;
  rows: ScreenerRow[];
}

export const SORTS = [
  { key: 'change_pct', label: 'Day change' },
  { key: 'rel_volume', label: 'Volume vs usual' },
  { key: 'model_pct', label: 'Model view' },
  { key: 'next_event', label: 'Next event' },
] as const;

export type SortKey = (typeof SORTS)[number]['key'];

export const SIZES = [
  { label: 'All sizes', min: 0 },
  { label: '$1B and up', min: 1e9 },
  { label: '$10B and up', min: 1e10 },
  { label: '$100B and up', min: 1e11 },
] as const;

/** The eleven sectors the heat map and the screener share, in the order the heat map lists them by size of group. */
export const SECTORS = ['Information Technology', 'Financials', 'Industrials', 'Health Care', 'Consumer Discretionary', 'Energy', 'Materials', 'Consumer Staples', 'Utilities', 'Real Estate', 'Communication Services'] as const;

export function screenerUrl(sort: SortKey, dir: 'asc' | 'desc', minCap: number, limit = 100, sector: string | null = null): string {
  return `/stocks/screener?sort=${sort}&dir=${dir}&limit=${limit}${minCap > 0 ? `&min_cap=${minCap}` : ''}${sector ? `&sector=${encodeURIComponent(sector)}` : ''}`;
}

/** "4.4 times its usual volume", or an empty string when unknown. */
export function volumeWords(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '';
  return `${v.toFixed(1)}×`;
}

export function eventWords(e: ScreenerRow['next_event'], day: (iso: string) => string): string {
  if (!e?.date) return '';
  const kind = e.kind === 'earnings' ? (e.estimated ? 'Earnings expected' : 'Earnings') : e.kind;
  return `${kind} ${day(e.date)}`;
}
