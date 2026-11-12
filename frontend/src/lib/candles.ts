/**
 * Price bars for the stock chart, as plain functions: clean what the route returns, work out the averages and the
 * summary line, and pick the bar under the pointer. The bars themselves come from `/radar/candles/{ticker}`, which
 * serves daily and longer from the consolidated daily table, so the chart and the price in the header agree.
 */

export interface Bar {
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface CandleResponse {
  ticker?: string;
  timeframe?: string;
  source?: string;
  count?: number;
  candles?: Partial<Bar>[];
}

export const FRAMES = [
  { key: '15m', label: '15 minutes', limit: 130 },
  { key: '1h', label: '1 hour', limit: 130 },
  { key: '1d', label: 'Day', limit: 160 },
  { key: '1w', label: 'Week', limit: 120 },
] as const;

export type FrameKey = (typeof FRAMES)[number]['key'];

const finite = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);

/**
 * Keeps only bars that can be drawn: every price a positive number, high at or above the rest, low at or below.
 * Sorted oldest first and one per timestamp, because the cache path returns newest first.
 */
export function cleanBars(raw: Partial<Bar>[] | undefined): Bar[] {
  const seen = new Map<string, Bar>();
  for (const c of raw ?? []) {
    if (typeof c.ts !== 'string' || !finite(c.open) || !finite(c.high) || !finite(c.low) || !finite(c.close)) continue;
    if (c.open <= 0 || c.high <= 0 || c.low <= 0 || c.close <= 0) continue;
    const high = Math.max(c.high, c.open, c.close);
    const low = Math.min(c.low, c.open, c.close);
    seen.set(c.ts, { ts: c.ts, open: c.open, high, low, close: c.close, volume: finite(c.volume) && c.volume > 0 ? c.volume : 0 });
  }
  return [...seen.values()].sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0));
}

/** Simple moving average of the closes; null until there are `n` bars to average. */
export function movingAverage(bars: Bar[], n: number): (number | null)[] {
  const out: (number | null)[] = [];
  let sum = 0;
  for (let i = 0; i < bars.length; i++) {
    sum += bars[i].close;
    if (i >= n) sum -= bars[i - n].close;
    out.push(i >= n - 1 ? sum / n : null);
  }
  return out;
}

export interface Range {
  lo: number;
  hi: number;
  maxVolume: number;
}

/** The price and volume range to scale to, padded a little so wicks do not touch the edge. */
export function barRange(bars: Bar[], extra: (number | null)[][] = []): Range | null {
  if (bars.length === 0) return null;
  let lo = Infinity;
  let hi = -Infinity;
  let maxVolume = 0;
  for (const b of bars) {
    lo = Math.min(lo, b.low);
    hi = Math.max(hi, b.high);
    maxVolume = Math.max(maxVolume, b.volume);
  }
  for (const s of extra) for (const v of s) if (v != null) { lo = Math.min(lo, v); hi = Math.max(hi, v); }
  const pad = (hi - lo || hi * 0.01) * 0.04;
  return { lo: lo - pad, hi: hi + pad, maxVolume };
}

export interface Summary {
  first: Bar;
  last: Bar;
  change: number;
  changePct: number;
  high: number;
  low: number;
}

/** Change from the first bar's open to the last bar's close, with the window's high and low. */
export function summarize(bars: Bar[]): Summary | null {
  if (bars.length === 0) return null;
  const first = bars[0];
  const last = bars[bars.length - 1];
  return {
    first,
    last,
    change: last.close - first.open,
    changePct: (last.close / first.open - 1) * 100,
    high: Math.max(...bars.map((b) => b.high)),
    low: Math.min(...bars.map((b) => b.low)),
  };
}

/** Which bar a pointer at `fraction` (0 to 1 across the plot) is over. */
export function barAt(fraction: number, count: number): number {
  if (count <= 0) return -1;
  return Math.min(count - 1, Math.max(0, Math.floor(fraction * count)));
}

/** Up bars are green, down bars red; an unchanged bar counts as up. */
export const isUp = (b: Bar): boolean => b.close >= b.open;

/** A label for a bar's time: the date for day bars and longer, date and time for intraday. */
export function barLabel(ts: string, frame: FrameKey): string {
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  const date = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: frame === '1w' ? 'numeric' : undefined, timeZone: 'UTC' });
  if (frame === '1d' || frame === '1w') return date;
  const time = d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' });
  return `${date}, ${time} New York time`;
}
