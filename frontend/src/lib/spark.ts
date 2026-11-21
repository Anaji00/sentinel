/**
 * Trend lines inside table rows, from the batch closes route (`/stocks/closes`): daily closes per ticker, oldest first,
 * `[]` for a ticker with no bars. A line is drawn only with enough closes; otherwise the cell shows a dash.
 */

export const MIN_POINTS = 5;

export type Closes = Record<string, number[]>;

/** The closes that can be drawn: finite, positive, and at least MIN_POINTS of them. Null otherwise. */
export function drawable(values: readonly number[] | undefined): number[] | null {
  const v = (values ?? []).filter((x) => Number.isFinite(x) && x > 0);
  return v.length >= MIN_POINTS ? v : null;
}

/** Points for a polyline scaled into a w by h box with a 1px margin; a flat series runs along the middle. */
export function sparkPoints(values: readonly number[], w: number, h: number): string {
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  const span = hi - lo;
  return values
    .map((v, i) => {
      const x = (i / (values.length - 1)) * (w - 2) + 1;
      const y = span === 0 ? h / 2 : h - 1 - ((v - lo) / span) * (h - 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
}

/** 'up', 'down' or 'flat' from the first to the last close. */
export function trend(values: readonly number[]): 'up' | 'down' | 'flat' {
  const a = values[0];
  const b = values[values.length - 1];
  return b > a ? 'up' : b < a ? 'down' : 'flat';
}

/** "up 12.3% over 60 days" for the cell's text label. */
export function trendWords(values: readonly number[]): string {
  const a = values[0];
  const b = values[values.length - 1];
  const pct = (b / a - 1) * 100;
  const dir = trend(values);
  return dir === 'flat' ? `flat over ${values.length} days` : `${dir} ${Math.abs(pct).toFixed(1)}% over ${values.length} days`;
}

/** The tickers to ask for: unique, upper-case, at most 100 (the route's limit). */
export function closesKey(tickers: readonly string[], days = 60): string | null {
  const list = [...new Set(tickers.map((t) => t.trim().toUpperCase()).filter(Boolean))].slice(0, 100);
  return list.length ? `/stocks/closes?tickers=${encodeURIComponent(list.join(','))}&days=${days}` : null;
}
