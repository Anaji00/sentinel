/**
 * Putting two stocks side by side from their daily closes: both rebased to the same starting point so the lines can be
 * read against each other, the return of each over a month, three months and the whole window, and how closely their
 * daily moves track. Everything is computed from the closes the model itself reads. A figure that the history cannot
 * support is absent, never zero.
 */

export interface Close {
  day: string;
  close: number;
}

export interface Aligned {
  days: string[];
  a: number[];
  b: number[];
}

/** Closes for the days both stocks traded, oldest first. Non-positive or non-finite closes are dropped. */
export function alignCloses(a: readonly Close[] | null | undefined, b: readonly Close[] | null | undefined): Aligned {
  const valid = (c: Close) => typeof c.close === 'number' && Number.isFinite(c.close) && c.close > 0;
  const bMap = new Map((b ?? []).filter(valid).map((c) => [c.day, c.close]));
  const days: string[] = [];
  const av: number[] = [];
  const bv: number[] = [];
  for (const c of [...(a ?? [])].filter(valid).sort((x, y) => x.day.localeCompare(y.day))) {
    const other = bMap.get(c.day);
    if (other !== undefined) {
      days.push(c.day);
      av.push(c.close);
      bv.push(other);
    }
  }
  return { days, a: av, b: bv };
}

/** Percent change from the first value, point by point: both lines start at 0. */
export function rebased(values: readonly number[]): number[] {
  if (values.length === 0) return [];
  const first = values[0];
  return values.map((v) => (v / first - 1) * 100);
}

/** Percent change over the last `sessions` closes, or null when the history is shorter. */
export function returnOver(values: readonly number[], sessions: number): number | null {
  if (values.length <= sessions) return null;
  const then = values[values.length - 1 - sessions];
  return then > 0 ? (values[values.length - 1] / then - 1) * 100 : null;
}

/** Correlation of the two stocks' daily returns: 1 moves in lockstep, 0 unrelated, negative opposite. Null on thin data. */
export function dailyCorrelation(a: readonly number[], b: readonly number[], minObs = 20): number | null {
  const n = Math.min(a.length, b.length) - 1;
  if (n < minObs) return null;
  const ra: number[] = [];
  const rb: number[] = [];
  for (let i = 1; i <= n; i++) {
    ra.push(a[i] / a[i - 1] - 1);
    rb.push(b[i] / b[i - 1] - 1);
  }
  const mean = (x: number[]) => x.reduce((s, v) => s + v, 0) / x.length;
  const ma = mean(ra);
  const mb = mean(rb);
  let cov = 0;
  let va = 0;
  let vb = 0;
  for (let i = 0; i < ra.length; i++) {
    cov += (ra[i] - ma) * (rb[i] - mb);
    va += (ra[i] - ma) ** 2;
    vb += (rb[i] - mb) ** 2;
  }
  return va > 0 && vb > 0 ? cov / Math.sqrt(va * vb) : null;
}

export interface Side {
  last: number;
  month: number | null;
  quarter: number | null;
  window: number;
}

export interface Comparison {
  sessions: number;
  a: Side;
  b: Side;
  correlation: number | null;
}

export const MONTH = 21;
export const QUARTER = 63;

export function compare(aligned: Aligned): Comparison | null {
  if (aligned.days.length < 2) return null;
  const side = (v: number[]): Side => ({ last: v[v.length - 1], month: returnOver(v, MONTH), quarter: returnOver(v, QUARTER), window: (v[v.length - 1] / v[0] - 1) * 100 });
  return { sessions: aligned.days.length, a: side(aligned.a), b: side(aligned.b), correlation: dailyCorrelation(aligned.a, aligned.b) };
}

/** What the correlation means, in a few plain words. */
export function correlationWords(c: number | null): string {
  if (c == null) return 'not enough shared days to say';
  if (c >= 0.8) return 'they usually move together';
  if (c >= 0.5) return 'they often move together';
  if (c >= 0.2) return 'they move together a little';
  if (c > -0.2) return 'their daily moves are mostly unrelated';
  return 'they tend to move in opposite directions';
}

/** One sentence on who did better over the whole window, with the gap in percentage points. */
export function leaderSentence(aTicker: string, bTicker: string, c: Comparison | null): string | null {
  if (!c) return null;
  const gap = c.a.window - c.b.window;
  if (Math.abs(gap) < 0.5) return `${aTicker} and ${bTicker} are level over the last ${c.sessions} trading days.`;
  const [lead, trail] = gap > 0 ? [aTicker, bTicker] : [bTicker, aTicker];
  return `${lead} has done ${Math.abs(gap).toFixed(1)} percentage points better than ${trail} over the last ${c.sessions} trading days.`;
}
