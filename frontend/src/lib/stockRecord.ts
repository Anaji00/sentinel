/**
 * The record of past trade ideas on one stock (`/predictions/track-record/slice?ticker=`), and the point where the model
 * portfolio was furthest behind the S&P 500. Both say how thin the evidence is, and an empty record says so plainly.
 */

import { trackLine, type TrackSlice } from './liveFigures';

/** One line for the stock page. A stock with no past ideas says that, which is not the same as a good record. */
export function stockRecordLine(ticker: string, s: TrackSlice | null | undefined, since: (iso: string) => string): { headline: string; detail: string } | null {
  if (!s) return null;
  if (s.n_calls == null || s.n_calls <= 0) {
    return { headline: `No past ideas on ${ticker}.`, detail: 'The model has not flagged this stock as a trade idea since the record began, so there is nothing to grade.' };
  }
  const t = trackLine(s, since);
  if (!t) return null;
  return { headline: t.headline.replace(/^Past (\S+) ideas/, `Past $1 ideas on ${ticker}`), detail: t.detail };
}

export interface NavPoint {
  day: string;
  nav: number;
  spy_nav: number;
}

/**
 * The day the book stood furthest behind the S&P 500, as a percentage-point gap between their two growth paths since the
 * start. Null with fewer than three points or when the book was never behind.
 */
export function furthestBehind(nav: readonly NavPoint[] | undefined): { index: number; day: string; gapPct: number } | null {
  if (!nav || nav.length < 3 || !(nav[0].nav > 0) || !(nav[0].spy_nav > 0)) return null;
  let worst = { index: -1, gap: 0 };
  nav.forEach((p, i) => {
    const gap = (p.nav / nav[0].nav - p.spy_nav / nav[0].spy_nav) * 100;
    if (gap < worst.gap) worst = { index: i, gap };
  });
  return worst.index < 0 ? null : { index: worst.index, day: nav[worst.index].day, gapPct: worst.gap };
}
