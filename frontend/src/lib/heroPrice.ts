/**
 * The number at the top of a stock page. It comes from the daily closes the chart draws; a newer trade replaces it only
 * when it differs from the last close, and is then measured from that close. Using a call's own "today" figure here
 * mixed bases: it was measured from the previous day's close while the price beside it was a later trade.
 */

export interface HeadlinePrice {
  price: number;
  /** 'close' is the last daily close; 'trade' is a newer price. */
  kind: 'close' | 'trade';
  /** Change in percent from the previous close (kind close) or from the last close (kind trade). */
  changePct: number | null;
}

/** A trade this close to the last close is the same price for display; show the close. */
const SAME = 0.0005;

/** Days since the model's scores were written, or null when the date is missing or unreadable. */
export function scoreAgeDays(asOf: string | null | undefined, nowMs: number): number | null {
  if (!asOf) return null;
  const t = Date.parse(`${asOf.slice(0, 10)}T00:00:00Z`);
  if (Number.isNaN(t) || nowMs <= 0) return null;
  return Math.max(0, Math.floor((nowMs - t) / 86_400_000));
}

/** Scores older than this many days are called out as out of date. The model runs nightly, so a weekend is three. */
export const SCORES_STALE_DAYS = 4;

export function headlinePrice(closes: number[], trade: number | null): HeadlinePrice | null {
  const last = closes.length ? closes[closes.length - 1] : null;
  const prev = closes.length > 1 ? closes[closes.length - 2] : null;
  const usable = trade != null && Number.isFinite(trade) && trade > 0;
  if (usable && last != null && Math.abs(trade / last - 1) > SAME) {
    return { price: trade, kind: 'trade', changePct: (trade / last - 1) * 100 };
  }
  if (last != null) return { price: last, kind: 'close', changePct: prev ? (last / prev - 1) * 100 : null };
  if (usable) return { price: trade, kind: 'trade', changePct: null };
  return null;
}
