/**
 * Stocks related to a stock (GET /stocks/{ticker}/related), from the nightly
 * peer tables. The note it carries is part of the answer: this is past
 * co-movement, and nothing here says which way a stock will go.
 */

export interface MovesWith {
  ticker: string;
  name: string;
  correlation: number;
  sessions: number;
  closeness: string;
}

export interface SimilarBusiness {
  ticker: string;
  name: string;
  similarity: number;
}

export interface RelatedStocks {
  ticker: string;
  as_of: string | null;
  stale: boolean;
  window_sessions: number;
  moves_with: MovesWith[];
  similar_business: SimilarBusiness[];
  empty_reason: 'not_computed' | 'none_found' | null;
  note: string;
}

export const relatedKey = (ticker: string): string => `/stocks/${encodeURIComponent(ticker)}/related`;

/** Why a stock has no related stocks, in a sentence a visitor can act on. */
export function emptyRelated(reason: RelatedStocks['empty_reason'], windowSessions: number): { title: string; detail: string } {
  if (reason === 'not_computed') {
    return {
      title: 'Not worked out yet',
      detail: 'The nightly job that measures which stocks move together has not produced a result.',
    };
  }
  return {
    title: 'No close relatives found',
    detail: `No other stock tracked it closely enough over the last ${windowSessions} trading days, or it is not in the set we measure.`,
  };
}

/** "Nov 5" for a stored date, or '' when there is none. */
export function shortDate(iso: string | null | undefined): string {
  if (!iso) return '';
  const d = new Date(`${iso}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}
