/**
 * The bull and bear case for a stock (GET and POST /stocks/{ticker}/thesis),
 * written on request by the local model from facts the platform holds. A model
 * call takes a minute or more, so the page asks once and then polls.
 */

export interface ThesisSource {
  id: string;
  text: string;
}

export interface ThesisPoint {
  point: string;
  sources: ThesisSource[];
}

export type ThesisStatus = 'none' | 'pending' | 'ready' | 'failed' | 'busy' | 'limited';

export interface ThesisState {
  ticker: string;
  status: ThesisStatus;
  message: string;
  generated_at: string | null;
  model: string | null;
  bull: ThesisPoint[];
  bear: ThesisPoint[];
  would_change_it: string;
  facts_used: number;
  disclaimer: string;
}

export const PENDING_POLL_MS = 4000;

export const thesisKey = (ticker: string): string => `/stocks/${encodeURIComponent(ticker)}/thesis`;

/** Poll while it is being written, and only then. */
export const thesisRefresh = (state: ThesisState | undefined): number =>
  state?.status === 'pending' ? PENDING_POLL_MS : 0;

/** Whether the button should be offered: nothing there, or the last try did not produce one. */
export const canAsk = (state: ThesisState | undefined): boolean =>
  !state || state.status === 'none' || state.status === 'failed' || state.status === 'busy';
