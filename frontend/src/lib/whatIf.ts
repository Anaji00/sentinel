/**
 * Helpers for the "What if" form: turning what a person types into the request the gateway accepts
 * (POST /scenarios/request), and what the request's status means in words.
 */

export const TOPIC_MIN = 3;
export const TOPIC_MAX = 300;
export const MAX_TICKERS = 10;

const TICKER = /^[A-Z][A-Z0-9.-]{0,9}$/;

export interface ParsedRequest {
  topic: string;
  tickers: string[];
  /** Entries that are not symbols, shown back to the person so nothing is silently dropped. */
  invalid: string[];
}

export function parseRequest(topicText: string, tickerText: string): ParsedRequest {
  const topic = topicText.split(/\s+/).filter(Boolean).join(' ').slice(0, TOPIC_MAX);
  const tickers: string[] = [];
  const invalid: string[] = [];
  for (const raw of tickerText.split(/[\s,;]+/).filter(Boolean)) {
    const t = raw.replace(/^\$/, '').toUpperCase();
    if (!TICKER.test(t)) invalid.push(raw);
    else if (!tickers.includes(t)) tickers.push(t);
  }
  return { topic, tickers: tickers.slice(0, MAX_TICKERS), invalid };
}

/** Why the form cannot be sent yet, or null when it can. */
export function problem(p: ParsedRequest): string | null {
  if (p.topic.length < TOPIC_MIN) return 'Describe the development in a few words.';
  if (p.invalid.length) return `Not a stock symbol: ${p.invalid.join(', ')}.`;
  return null;
}

export type RequestStatus = 'queued' | 'running' | 'done' | 'failed';

export interface RequestState {
  status?: string;
  scenario_id?: string | number | null;
  headline?: string | null;
  error?: string | null;
}

export const isFinished = (s: RequestState | null | undefined): boolean => s?.status === 'done' || s?.status === 'failed';

export function statusWords(s: RequestState | null | undefined): string {
  switch (s?.status) {
    case 'queued':
      return 'Waiting its turn. A small model on this machine writes these, one at a time.';
    case 'running':
      return 'Writing the outcomes and what would confirm or rule out each. This takes a minute or two.';
    case 'done':
      return 'Finished.';
    case 'failed':
      return s.error ? `It could not be written: ${s.error}` : 'It could not be written. Try again in a few minutes.';
    default:
      return 'Sending…';
  }
}
