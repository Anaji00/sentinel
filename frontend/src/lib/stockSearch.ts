/**
 * What the header search suggests as you type.
 *
 * Sources, all already on the page or in this browser: the stocks you follow, the ones you
 * opened lately, and the names the signal board already carries. Nothing is fetched per keystroke.
 * Whatever is typed is always offered as a ticker to open, so a stock outside the lists still works:
 * the drawer says plainly when it has nothing stored.
 */

import { cleanTicker, looksLikeTicker } from './openStock';

export interface NamedStock {
  ticker: string;
  name: string | null;
}

export type SuggestionKind = 'following' | 'recent' | 'match' | 'typed';

export interface Suggestion {
  ticker: string;
  name: string | null;
  kind: SuggestionKind;
}

const RANK: Record<SuggestionKind, number> = { following: 0, recent: 1, match: 2, typed: 3 };

export const MAX_SUGGESTIONS = 7;
export const MAX_RECENT = 6;

interface Candidate extends Suggestion {
  score: number;
}

function score(ticker: string, name: string | null, q: string, upper: string): number | null {
  if (ticker === upper) return 0;
  if (ticker.startsWith(upper)) return 1;
  const n = (name ?? '').toLowerCase();
  if (!n) return null;
  if (n.startsWith(q)) return 2;
  if (n.split(/[\s,.-]+/).some((w) => w.startsWith(q))) return 3;
  return n.includes(q) ? 4 : null;
}

export function suggest(input: {
  query: string;
  followed: readonly string[];
  recent: readonly string[];
  board: readonly NamedStock[];
  limit?: number;
}): Suggestion[] {
  const limit = input.limit ?? MAX_SUGGESTIONS;
  const names = new Map<string, string | null>();
  for (const b of input.board) names.set(cleanTicker(b.ticker), b.name);
  const nameOf = (t: string) => names.get(t) ?? null;

  const query = input.query.trim();
  const out = new Map<string, Candidate>();
  const add = (ticker: string, kind: SuggestionKind, s: number) => {
    const t = cleanTicker(ticker);
    if (!t) return;
    const prior = out.get(t);
    if (!prior || RANK[kind] < RANK[prior.kind] || (kind === prior.kind && s < prior.score)) {
      out.set(t, { ticker: t, name: nameOf(t), kind, score: s });
    }
  };

  if (!query) {
    input.followed.forEach((t) => add(t, 'following', 0));
    input.recent.forEach((t) => add(t, 'recent', 0));
    return [...out.values()].slice(0, limit).map(({ ticker, name, kind }) => ({ ticker, name, kind }));
  }

  const q = query.toLowerCase();
  const upper = cleanTicker(query);
  input.followed.forEach((t) => {
    const s = score(cleanTicker(t), nameOf(cleanTicker(t)), q, upper);
    if (s !== null) add(t, 'following', s);
  });
  input.recent.forEach((t) => {
    const s = score(cleanTicker(t), nameOf(cleanTicker(t)), q, upper);
    if (s !== null) add(t, 'recent', s);
  });
  names.forEach((name, t) => {
    const s = score(t, name, q, upper);
    if (s !== null) add(t, 'match', s);
  });

  const ranked = [...out.values()].sort((a, b) => a.score - b.score || RANK[a.kind] - RANK[b.kind] || a.ticker.localeCompare(b.ticker));
  const list: Suggestion[] = ranked.map(({ ticker, name, kind }) => ({ ticker, name, kind }));
  // Whatever was typed can be opened, even when it is in no list: last, so real matches lead.
  if (looksLikeTicker(query) && !out.has(upper)) return [...list.slice(0, limit - 1), { ticker: upper, name: null, kind: 'typed' }];
  return list.slice(0, limit);
}

/** The recent list with this ticker first, no repeats, at most MAX_RECENT. */
export function withRecent(list: readonly string[], ticker: string): string[] {
  const t = cleanTicker(ticker);
  if (!t) return [...list];
  return [t, ...list.filter((x) => x !== t)].slice(0, MAX_RECENT);
}

const RECENT_KEY = 'sentinel:recent-stocks';

type Store = Pick<Storage, 'getItem' | 'setItem'>;

function browserStore(): Store | null {
  try {
    return typeof window === 'undefined' ? null : window.localStorage;
  } catch {
    return null;
  }
}

/** Recently opened tickers from this browser; an empty list when storage is missing or holds nonsense. */
export function readRecent(store: Store | null = browserStore()): string[] {
  if (!store) return [];
  try {
    const parsed: unknown = JSON.parse(store.getItem(RECENT_KEY) ?? '[]');
    return Array.isArray(parsed) ? parsed.filter((x): x is string => typeof x === 'string').map(cleanTicker).filter(Boolean).slice(0, MAX_RECENT) : [];
  } catch {
    return [];
  }
}

export function writeRecent(list: readonly string[], store: Store | null = browserStore()): void {
  if (!store) return;
  try {
    store.setItem(RECENT_KEY, JSON.stringify(list.slice(0, MAX_RECENT)));
  } catch {
    /* not remembered; search still works */
  }
}
