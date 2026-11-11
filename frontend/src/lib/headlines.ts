/**
 * The one number each page opens with, and the words around it. Pure: the components fetch, these decide
 * what is said, so a headline never claims more than the data under it.
 */

import { formatNumber } from './format';

export type HeadlineTone = 'positive' | 'negative' | 'neutral';

export interface HeadlineView {
  value: string;
  label: string;
  detail: string;
  tone: HeadlineTone;
}

interface BoardRowLike {
  net: number;
  confidence?: string | null;
}

/** Calls: how many stocks have strong evidence leaning one way today. */
export function callsHeadline(board: readonly BoardRowLike[] | null | undefined): HeadlineView | null {
  if (!board) return null;
  const strong = board.filter((r) => r.confidence === 'higher' && r.net !== 0);
  const up = strong.filter((r) => r.net > 0).length;
  const down = strong.length - up;
  if (strong.length === 0) {
    return { value: '0', label: 'stocks with strong evidence today', detail: 'Nothing leans one way with strong evidence right now.', tone: 'neutral' };
  }
  return {
    value: String(strong.length),
    label: strong.length === 1 ? 'stock with strong evidence today' : 'stocks with strong evidence today',
    detail: `${up} point up, ${down} point down. Tap one to see why.`,
    tone: up === down ? 'neutral' : up > down ? 'positive' : 'negative',
  };
}

const REGIME_WORD: Record<string, string> = { calm: 'A calm market', stressed: 'A stressed market', panic: 'A panicking market' };

/** Markets: where the S&P 500 is today and what kind of market it is. */
export function marketHeadline(pulse: { spy_change_pct?: number | null; regime?: string | null } | null | undefined): HeadlineView | null {
  if (!pulse || pulse.spy_change_pct == null) return null;
  const pct = pulse.spy_change_pct;
  const sign = pct > 0 ? '+' : pct < 0 ? '−' : '';
  return {
    value: `${sign}${Math.abs(pct).toFixed(2)}%`,
    label: 'S&P 500 today',
    detail: pulse.regime ? `${REGIME_WORD[pulse.regime] ?? pulse.regime}.` : 'Market mood not measured.',
    tone: pct > 0 ? 'positive' : pct < 0 ? 'negative' : 'neutral',
  };
}

/** Intel: how many conflict zones are in the news, and how many tracked ships have gone dark. */
export function intelHeadline(
  zones: number | null | undefined,
  tracked: number | null | undefined,
  dark: number | null | undefined,
): HeadlineView | null {
  if (zones == null) return null;
  const ships =
    tracked != null && dark != null && tracked > 0
      ? `${formatNumber(dark, { decimals: 0 })} of ${formatNumber(tracked, { decimals: 0 })} tracked ships have switched off their transponder.`
      : 'Ship tracking is not reporting right now.';
  return {
    value: String(zones),
    label: zones === 1 ? 'conflict zone in the news' : 'conflict zones in the news',
    detail: ships,
    tone: zones > 0 ? 'negative' : 'neutral',
  };
}
