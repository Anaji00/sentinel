/**
 * Two figures for the top bar: how the paper book stands against the S&P 500, and how many stock-versus-peer situations
 * are open. Pure wording and counting; the components fetch. A figure that cannot be measured is absent, never zero.
 */

import { openSituationList } from './divergence';

export interface BookFigure {
  text: string;
  tone: 'positive' | 'negative' | 'neutral';
  title: string;
}

const pct = (v: number) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toFixed(2)}%`;

export function bookFigure(s: { return_pct?: number | null; spy_return_pct?: number | null; sessions?: number | null } | null | undefined): BookFigure | null {
  if (!s || typeof s.return_pct !== 'number' || !Number.isFinite(s.return_pct)) return null;
  const spy = typeof s.spy_return_pct === 'number' && Number.isFinite(s.spy_return_pct) ? s.spy_return_pct : null;
  const gap = spy == null ? null : s.return_pct - spy;
  return {
    text: spy == null ? pct(s.return_pct) : `${pct(s.return_pct)} vs S&P ${pct(spy)}`,
    tone: gap == null || Math.abs(gap) < 0.25 ? 'neutral' : gap > 0 ? 'positive' : 'negative',
    title: `The paper book since it started${s.sessions ? ` (${s.sessions} trading days)` : ''}, against the S&P 500 over the same days. A short record says little.`,
  };
}

/** Open situations: divergence alerts that are not resolved. One per stock, so a stock flagged twice counts once. */
export function openSituations(alerts: ReadonlyArray<{ correlation_id: string; description?: string | null; detected_at?: string | null; tags?: string[] | null; situation?: { stage?: string } | null }> | null | undefined): number {
  return openSituationList(alerts).length;
}
