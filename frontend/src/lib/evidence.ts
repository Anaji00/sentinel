/**
 * The one-call evidence bundle for a stock (`/stocks/{ticker}/evidence`): quote, snapshot, held-back signals, regime,
 * producer grades, positioning and the model's rank, each with its own status so one slow source cannot blank the page.
 * `ok` has data, `missing` means the source answered and has nothing (the note says why), `error` means it failed.
 */

import type { Quote } from './liveFigures';
import { formatNumber } from './format';
import type { Snapshot } from './snapshot';
import type { HeldBackResponse } from './heldBack';
import type { Regime } from './divergence';

export type SectionStatus = 'ok' | 'missing' | 'error';

export interface Section<T> {
  status: SectionStatus;
  data: T | null;
  note: string | null;
}

export interface ProducerGrade {
  rule: string;
  family: string;
  graded: 'rule' | 'family';
  verdict: string;
  n: number | null;
  mean_ratio: number | null;
  t: number | null;
  q: number | null;
}

export interface RegimeSection extends Regime {
  alert_at?: string | null;
  alert_rule?: string | null;
}

export interface Evidence {
  ticker: string;
  as_of: string | null;
  sections: {
    quote?: Section<Quote>;
    snapshot?: Section<Snapshot>;
    held_back?: Section<HeldBackResponse>;
    regime?: Section<RegimeSection>;
    producer_grade?: Section<ProducerGrade[]>;
    positioning?: Section<Record<string, unknown>>;
    model?: Section<{ percentile: number | null; quintile: number | null; as_of: string | null }>;
  };
}

/** Today's options trading in plain words, from the positioning section; empty when no field is present. */
export function optionsWords(p: Record<string, unknown> | null | undefined): string[] {
  const live = (p?.live ?? null) as Record<string, unknown> | null;
  if (!live) return [];
  const n = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
  const calls = n(live.call_volume);
  const puts = n(live.put_volume);
  const ratio = n(live.put_call_ratio);
  const out: string[] = [];
  if (calls != null && puts != null) out.push(`${formatNumber(Math.round(calls), { decimals: 0 })} call contracts and ${formatNumber(Math.round(puts), { decimals: 0 })} put contracts traded.`);
  if (ratio != null) {
    out.push(
      ratio > 1.2
        ? `More puts than calls (${ratio.toFixed(2)} puts per call): more bets on a fall, or protection.`
        : ratio < 0.7
          ? `More calls than puts (${ratio.toFixed(2)} puts per call): more bets on a rise.`
          : `About as many puts as calls (${ratio.toFixed(2)} puts per call).`,
    );
  }
  return out;
}

/**
 * The latest short-interest row in plain words: how many days of normal trading it would take to buy back the shares sold
 * short, and the change since the last report. A description of the position, never a signal. Empty when no row.
 */
export function shortWords(p: Record<string, unknown> | null | undefined): string[] {
  const rows = Array.isArray(p?.short_interest) ? (p?.short_interest as Record<string, unknown>[]) : [];
  const latest = rows[0];
  if (!latest) return [];
  const n = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
  const days = n(latest.days_to_cover);
  const change = n(latest.change_pct);
  const date = typeof latest.settlement_date === 'string' ? latest.settlement_date : null;
  const out: string[] = [];
  if (days != null) out.push(`Short sellers would need about ${days.toFixed(1)} days of normal trading to buy their shares back${date ? ` (report for ${date})` : ''}.`);
  if (change != null) out.push(`Shares sold short ${change >= 0 ? 'rose' : 'fell'} ${Math.abs(change).toFixed(1)}% since the previous report.`);
  return out;
}

export const evidenceKey = (ticker: string): string => `/stocks/${encodeURIComponent(ticker)}/evidence`;

/** A section's data when it is ok, else null. */
export function okData<T>(s: Section<T> | undefined): T | null {
  return s && s.status === 'ok' ? s.data : null;
}

/** True when a source failed, as opposed to having nothing to say. */
export function sectionFailed(s: Section<unknown> | undefined): boolean {
  return s?.status === 'error';
}

/**
 * One plain sentence for a grade. The grader checks whether the stock then moved MORE than usual over five sessions,
 * not which way and not whether it was profitable, so the sentence says only that.
 */
export function gradeLine(g: ProducerGrade): string {
  const sample = g.n != null ? `${g.n} past cases` : 'few past cases';
  const verdict = g.verdict?.trim() || 'not graded';
  return `${g.graded === 'rule' ? 'This kind of alert' : 'Alerts from the same family'}: ${verdict} over five sessions (${sample}). This checks size of move, not direction or profit.`;
}
