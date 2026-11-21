/**
 * Plain wording for the clustered grades in `/predictions/producer-grades` (`clustered`). The older keys count every alert
 * as independent, so their t values are far too large; these count one unit per family, stock and day and compare it with a
 * random stock the same day. A stored per-call t is never shown as proof.
 */

export interface ClusteredFamily {
  family: string;
  k: number;
  units: number | null;
  dates: number | null;
  mean_ratio_by_date: number | null;
  excess_over_control: number | null;
  t_over_dates: number | null;
  top3_date_share: number | null;
  verdict: string;
}

export interface ClusteredAgent {
  /** The agent's name, with a variant after a dot, such as `stock_correlation_agent.baseline`. */
  agent: string;
  /** The holding period in hours, or null on the row that covers all horizons. */
  horizon_h: number | null;
  basis: string;
  calls: number | null;
  dates: number | null;
  mean_signed_bps: number | null;
  t_over_dates: number | null;
  top3_date_share: number | null;
  hit_share_by_date?: number | null;
  verdict: string;
}

export interface Clustered {
  generated_at?: string | null;
  window_days?: number | null;
  note?: string | null;
  rows?: ClusteredFamily[];
  agents?: { rows?: ClusteredAgent[]; window_days?: number | null; min_dates?: number | null; note?: string | null } | null;
}

export type Tone = 'good' | 'warn' | 'plain' | 'quiet';

/** The grader's verdict on a family, as a plain sentence and a tone. Unknown text is shown as it came. */
export function familyVerdict(v: string): { text: string; tone: Tone } {
  const s = v.trim().toLowerCase();
  if (s.startsWith('moves more')) return { text: 'Moves more than a random stock the same day', tone: 'good' };
  if (s.startsWith('moves less')) return { text: 'Moves less than a random stock the same day', tone: 'warn' };
  if (s.startsWith('no difference')) return { text: 'No difference from a random stock the same day', tone: 'plain' };
  if (s.startsWith('too few')) return { text: 'Too few days to judge', tone: 'quiet' };
  return { text: v, tone: 'plain' };
}

/** An agent's verdict: only "not judged" is rewritten; anything else is shown as the grader wrote it. */
export function agentVerdict(v: string): { text: string; tone: Tone } {
  const s = v.trim().toLowerCase();
  if (s.startsWith('not judged')) return { text: 'Not judged: too few days', tone: 'quiet' };
  return { text: v, tone: 'plain' };
}

/** How concentrated the sample is: "11 days, 54% of cases on the three busiest". Empty when the count is missing. */
export function datesNote(dates: number | null | undefined, top3: number | null | undefined): string {
  if (dates == null) return '';
  const spread = top3 != null ? `, ${Math.round(top3 * 100)}% of cases on the three busiest` : '';
  return `${dates} day${dates === 1 ? '' : 's'}${spread}`;
}

/** `stock_correlation_agent.baseline` as "stock correlation agent (baseline)". */
export function agentLabel(r: ClusteredAgent): string {
  const [base, ...variant] = (r.agent || 'agent').split('.');
  const name = base.replace(/_/g, ' ');
  return variant.length > 0 ? `${name} (${variant.join(' ').replace(/_/g, ' ')})` : name;
}

/** A holding period in hours as words: 24 is "1 day", 120 is "5 days", null is "all horizons". */
export function horizonHoursWords(h: number | null | undefined): string {
  if (h == null) return 'all horizons';
  if (h >= 24 && h % 24 === 0) return `${h / 24} day${h === 24 ? '' : 's'}`;
  return `${h} hour${h === 1 ? '' : 's'}`;
}

/** The clustered row for an alert family at five sessions, matched by exact name; null when there is none. */
export function gradeFor(c: Clustered | null | undefined, family: string | null | undefined): ClusteredFamily | null {
  if (!family) return null;
  return (c?.rows ?? []).find((r) => r.k === 5 && r.family === family) ?? null;
}

const ORDER: Record<Tone, number> = { good: 0, warn: 1, plain: 2, quiet: 3 };

/** Families at one horizon (k sessions), those with a finding first, then by size of the gap to the control. */
export function familyRows(c: Clustered | null | undefined, k: number): ClusteredFamily[] {
  return (c?.rows ?? [])
    .filter((r) => r.k === k)
    .sort((a, b) => ORDER[familyVerdict(a.verdict).tone] - ORDER[familyVerdict(b.verdict).tone] || (b.excess_over_control ?? -Infinity) - (a.excess_over_control ?? -Infinity));
}

/** The agent rows judged against the market with beta removed, which is the only basis to read. */
export function betaAgentRows(c: Clustered | null | undefined): ClusteredAgent[] {
  return (c?.agents?.rows ?? []).filter((r) => r.basis === 'excess_beta');
}

/** One honest sentence on whether any agent shows a measured edge. Null when there is nothing to base it on. */
export function agentHeadline(rows: ClusteredAgent[]): string | null {
  if (rows.length === 0) return null;
  const judged = rows.filter((r) => !r.verdict.toLowerCase().startsWith('not judged'));
  if (judged.length === 0) return 'No agent has been on enough different days to judge yet.';
  const edge = judged.filter((r) => (r.t_over_dates ?? 0) >= 2 && (r.mean_signed_bps ?? 0) > 0);
  return edge.length === 0
    ? `${judged.length} agent result${judged.length === 1 ? '' : 's'} could be judged, and none shows a measured edge once market moves and clustered days are allowed for.`
    : `${edge.length} agent result${edge.length === 1 ? '' : 's'} show a measured edge counted by day; read the days column before trusting it.`;
}
