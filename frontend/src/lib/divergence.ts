/**
 * Divergence alerts: a stock moving away from what its peers did. Each carries where the situation stands (a stage) and
 * a seven-section brief. The brief is rendered section by section, in the fixed order the backend writes it, and text
 * saying "not measured" is shown as it is: it is deliberate, and a section the platform could not fill is never padded.
 */

export type Stage = 'DEVELOPING' | 'CONFIRMED' | 'ESCALATING' | 'RESOLVED';

export interface Situation {
  stage: Stage;
  number?: number;
  sessions?: number;
  peak_z?: number;
  last_z?: number;
  first_session?: string;
  last_session?: string;
  news?: boolean;
}

export type Horizon = 'session' | '5d';

/** What the measurement window is, in words: a today-so-far gap reads differently from a five-session one. */
export function horizonWords(h: string | null | undefined): string | null {
  return h === 'session' ? 'today so far' : h === '5d' ? 'over 5 sessions' : null;
}

export interface Measures {
  horizon?: Horizon | string | null;
  /** The share of its group moving the other way, 0 to 1. */
  breadth?: number | null;
  /** How much the stock has usually moved for each 1% move of its group. */
  beta?: number | null;
  /** Today's volume as a multiple of the usual volume at this time of day; absent for 5-session rows. */
  relative_volume?: number | null;
}

const num = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);

/** The measurements as plain sentences. A measurement the alert did not carry is left out, never filled in. */
export function measuresLines(m: Measures | null | undefined): string[] {
  const out: string[] = [];
  if (!m) return out;
  if (num(m.breadth)) out.push(`${Math.round(m.breadth * 100)}% of its group moved the other way, so ${m.breadth >= 0.5 ? 'most of the group disagrees with this stock' : 'this is mostly one stock, not the whole group'}.`);
  if (num(m.beta)) out.push(`Beta ${m.beta.toFixed(2)}: for each 1% its group moves, this stock has usually moved about ${m.beta.toFixed(2)}%.`);
  if (num(m.relative_volume)) out.push(`Trading ${m.relative_volume.toFixed(1)} times its usual volume for this time of day.`);
  return out;
}

export type AlertKind = 'all' | 'divergence' | 'other';

/** True for a divergence alert: its tags say so, or it carries a situation. */
export function isDivergence(c: { tags?: string[] | null; situation?: unknown }): boolean {
  return Boolean(c.situation) || (c.tags ?? []).some((t) => t.toLowerCase() === 'divergence');
}

export const ALERT_KINDS: ReadonlyArray<{ id: AlertKind; label: string; hint: string }> = [
  { id: 'all', label: 'Everything', hint: 'Every alert' },
  { id: 'divergence', label: 'Stock vs its peers', hint: 'A stock moving away from what its peers did' },
  { id: 'other', label: 'Other rules', hint: 'Alerts from every other rule' },
];

export function filterAlerts<T extends { tags?: string[] | null; situation?: unknown }>(alerts: readonly T[], kind: AlertKind): T[] {
  if (kind === 'all') return [...alerts];
  return alerts.filter((c) => (kind === 'divergence') === isDivergence(c));
}

/** The stock an alert is about: the first word of its description, when that looks like a ticker. */
export function alertTicker(c: { description?: string | null }): string | null {
  const first = (c.description ?? '').trim().split(/\s+/)[0] ?? '';
  return /^[A-Z][A-Z0-9.-]{0,9}$/.test(first) ? first : null;
}

export interface Pickable {
  correlation_id: string;
  detected_at?: string | null;
  description?: string | null;
  tags?: string[] | null;
  situation?: { stage?: string } | null;
}

/** Open stock-versus-peer situations, newest first, one per stock (its latest alert), resolved ones left out. */
export function openSituationList<T extends Pickable>(alerts: readonly T[] | null | undefined): Array<T & { ticker: string }> {
  const seen = new Set<string>();
  const out: Array<T & { ticker: string }> = [];
  const sorted = [...(alerts ?? [])].sort((a, b) => String(b.detected_at ?? '').localeCompare(String(a.detected_at ?? '')));
  for (const a of sorted) {
    if (!isDivergence(a) || a.situation?.stage === 'RESOLVED') continue;
    const ticker = alertTicker(a);
    if (!ticker || seen.has(ticker)) continue;
    seen.add(ticker);
    out.push({ ...a, ticker });
  }
  return out;
}

export interface Brief {
  what_happened?: string;
  catalyst?: string;
  peers?: { reference?: string; members?: string[]; returns_known?: boolean } | null;
  evidence?: string;
  positioning?: string;
  watch?: string[];
  limits?: string[];
}

export interface StageView {
  word: string;
  tone: 'caution' | 'accent' | 'negative' | 'muted';
  meaning: string;
}

const STAGES: Record<Stage, StageView> = {
  DEVELOPING: { word: 'Developing', tone: 'muted', meaning: 'First flagged. It has not yet survived a close.' },
  CONFIRMED: { word: 'Confirmed', tone: 'accent', meaning: 'Flagged again after a later close, so it survived at least one.' },
  ESCALATING: { word: 'Escalating', tone: 'negative', meaning: 'The gap has widened well past the largest seen so far.' },
  RESOLVED: { word: 'Resolved', tone: 'muted', meaning: 'The gap has closed, or the stock is moving with its peers again.' },
};

/** The stage as a word, a tone and a sentence; null for a stage this code does not know, so nothing unknown is badged. */
export function stageView(stage: string | null | undefined): StageView | null {
  return stage && stage in STAGES ? STAGES[stage as Stage] : null;
}

/** One line on how long the situation has run and how big the gap got, from its measurements. */
export function situationLine(s: Situation | null | undefined): string | null {
  if (!s) return null;
  const parts: string[] = [];
  if (s.sessions != null) parts.push(`flagged on ${s.sessions} session${s.sessions === 1 ? '' : 's'}`);
  if (s.peak_z != null) parts.push(`peak gap ${s.peak_z} standard deviations`);
  if (s.last_z != null) parts.push(`latest ${s.last_z}`);
  if (s.first_session && s.last_session && s.first_session !== s.last_session) parts.push(`${s.first_session} to ${s.last_session}`);
  return parts.length ? parts.join(', ') : null;
}

export interface BriefBlock {
  key: string;
  label: string;
  /** One paragraph, or the items of a list. */
  lines: string[];
}

const LABELS: Array<[keyof Brief, string]> = [
  ['what_happened', 'What happened'],
  ['catalyst', 'What may have caused it'],
  ['peers', 'Peers it is measured against'],
  ['evidence', 'The evidence'],
  ['positioning', 'Positioning'],
  ['watch', 'What to watch'],
  ['limits', 'Limits'],
];

export const NOT_PROVIDED = 'not provided in this alert';

function peersLines(p: Brief['peers']): string[] {
  if (!p) return [NOT_PROVIDED];
  const ref = p.reference === 'industry' ? 'its industry group' : p.reference ? 'its closest peers' : null;
  const out: string[] = [];
  if (ref) out.push(`Compared with ${ref}.`);
  if (p.members?.length) out.push(p.members.join(', '));
  if (p.returns_known === false) out.push('Their returns are not available, so the names are shown without them.');
  return out.length ? out : [NOT_PROVIDED];
}

/** The seven sections in their fixed order. A missing one says so; it is never left out or filled in. */
export function briefBlocks(brief: Brief | null | undefined): BriefBlock[] {
  if (!brief) return [];
  return LABELS.map(([key, label]) => {
    if (key === 'peers') return { key, label, lines: peersLines(brief.peers) };
    const v = brief[key];
    const lines = Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string' && x.trim() !== '') : typeof v === 'string' && v.trim() ? [v] : [];
    return { key, label, lines: lines.length ? lines : [NOT_PROVIDED] };
  });
}

/** True for lines the backend marks as not measured, so they can be set apart from measurements. */
export const isNotMeasured = (line: string): boolean => /^not measured/i.test(line.trim()) || /\bnot measured\b/i.test(line);
