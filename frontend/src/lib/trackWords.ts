/**
 * Wording rules for graded calls and paper orders, so a screen never says more than the record supports.
 *
 * A hit rate on few calls, or without the rate chance alone would give, proves little: an always-down
 * forecaster was right 70% of the time in a market that fell 70% of the time, with no skill at all.
 */

/** Fewer resolved calls than this and a rate is shown as "too few to grade", not as a number. */
export const MIN_GRADED = 20;

export const tooFewToGrade = (n: number | null | undefined): boolean => (n ?? 0) < MIN_GRADED;

export const TOO_FEW = `too few to grade (needs ${MIN_GRADED})`;

/** Shown beside any hit rate. Skill is the gap to the chance rate, not the rate itself. */
export const HIT_RATE_CAUTION =
  'A hit rate on its own proves little: a forecaster who always said "down" was right 70% of the time in a market that fell 70% of the time, with no skill at all. What counts is the gap to what chance would have given, and at 55% to 60% accuracy that takes hundreds of calls to show.';

/** What a paper order's status means, in words that do not alarm when there is nothing to be alarmed about. */
export function orderStatusWords(status: string): { text: string; tone: 'neutral' | 'pending' | 'bad' } {
  switch (status) {
    case 'unknown':
      // A timeout: the order may be live at the broker. Never "rejected" or "failed".
      return { text: 'Not confirmed, being checked', tone: 'pending' };
    case 'duplicate':
      return { text: 'Already sent today', tone: 'neutral' };
    case 'rejected':
      return { text: 'Refused by the broker', tone: 'bad' };
    case 'filled':
      return { text: 'Filled', tone: 'neutral' };
    default:
      return { text: status ? status.replace(/_/g, ' ') : 'Sent', tone: 'neutral' };
  }
}

export interface OrderLike {
  status: string;
}

/** How many orders are in each state that needs saying: refused, and not yet confirmed. */
export function orderCounts(orders: readonly OrderLike[]): { rejected: number; unconfirmed: number } {
  return {
    rejected: orders.filter((o) => o.status === 'rejected').length,
    unconfirmed: orders.filter((o) => o.status === 'unknown').length,
  };
}

export type SkillVerdict = 'skill' | 'none' | 'worse than chance' | 'too few';

/** The chance-adjusted fields the track-record route carries on each trade-idea horizon. */
export interface SkillFields {
  n_called?: number;
  hit_called?: number | null;
  chance_hit?: number | null;
  heidke?: number | null;
  n_independent?: number | null;
  skill_verdict?: SkillVerdict | string | null;
}

const UNITS: Record<string, string> = { daily: 'days', weekly: 'weeks', monthly: 'months', events: 'periods', all: 'periods' };

export const independentUnit = (horizon: string): string => UNITS[horizon] ?? 'periods';

/** "about 4", "less than 1": independent periods are an estimate, so they are never shown to a decimal. */
export function aboutPeriods(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return 'few';
  if (n < 1) return 'less than 1';
  return `about ${Math.round(n)}`;
}

const pctText = (v: number) => `${Math.round(v * 100)}%`;

/**
 * One line for a horizon: the hit rate beside what chance would give, and how many independent periods
 * stand behind it. The verdict comes from the backend and is the only thing allowed to say "worse than
 * chance": a few repeated weeks can look alarming without being evidence. Null when the route sent no
 * chance-adjusted fields, so older payloads fall back to the plain rules.
 */
export function skillLine(s: SkillFields | null | undefined, horizon: string): string | null {
  if (!s || !s.skill_verdict) return null;
  const hit = s.hit_called;
  const chance = s.chance_hit;
  const base = hit != null && chance != null ? `hit ${pctText(hit)} against ${pctText(chance)} by chance` : 'hit rate not available';
  const periods = `${aboutPeriods(s.n_independent)} independent ${independentUnit(horizon)}`;
  switch (s.skill_verdict) {
    case 'skill':
      return `${base}; ${periods}: better than chance`;
    case 'worse than chance':
      return `${base}; ${periods}: worse than chance`;
    case 'none':
      return `${base}; ${periods}: no better than chance`;
    default:
      return `${base}; only ${periods} so far, ${TOO_FEW}`;
  }
}
