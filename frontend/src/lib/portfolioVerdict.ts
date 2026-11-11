/**
 * The one-word answer the Performance page leads with: is the model portfolio ahead of the S&P 500?
 *
 * The word follows the measured gap, and the note says how much history stands behind it, because a
 * few weeks of results cannot tell skill from luck and the page must not imply otherwise.
 */

export type VerdictTone = 'positive' | 'negative' | 'neutral';

export interface Verdict {
  word: string;
  tone: VerdictTone;
  note: string;
}

/** Below this many trading days, a gap says little. */
export const MIN_SESSIONS = 60;
/** A gap this small (in percentage points) is called level. */
export const LEVEL_BAND = 0.25;

export function portfolioVerdict(excessPct: number | null | undefined, sessions: number | null | undefined): Verdict {
  if (excessPct == null || !sessions || sessions < 1) {
    return { word: 'Too early', tone: 'neutral', note: 'No trading days have been scored yet.' };
  }
  const days = `${sessions} trading day${sessions === 1 ? '' : 's'}`;
  const note =
    sessions < MIN_SESSIONS
      ? `Only ${days} so far: too early to tell skill from luck.`
      : `Over ${days}.`;
  if (Math.abs(excessPct) < LEVEL_BAND) return { word: 'Level with the S&P 500', tone: 'neutral', note };
  return excessPct > 0
    ? { word: 'Ahead of the S&P 500', tone: 'positive', note }
    : { word: 'Behind the S&P 500', tone: 'negative', note };
}
