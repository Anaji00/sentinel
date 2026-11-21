/**
 * Which part of the US stock day it is, from the New York clock: pre-market from 4:00 to 9:30, regular hours to 16:00,
 * after hours to 20:00, closed otherwise and all weekend. Market holidays are not tracked, so on one of those days this
 * will say the market is open when it is not; the as-of time next to a price is the reliable part.
 */

export type SessionState = 'pre' | 'open' | 'after' | 'closed';

export interface Session {
  state: SessionState;
  label: string;
}

const LABEL: Record<SessionState, string> = {
  pre: 'Pre-market',
  open: 'Market open',
  after: 'After hours',
  closed: 'Market closed',
};

export function marketSession(now: Date): Session {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    weekday: 'short',
    hour: 'numeric',
    minute: 'numeric',
    hourCycle: 'h23',
  }).formatToParts(now);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? '';
  const weekend = get('weekday') === 'Sat' || get('weekday') === 'Sun';
  const minutes = Number(get('hour')) * 60 + Number(get('minute'));
  let state: SessionState = 'closed';
  if (!weekend) {
    if (minutes >= 240 && minutes < 570) state = 'pre';
    else if (minutes >= 570 && minutes < 960) state = 'open';
    else if (minutes >= 960 && minutes < 1200) state = 'after';
  }
  return { state, label: LABEL[state] };
}

const BOUNDS = [240, 570, 960, 1200];
const STATE_WORD: Record<SessionState, string> = { pre: 'pre-market starts', open: 'opens', after: 'after hours start', closed: 'closes' };

function nyClock(now: Date): { weekday: number; minutes: number } {
  const parts = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', weekday: 'short', hour: 'numeric', minute: 'numeric', hourCycle: 'h23' }).formatToParts(now);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? '';
  const weekday = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(get('weekday'));
  return { weekday, minutes: Number(get('hour')) * 60 + Number(get('minute')) };
}

export interface SessionClock extends Session {
  /** Minutes until the next change of state, holidays not counted. */
  minutesToNext: number;
  /** What happens then, for the sentence: "closes", "opens", "after hours start", "pre-market starts". */
  next: string;
  /** Where the New York day has got to between 4:00 and 20:00, 0 to 1; null on a weekend or outside those hours. */
  dayProgress: number | null;
}

/** The session now, how long until it changes, and how far through the trading day it is. */
export function sessionClock(now: Date): SessionClock {
  const base = marketSession(now);
  const { weekday, minutes } = nyClock(now);
  const weekdayNow = weekday >= 1 && weekday <= 5;
  let minutesToNext: number;
  let next: string;
  if (weekdayNow && minutes < 1200) {
    const b = BOUNDS.find((x) => x > minutes) as number;
    minutesToNext = b - minutes;
    const idx = BOUNDS.indexOf(b);
    // The state that begins at this boundary decides the wording: opens, closes, after hours.
    next = idx === 0 ? STATE_WORD.pre : idx === 1 ? STATE_WORD.open : idx === 2 ? 'closes' : 'ends';
  } else {
    // Closed: minutes to the next weekday's 4:00 pre-market.
    let days = 1;
    let d = (weekday + 1) % 7;
    while (d === 0 || d === 6) {
      days += 1;
      d = (d + 1) % 7;
    }
    minutesToNext = 1440 - minutes + 1440 * (days - 1) + BOUNDS[0];
    next = STATE_WORD.pre;
  }
  const inDay = weekdayNow && minutes >= BOUNDS[0] && minutes < BOUNDS[3];
  return { ...base, minutesToNext, next, dayProgress: inDay ? (minutes - BOUNDS[0]) / (BOUNDS[3] - BOUNDS[0]) : null };
}

/** "2 h 14 min", "45 min", "1 day 3 h". */
export function durationWords(minutes: number): string {
  if (minutes < 60) return `${Math.max(1, Math.round(minutes))} min`;
  const days = Math.floor(minutes / 1440);
  const hours = Math.floor((minutes % 1440) / 60);
  const mins = Math.round(minutes % 60);
  if (days > 0) return `${days} day${days === 1 ? '' : 's'}${hours ? ` ${hours} h` : ''}`;
  return `${hours} h${mins ? ` ${mins} min` : ''}`;
}
