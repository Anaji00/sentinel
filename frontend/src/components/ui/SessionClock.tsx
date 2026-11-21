'use client';

/**
 * The US market day as a small ring and one sentence: where the day has got to (pre-market, regular hours, after hours),
 * and how long until it changes. The ring is a picture of the sentence beside it, never the only carrier of the state.
 * Holidays are not tracked, so on one it can say the market is open when it is not.
 */

import React from 'react';
import { durationWords, sessionClock, type SessionClock as Clock } from '../../lib/marketSession';

const R = 14;
const C = 2 * Math.PI * R;
// Pre-market 4:00 to 9:30, regular to 16:00, after hours to 20:00, as shares of the 16-hour trading day.
const SEGMENTS: { from: number; to: number; cls: string }[] = [
  { from: 0, to: 330 / 960, cls: 'stroke-ink-mute' },
  { from: 330 / 960, to: 720 / 960, cls: 'stroke-accent' },
  { from: 720 / 960, to: 1, cls: 'stroke-ink-mute' },
];

export function SessionClock() {
  const [clock, setClock] = React.useState<Clock | null>(null);
  // Read after mount and refreshed each minute, so the server render and first paint agree.
  React.useEffect(() => {
    const tick = () => setClock(sessionClock(new Date()));
    tick();
    const id = window.setInterval(tick, 60_000);
    return () => window.clearInterval(id);
  }, []);
  if (!clock) return null;
  const sentence = `${clock.label}, ${clock.next} in ${durationWords(clock.minutesToNext)}`;
  return (
    <span className="hidden items-center gap-2 whitespace-nowrap text-micro 2xl:flex" title="US stock market hours, New York time. Market holidays are not tracked.">
      <svg viewBox="0 0 36 36" width="22" height="22" aria-hidden className="shrink-0 -rotate-90">
        <circle cx="18" cy="18" r={R} fill="none" stroke="var(--border-subtle)" strokeWidth="3" />
        {SEGMENTS.map((s) => (
          <circle key={s.from} cx="18" cy="18" r={R} fill="none" className={s.cls} strokeWidth="3" strokeDasharray={`${(s.to - s.from) * C - 1} ${C}`} strokeDashoffset={-s.from * C} />
        ))}
        {clock.dayProgress != null && (
          <circle cx={18 + R * Math.cos(clock.dayProgress * 2 * Math.PI)} cy={18 + R * Math.sin(clock.dayProgress * 2 * Math.PI)} r="3.2" fill="var(--text-primary)" stroke="var(--bg-base)" strokeWidth="1" />
        )}
      </svg>
      <span role="status" className={clock.state === 'open' ? 'text-ink' : 'text-ink-dim'}>
        {sentence}
      </span>
    </span>
  );
}

export default SessionClock;
