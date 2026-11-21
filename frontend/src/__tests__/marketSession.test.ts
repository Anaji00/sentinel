import { describe, expect, it } from 'vitest';
import { durationWords, marketSession, sessionClock } from '../lib/marketSession';

// 11 Nov 2026 is a Wednesday; New York is on standard time (UTC-5).
const at = (utc: string) => marketSession(new Date(utc)).state;

describe('marketSession', () => {
  it('is pre-market from 4:00 to 9:30 New York time', () => {
    expect(at('2026-11-11T09:00:00Z')).toBe('pre'); // 4:00
    expect(at('2026-11-11T14:29:00Z')).toBe('pre'); // 9:29
  });
  it('is open from 9:30 to 16:00', () => {
    expect(at('2026-11-11T14:30:00Z')).toBe('open');
    expect(at('2026-11-11T20:59:00Z')).toBe('open'); // 15:59
  });
  it('is after hours from 16:00 to 20:00, then closed', () => {
    expect(at('2026-11-11T21:00:00Z')).toBe('after');
    expect(at('2026-11-12T00:58:00Z')).toBe('after'); // 19:58
    expect(at('2026-11-12T01:00:00Z')).toBe('closed'); // 20:00
    expect(at('2026-11-11T08:59:00Z')).toBe('closed'); // 3:59
  });
  it('is closed all weekend', () => {
    expect(at('2026-11-14T16:00:00Z')).toBe('closed'); // Saturday noon
    expect(at('2026-11-15T16:00:00Z')).toBe('closed');
  });
  it('follows daylight saving time', () => {
    expect(at('2026-07-15T13:30:00Z')).toBe('open'); // 9:30 EDT
  });
});

describe('sessionClock', () => {
  const at = (utc: string) => sessionClock(new Date(utc));
  it('counts down to the close during regular hours', () => {
    const c = at('2026-11-11T18:46:00Z'); // 13:46 New York
    expect(c.state).toBe('open');
    expect(c.next).toBe('closes');
    expect(c.minutesToNext).toBe(134);
  });
  it('counts to the open in pre-market and to the end of after hours', () => {
    expect(at('2026-11-11T13:00:00Z').next).toBe('opens'); // 8:00
    expect(at('2026-11-11T13:00:00Z').minutesToNext).toBe(90);
    expect(at('2026-11-12T00:00:00Z').next).toBe('ends'); // 19:00
  });
  it('counts overnight and across a weekend to the next pre-market', () => {
    expect(at('2026-11-12T01:30:00Z').minutesToNext).toBe(450); // Wed 20:30 to Thu 4:00
    expect(at('2026-11-14T17:00:00Z').next).toBe('pre-market starts'); // Saturday noon
    expect(at('2026-11-14T17:00:00Z').dayProgress).toBeNull();
  });
  it('reports how far through the trading day it is', () => {
    expect(at('2026-11-11T14:30:00Z').dayProgress).toBeCloseTo(330 / 960); // 9:30
  });
  it('words a duration', () => {
    expect(durationWords(45)).toBe('45 min');
    expect(durationWords(134)).toBe('2 h 14 min');
    expect(durationWords(1440 + 180)).toBe('1 day 3 h');
  });
});
