import { describe, expect, it } from 'vitest';
import { alignCloses, compare, correlationWords, dailyCorrelation, leaderSentence, rebased, returnOver } from '../lib/compare';

const series = (start: number, steps: number[], first = 1) =>
  steps.reduce<{ day: string; close: number }[]>(
    (acc, s, i) => [...acc, { day: `2026-01-${String(first + i + 1).padStart(2, '0')}`, close: (acc[acc.length - 1]?.close ?? start) * (1 + s) }],
    [],
  );

describe('aligning', () => {
  it('keeps only the days both traded, oldest first, and drops bad closes', () => {
    const a = [{ day: '2026-01-03', close: 12 }, { day: '2026-01-01', close: 10 }, { day: '2026-01-02', close: 0 }];
    const b = [{ day: '2026-01-01', close: 5 }, { day: '2026-01-03', close: 6 }, { day: '2026-01-04', close: 7 }];
    expect(alignCloses(a, b)).toEqual({ days: ['2026-01-01', '2026-01-03'], a: [10, 12], b: [5, 6] });
    expect(alignCloses(null, undefined)).toEqual({ days: [], a: [], b: [] });
  });
});

describe('returns', () => {
  it('rebases both lines to start at zero', () => {
    const r = rebased([50, 55, 45]);
    expect(r[0]).toBe(0);
    expect(r[1]).toBeCloseTo(10, 9);
    expect(r[2]).toBeCloseTo(-10, 9);
    expect(rebased([])).toEqual([]);
  });

  it('measures a return over n sessions and says nothing without that much history', () => {
    const v = Array.from({ length: 30 }, (_, i) => 100 + i);
    expect(returnOver(v, 21)).toBeCloseTo((129 / 108 - 1) * 100, 6);
    expect(returnOver(v, 40)).toBeNull();
  });
});

describe('correlation', () => {
  const steps = [0.01, -0.02, 0.015, 0.005, -0.01, 0.02, -0.015, 0.01, 0.0, -0.005, 0.012, -0.018, 0.02, 0.003, -0.007, 0.011, -0.012, 0.009, 0.004, -0.02, 0.013, 0.006, -0.008];
  const a = series(100, steps).map((c) => c.close);

  it('is 1 for a stock against itself and -1 against its mirror', () => {
    expect(dailyCorrelation(a, a)).toBeCloseTo(1, 6);
    const mirror = series(100, steps.map((s) => -s)).map((c) => c.close);
    expect(dailyCorrelation(a, mirror)).toBeLessThan(-0.99);
  });

  it('is absent on thin data or a flat line', () => {
    expect(dailyCorrelation([1, 2, 3], [1, 2, 3])).toBeNull();
    expect(dailyCorrelation(a, a.map(() => 5))).toBeNull();
  });

  it('describes it in plain words', () => {
    expect(correlationWords(0.9)).toBe('they usually move together');
    expect(correlationWords(0.0)).toBe('their daily moves are mostly unrelated');
    expect(correlationWords(-0.6)).toBe('they tend to move in opposite directions');
    expect(correlationWords(null)).toContain('not enough');
  });
});

describe('comparison', () => {
  it('summarises both sides and names the leader', () => {
    const days = Array.from({ length: 70 }, (_, i) => `d${i}`);
    const c = compare({ days, a: days.map((_, i) => 100 + i), b: days.map(() => 100) });
    expect(c?.sessions).toBe(70);
    expect(c?.a.quarter).not.toBeNull();
    expect(c?.b.window).toBe(0);
    expect(leaderSentence('AAA', 'BBB', c)).toBe('AAA has done 69.0 percentage points better than BBB over the last 70 trading days.');
  });

  it('is absent without two shared days, and calls a near tie level', () => {
    expect(compare({ days: ['d1'], a: [1], b: [1] })).toBeNull();
    expect(leaderSentence('A', 'B', null)).toBeNull();
    const c = compare({ days: ['d1', 'd2'], a: [100, 100.2], b: [100, 100] });
    expect(leaderSentence('A', 'B', c)).toBe('A and B are level over the last 2 trading days.');
  });
});
