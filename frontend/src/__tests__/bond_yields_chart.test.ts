import { describe, it, expect } from 'vitest';
import { timePath, YIELD_SYMBOLS } from '../components/charts/BondYieldsChart';

const box = { t0: Date.parse('2026-10-15T10:00:00Z'), t1: Date.parse('2026-10-15T12:00:00Z'), lo: 4, hi: 5, width: 600, height: 180 };

describe('treasury yield chart', () => {
  it('places points by time, so a short series sits where its readings are', () => {
    // Two readings in the second hour only: they span the right half, not the whole width.
    const path = timePath(
      [
        { timestamp: '2026-10-15T11:00:00Z', price: 4.5 },
        { timestamp: '2026-10-15T12:00:00Z', price: 5 },
      ],
      box,
    );
    expect(path).toBe('M 300.0 90.0 L 600.0 0.0');
  });

  it('sorts by time and skips unusable points', () => {
    const path = timePath(
      [
        { timestamp: '2026-10-15T12:00:00Z', price: 4 },
        { timestamp: 'not a time', price: 4.2 },
        { timestamp: '2026-10-15T10:00:00Z', price: null },
        { timestamp: '2026-10-15T10:00:00Z', price: 5 },
      ],
      box,
    );
    expect(path).toBe('M 0.0 0.0 L 600.0 180.0');
  });

  it('matches live ticks by exact symbol, never by a digit in the name', () => {
    expect(YIELD_SYMBOLS.US02Y.has('US02Y')).toBe(true);
    expect(YIELD_SYMBOLS.US02Y.has('IRX')).toBe(false);
    expect(YIELD_SYMBOLS.US02Y.has('AAPL2')).toBe(false);
    expect(YIELD_SYMBOLS.US10Y.has('TNX')).toBe(true);
  });
});

import { sameDaySpread } from '../components/charts/BondYieldsChart';

describe('2s10s spread', () => {
  it('takes both legs from the same date, never yesterday against today', () => {
    const series = {
      US02Y: [
        { date: '2026-10-13', yield: 4.79 },
        { date: '2026-10-14', yield: 4.82 },
      ],
      US10Y: [
        { date: '2026-10-13', yield: 5.21 },
        { date: '2026-10-14', yield: 5.19 },
        { date: '2026-10-15', yield: 5.1 },
      ],
    };
    const s = sameDaySpread(series);
    expect(s?.date).toBe('2026-10-14');
    expect(s?.spread).toBeCloseTo(0.37, 6);
    expect(sameDaySpread({ US02Y: [], US10Y: [{ date: '2026-10-14', yield: 5 }] })).toBeNull();
    expect(sameDaySpread(undefined)).toBeNull();
  });
});
