import { describe, expect, it } from 'vitest';
import { alertLine, alreadyPassed, newlyTriggered, parseLevel, type PriceAlert } from '../lib/alerts';

const alert = (over: Partial<PriceAlert>): PriceAlert => ({
  id: 1, ticker: 'NVDA', direction: 'above', level: 170.5, note: null, status: 'active', created_at: '2026-11-12T23:00:00Z',
  triggered_at: null, triggered_price: null, seen_at: null, ...over,
});
const money = (v: number) => `$${v.toFixed(2)}`;

describe('parseLevel', () => {
  it('reads plain and formatted prices', () => {
    expect(parseLevel('170')).toBe(170);
    expect(parseLevel('$1,170.50')).toBe(1170.5);
    expect(parseLevel(' 99.9 ')).toBe(99.9);
  });
  it('rejects anything else', () => {
    for (const bad of ['', '0', '-5', 'abc', '1e3', '12.', '1.2.3']) expect(parseLevel(bad)).toBeNull();
  });
});

describe('alertLine', () => {
  it('words an active alert', () => {
    expect(alertLine(alert({}), money)).toBe('NVDA rises above $170.50.');
    expect(alertLine(alert({ direction: 'below' }), money)).toBe('NVDA falls below $170.50.');
  });
  it('adds the price that crossed once fired', () => {
    expect(alertLine(alert({ status: 'triggered', triggered_price: 171.2 }), money)).toBe('NVDA rises above $170.50: reached $171.20.');
  });
});

describe('alreadyPassed', () => {
  it('warns when the price is already past the level', () => {
    expect(alreadyPassed('above', 100, 120)).toMatch(/already above/);
    expect(alreadyPassed('below', 100, 90)).toMatch(/already below/);
  });
  it('is quiet otherwise, and without a price', () => {
    expect(alreadyPassed('above', 100, 90)).toBeNull();
    expect(alreadyPassed('below', 100, null)).toBeNull();
  });
});

describe('newlyTriggered', () => {
  it('keeps fired, unseen, unannounced alerts, oldest first', () => {
    const list = [
      alert({ id: 3, status: 'triggered', triggered_at: '2026-11-12T10:05:00Z' }),
      alert({ id: 2, status: 'triggered', triggered_at: '2026-11-12T10:00:00Z' }),
      alert({ id: 4, status: 'triggered', triggered_at: '2026-11-12T10:01:00Z', seen_at: '2026-11-12T10:02:00Z' }),
      alert({ id: 5 }),
    ];
    expect(newlyTriggered(list, new Set([9])).map((a) => a.id)).toEqual([2, 3]);
    expect(newlyTriggered(list, new Set([2])).map((a) => a.id)).toEqual([3]);
    expect(newlyTriggered(undefined, new Set())).toEqual([]);
  });
});
