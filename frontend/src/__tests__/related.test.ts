import { describe, expect, it } from 'vitest';
import { emptyRelated, relatedKey, shortDate } from '../lib/related';

describe('related stocks', () => {
  it('builds the route from a ticker, encoded', () => {
    expect(relatedKey('NVDA')).toBe('/stocks/NVDA/related');
    expect(relatedKey('BRK.B')).toBe('/stocks/BRK.B/related');
    expect(relatedKey('A/B')).toBe('/stocks/A%2FB/related');
  });

  it('says why there is nothing, without claiming to know more than it does', () => {
    expect(emptyRelated('not_computed', 120).title).toBe('Not worked out yet');
    const none = emptyRelated('none_found', 120);
    expect(none.title).toBe('No close relatives found');
    expect(none.detail).toContain('120 trading days');
    expect(none.detail).toContain('not in the set');
  });

  it('writes a stored date as month and day, and nothing for a bad one', () => {
    expect(shortDate('2026-11-05')).toBe('Nov 5');
    expect(shortDate(null)).toBe('');
    expect(shortDate('not a date')).toBe('');
  });
});
