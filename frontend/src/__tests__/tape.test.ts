import { describe, it, expect } from 'vitest';
import { tapeRows } from '../lib/tape';

describe('live tape', () => {
  it('takes the latest price and the change across the readings, in tape order', () => {
    const rows = tapeRows(
      {
        QQQ: [
          { timestamp: 'a', price: 100 },
          { timestamp: 'b', price: 101 },
        ],
        SPY: [
          { timestamp: 'a', price: 200 },
          { timestamp: 'b', price: 198 },
        ],
      },
      ['SPY', 'QQQ', 'VIX'],
    );
    expect(rows.map((r) => r.symbol)).toEqual(['SPY', 'QQQ']);
    expect(rows[0].price).toBe(198);
    expect(rows[0].changePct).toBeCloseTo(-1);
  });

  it('skips bad readings and gives no change from one point', () => {
    const rows = tapeRows(
      {
        GLD: [
          { timestamp: 'a', price: 0 },
          { timestamp: 'b', price: 368.57 },
        ],
      },
      ['GLD'],
    );
    expect(rows).toEqual([{ symbol: 'GLD', price: 368.57, changePct: null }]);
    expect(tapeRows(undefined)).toEqual([]);
  });
});
