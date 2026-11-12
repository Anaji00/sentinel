import { describe, expect, it } from 'vitest';
import { headlinePrice } from '../lib/heroPrice';

describe('headlinePrice', () => {
  it('shows the last close with its change from the one before', () => {
    const p = headlinePrice([100, 110], null)!;
    expect(p.kind).toBe('close');
    expect(p.price).toBe(110);
    expect(p.changePct).toBeCloseTo(10);
  });
  it('shows a newer trade measured from the last close, not the day before', () => {
    // NVDA on 11 Nov: closes 168.89 then 173.73, latest trade 158.85. The old header said -5.9% (against 168.89).
    const p = headlinePrice([178.67, 168.89, 173.73], 158.85)!;
    expect(p.kind).toBe('trade');
    expect(p.price).toBe(158.85);
    expect(p.changePct).toBeCloseTo(-8.56, 1);
  });
  it('treats a trade at the close as the close', () => {
    expect(headlinePrice([100, 110], 110.01)!.kind).toBe('close');
  });
  it('falls back to the trade when there is no history, and to nothing without either', () => {
    expect(headlinePrice([], 50)).toEqual({ price: 50, kind: 'trade', changePct: null });
    expect(headlinePrice([], null)).toBeNull();
  });
  it('ignores a bad trade', () => {
    expect(headlinePrice([100], NaN)!.kind).toBe('close');
    expect(headlinePrice([100], -3)!.kind).toBe('close');
  });
});
