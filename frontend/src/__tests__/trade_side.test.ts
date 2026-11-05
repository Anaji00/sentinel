import { describe, expect, it } from 'vitest';
import { tradeSide } from '../lib/tradeSide';

describe('tradeSide', () => {
  it('reads both spellings enrichment writes', () => {
    expect(tradeSide(['tradfi', 'buyer_initiated'])).toBe('BUY');
    expect(tradeSide(['aggressor_buy'])).toBe('BUY');
    expect(tradeSide(['seller_initiated'])).toBe('SELL');
    expect(tradeSide(['aggressor_sell'])).toBe('SELL');
  });

  it('never turns a call or put, or a missing tag, into a side', () => {
    expect(tradeSide(['call', 'options_flow'])).toBe('UNKNOWN');
    expect(tradeSide([])).toBe('UNKNOWN');
    expect(tradeSide(undefined)).toBe('UNKNOWN');
  });
});
