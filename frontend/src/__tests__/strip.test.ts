import { describe, expect, it } from 'vitest';
import { earningsWords, stripFlags, stripTickers, type StripRow } from '../lib/strip';

const row = (over: Partial<StripRow>): StripRow => ({
  ticker: 'NVDA', price: 167.88, change_pct: 1.02, session: 'closed', delayed: false, as_of: null, next_earnings: { date: '2026-11-17', estimated: false },
  model_percentile: 0.97, model_as_of: '2026-11-12', held_back_count: 13, regime: null, ...over,
});

describe('earningsWords', () => {
  it('words a listed and an expected date', () => {
    expect(earningsWords({ date: '2026-11-17', estimated: false }, (d) => d)).toBe('Earnings 2026-11-17');
    expect(earningsWords({ date: '2027-02-03', estimated: true }, (d) => d)).toBe('Earnings expected 2027-02-03');
  });
  it('is empty with no date', () => {
    expect(earningsWords(null, (d) => d)).toBe('');
    expect(earningsWords({ date: null, estimated: null }, (d) => d)).toBe('');
  });
});

describe('stripFlags', () => {
  it('lists the regime, held-back signals and a late price', () => {
    expect(stripFlags(row({ regime: { label: 'unexplained dislocation', action: null, alert_at: null }, delayed: true }))).toEqual(['unexplained dislocation', '13 signals held back', 'price delayed']);
  });
  it('is empty when there is nothing to flag', () => {
    expect(stripFlags(row({ held_back_count: 0 }))).toEqual([]);
    expect(stripFlags(row({ held_back_count: null }))).toEqual([]);
  });
});

describe('stripTickers', () => {
  it('upper-cases, drops blanks and repeats, and caps at fifty', () => {
    expect(stripTickers([' nvda', 'NVDA', '', 'aapl'])).toEqual(['NVDA', 'AAPL']);
    expect(stripTickers(Array.from({ length: 60 }, (_, i) => `T${i}`))).toHaveLength(50);
    expect(stripTickers(undefined)).toEqual([]);
  });
});
