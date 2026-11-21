import { describe, expect, it } from 'vitest';
import { bigMoney, businessFacts, factsRow, factsSentence, rangePosition, type Snapshot } from '../lib/snapshot';

const day = (iso: string) => iso;
const base: Snapshot = {
  ticker: 'NVDA', market_cap: 4.82e12, market_cap_as_of: '2026-11-12', range_52w: { low: 90, high: 200, from_date: '2025-11-12' },
  next_earnings: { date: '2026-11-17', estimated: false }, pe_trailing: { value: 25, basis: 'market cap / trailing 12-month net income', as_of: '2026-07-26' },
  metrics: {
    revenue: 478.8e9, revenue_growth_pct: 15.1, gross_margin_pct: 48.8, operating_margin_pct: 33, net_margin_pct: 27.6, cash_from_operations_margin_pct: 30.3,
    return_on_equity_pct: 117.9, price_to_sales: 10.79, price_to_book: 46.11, basis: 'trailing 12 months', as_of: '2026-09-26',
  },
};

describe('bigMoney', () => {
  it('writes trillions, billions and millions', () => {
    expect(bigMoney(4.82e12)).toBe('$4.82T');
    expect(bigMoney(478.8e9)).toBe('$479B');
    expect(bigMoney(12.4e6)).toBe('$12.40M');
  });
  it('says nothing for missing or non-positive values', () => {
    expect(bigMoney(null)).toBeNull();
    expect(bigMoney(0)).toBeNull();
  });
});

describe('rangePosition', () => {
  it('places a price in its range and clamps outside it', () => {
    expect(rangePosition(145, 90, 200)).toBeCloseTo(0.5);
    expect(rangePosition(300, 90, 200)).toBe(1);
    expect(rangePosition(10, 90, 200)).toBe(0);
  });
  it('is null when the range is unusable', () => {
    expect(rangePosition(100, 100, 100)).toBeNull();
    expect(rangePosition(null, 90, 200)).toBeNull();
  });
});

describe('factsRow', () => {
  it('lists size, price to earnings and the next earnings date', () => {
    const f = factsRow(base, day);
    expect(f.map((x) => x.label)).toEqual(['Company size', 'Price to earnings', 'Next earnings']);
    expect(f[1].value).toBe('25.0');
  });
  it('calls an estimated date expected', () => {
    expect(factsRow({ ...base, next_earnings: { date: '2026-12-01', estimated: true } }, day)[2].label).toBe('Earnings expected');
  });
  it('says not meaningful for a loss, never 0', () => {
    expect(factsRow({ ...base, pe_trailing: { value: null, basis: null, as_of: null } }, day)[1].value).toBe('not meaningful');
  });
  it('is empty with no snapshot', () => {
    expect(factsRow(null, day)).toEqual([]);
  });
});

describe('businessFacts', () => {
  it('uses plain labels and signs growth', () => {
    const f = Object.fromEntries(businessFacts(base).map((x) => [x.label, x.value]));
    expect(f['Sales growth']).toBe('+15.1%');
    expect(f['Profit margin']).toBe('27.6%');
    expect(f['Return on equity']).toBe('117.9%');
  });
  it('marks figures that cannot be computed, and keeps a loss as a negative margin', () => {
    const f = Object.fromEntries(businessFacts({ ...base, metrics: { ...base.metrics!, net_margin_pct: -45.9, return_on_equity_pct: null, price_to_book: null } }).map((x) => [x.label, x.value]));
    expect(f['Profit margin']).toBe('−45.9%');
    expect(f['Return on equity']).toBe('not meaningful');
    expect(f['Price to book']).toBe('not meaningful');
  });
  it('is empty when there are no metrics', () => {
    expect(businessFacts({ ...base, metrics: null })).toEqual([]);
  });
});

describe('factsSentence', () => {
  const f = (label: string, value: string) => ({ label, value });
  const text = (parts: { text: string }[]) => parts.map((p) => p.text).join('');
  it('reads as one sentence with the figures marked', () => {
    const parts = factsSentence([f('Company size', '$2.6B'), f('Price to earnings', '28.1'), f('Next earnings', 'Nov 20')]);
    expect(text(parts)).toBe("A $2.6B company, priced at 28.1 times last year's profit. Next reports Nov 20.");
    expect(parts.filter((p) => p.fig).map((p) => p.text)).toEqual(['$2.6B', '28.1', 'Nov 20']);
  });
  it('says a missing price to earnings in words and an estimate as expected', () => {
    expect(text(factsSentence([f('Company size', '$900M'), f('Price to earnings', 'not meaningful'), f('Earnings expected', 'Dec 1')]))).toBe('A $900M company, not profitable over the last 12 months. Expected to report Dec 1.');
    expect(factsSentence([])).toEqual([]);
  });
});
