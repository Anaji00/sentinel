import { describe, it, expect } from 'vitest';
import {
  ABSENT,
  isPresent,
  formatNumber,
  formatCompact,
  formatCurrency,
  formatPercent,
  formatBps,
  formatRatio,
  formatTimestamp,
  formatDate,
  formatAge,
  formatDuration,
  formatSymbol,
  maskSecret,
} from '../lib/format';

/**
 * The central invariant: absence renders as an em dash, never as a number.
 * A fabricated "0.00" is indistinguishable from a measured zero once it is on
 * screen, which is exactly what the platform's zero-fabrication rule forbids.
 */
describe('absence is never rendered as zero', () => {
  const formatters: Array<[string, (v: unknown) => string]> = [
    ['formatNumber', (v) => formatNumber(v as number)],
    ['formatCompact', (v) => formatCompact(v as number)],
    ['formatCurrency', (v) => formatCurrency(v as number)],
    ['formatPercent', (v) => formatPercent(v as number)],
    ['formatBps', (v) => formatBps(v as number)],
    ['formatRatio', (v) => formatRatio(v as number)],
    ['formatTimestamp', (v) => formatTimestamp(v as string)],
    ['formatDate', (v) => formatDate(v as string)],
    ['formatAge', (v) => formatAge(v as string)],
    ['formatDuration', (v) => formatDuration(v as number)],
  ];

  for (const [name, fn] of formatters) {
    it(`${name} returns the absent marker for null/undefined/NaN`, () => {
      expect(fn(null)).toBe(ABSENT);
      expect(fn(undefined)).toBe(ABSENT);
      expect(fn(NaN)).toBe(ABSENT);
      expect(fn('')).toBe(ABSENT);
    });
  }

  it('distinguishes a measured zero from an absent value', () => {
    expect(formatNumber(0)).toBe('0.00');
    expect(formatCurrency(0)).toBe('$0.00');
    expect(formatPercent(0)).toBe('0.00%');
    expect(formatNumber(null)).toBe(ABSENT);
  });

  it('isPresent guards non-finite values', () => {
    expect(isPresent(0)).toBe(true);
    expect(isPresent(-1.5)).toBe(true);
    expect(isPresent(NaN)).toBe(false);
    expect(isPresent(Infinity)).toBe(false);
    expect(isPresent(null)).toBe(false);
    expect(isPresent('3')).toBe(false);
  });
});

describe('formatPercent requires an explicit convention', () => {
  it('treats input as already-scaled percent by default', () => {
    expect(formatPercent(12.5)).toBe('12.50%');
  });

  it('scales ratios only when told to', () => {
    expect(formatPercent(0.125, { from: 'ratio' })).toBe('12.50%');
  });

  it('does not silently double-scale', () => {
    // The bug this API exists to prevent: a ratio read as a percent, or a
    // percent scaled a second time. Both stay wrong-but-plausible on screen.
    expect(formatPercent(0.125)).toBe('0.13%');
    expect(formatPercent(12.5, { from: 'ratio' })).toBe('1250.00%');
  });

  it('honours decimals and signing', () => {
    expect(formatPercent(2.4567, { decimals: 1 })).toBe('2.5%');
    expect(formatPercent(2.4, { signed: true })).toBe('+2.40%');
    expect(formatPercent(-2.4, { signed: true })).toBe('-2.40%');
  });
});

describe('magnitude formatting', () => {
  it('abbreviates large currency', () => {
    expect(formatCurrency(247_100_000_000)).toBe('$247.1B');
    expect(formatCurrency(1_500_000)).toBe('$1.5M');
    expect(formatCurrency(2_500)).toBe('$2.5K');
  });

  it('renders small currency in full', () => {
    expect(formatCurrency(999)).toBe('$999.00');
    expect(formatCurrency(1234.5, { compact: false })).toBe('$1,234.50');
  });

  it('preserves sign', () => {
    expect(formatCurrency(-1_500_000)).toBe('-$1.5M');
    expect(formatCompact(-2_500)).toBe('-2.5K');
    expect(formatNumber(42, { signed: true, decimals: 0 })).toBe('+42');
  });

  it('scales compact counts by magnitude', () => {
    expect(formatCompact(1_234)).toBe('1.2K');
    expect(formatCompact(1_234_567)).toBe('1.2M');
    expect(formatCompact(3_400_000_000_000)).toBe('3.4T');
    expect(formatCompact(999)).toBe('999.0');
  });
});

describe('basis points and ratios', () => {
  it('converts ratios to bps', () => {
    expect(formatBps(0.0025, { from: 'ratio' })).toBe('25.0 bps');
  });

  it('converts percents to bps', () => {
    expect(formatBps(0.25, { from: 'percent' })).toBe('25.0 bps');
  });

  it('leaves dimensionless ratios unsuffixed', () => {
    // A Sharpe ratio is not a multiple; appending "x" misrepresents it.
    expect(formatRatio(1.87)).toBe('1.87');
    expect(formatRatio(1.87)).not.toContain('x');
  });
});

describe('temporal formatting', () => {
  const t = '2026-08-21T14:32:05.000Z';

  it('renders absolute UTC timestamps', () => {
    expect(formatTimestamp(t)).toBe('2026-08-21 14:32:05Z');
    expect(formatTimestamp(t, { seconds: false })).toBe('2026-08-21 14:32Z');
    expect(formatDate(t)).toBe('2026-08-21');
  });

  it('rejects unparseable input rather than rendering Invalid Date', () => {
    expect(formatTimestamp('not-a-date')).toBe(ABSENT);
    expect(formatDate('not-a-date')).toBe(ABSENT);
    expect(formatAge('not-a-date')).toBe(ABSENT);
  });

  it('renders compact ages', () => {
    const now = new Date(t).getTime();
    expect(formatAge(new Date(now - 12_000).toISOString(), { now })).toBe('12s');
    expect(formatAge(new Date(now - 4 * 60_000).toISOString(), { now })).toBe('4m');
    expect(formatAge(new Date(now - 3 * 3_600_000).toISOString(), { now })).toBe('3h');
    expect(formatAge(new Date(now - 6 * 86_400_000).toISOString(), { now })).toBe('6d');
  });

  it('clamps future timestamps instead of showing negative age', () => {
    const now = new Date(t).getTime();
    expect(formatAge(new Date(now + 60_000).toISOString(), { now })).toBe('now');
  });

  it('renders durations', () => {
    expect(formatDuration(45)).toBe('45s');
    expect(formatDuration(750)).toBe('12m 30s');
    expect(formatDuration(7_500)).toBe('2h 05m');
    expect(formatDuration(-1)).toBe(ABSENT);
  });
});

describe('symbols and secrets', () => {
  it('normalizes symbols', () => {
    expect(formatSymbol('  nvda ')).toBe('NVDA');
    expect(formatSymbol('')).toBe(ABSENT);
    expect(formatSymbol(null)).toBe(ABSENT);
  });

  it('masks secrets leaving only a short suffix', () => {
    const masked = maskSecret('sk-live-abcdef123456');
    expect(masked.endsWith('3456')).toBe(true);
    expect(masked).not.toContain('abcdef');
    expect(maskSecret(null)).toBe(ABSENT);
  });

  it('never echoes a short secret in the clear', () => {
    expect(maskSecret('abc')).toBe('•••');
  });
});
