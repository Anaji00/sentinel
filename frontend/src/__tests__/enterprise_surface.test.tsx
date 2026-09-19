import React from 'react';
import { describe, it, expect } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { formatClock, formatDateTime, formatNumber, DISPLAY_LOCALE, ABSENT } from '../lib/format';
import { toCsv, csvFilename, type CsvColumn } from '../lib/csv';
import { DEFAULT_ZONE, TIMEZONES } from '../components/ui/TimeZoneContext';
import { ClockTime } from '../components/ui/ClockTime';

/**
 * The instant every zone assertion below is about.
 *
 * 2026-09-15T18:30:00Z is 14:30 in New York, 20:30 in Paris and 03:30 the
 * following day in Tokyo -- so it separates a zone bug from an off-by-an-hour
 * bug, and it crosses a date line, which is where "just format the time" stops
 * being enough.
 */
const INSTANT = '2026-09-15T18:30:00.000Z';

describe('times render in the selected zone, not the machine’s', () => {
  it('gives a different wall clock per zone for one instant', () => {
    expect(formatClock(INSTANT, 'America/New_York', { seconds: false })).toBe('14:30');
    expect(formatClock(INSTANT, 'UTC', { seconds: false })).toBe('18:30');
    expect(formatClock(INSTANT, 'Europe/Paris', { seconds: false })).toBe('20:30');
    expect(formatClock(INSTANT, 'Asia/Tokyo', { seconds: false })).toBe('03:30');
  });

  it('crosses the date line rather than only shifting the clock', () => {
    // Tokyo is already on the 16th. A component rendering the time alone would
    // show 03:30 against a row dated the 15th.
    expect(formatDateTime(INSTANT, 'Asia/Tokyo', { withZone: false })).toContain('2026-09-16');
    expect(formatDateTime(INSTANT, 'America/New_York', { withZone: false })).toContain(
      '2026-09-15',
    );
  });

  it('names the zone when asked, so two clocks are never ambiguous', () => {
    const withZone = formatClock(INSTANT, 'America/New_York', { withZone: true });
    expect(withZone).toMatch(/\b(EDT|EST|GMT-[45])\b/);
  });

  it('falls back to a labelled UTC rather than to the machine zone', () => {
    // An unknown identifier throws inside Intl. Silently using the browser's
    // zone there would reintroduce the whole defect on one bad input.
    const out = formatClock(INSTANT, 'Mars/Olympus', { seconds: false });
    expect(out).toBe('18:30Z');
  });

  it('renders absence, not the epoch', () => {
    expect(formatClock(null, 'UTC')).toBe(ABSENT);
    expect(formatClock(undefined, 'UTC')).toBe(ABSENT);
    expect(formatClock('', 'UTC')).toBe(ABSENT);
    expect(formatClock('not a date', 'UTC')).toBe(ABSENT);
  });

  it('every offered zone is one Intl actually accepts', () => {
    // A typo in the list would silently send that operator to the UTC branch.
    for (const [zone, label] of TIMEZONES) {
      expect(
        () => new Intl.DateTimeFormat('en-US', { timeZone: zone }).format(new Date()),
        label,
      ).not.toThrow();
    }
    expect(TIMEZONES.some(([z]) => z === DEFAULT_ZONE)).toBe(true);
  });

  it('ClockTime keeps the exact instant alongside the readable form', () => {
    const html = renderToStaticMarkup(<ClockTime value={INSTANT} />);
    // The wall clock is what is read; the ISO instant is what is reconciled
    // against a log, and losing it to make room for the readable form would
    // trade one for the other.
    expect(html).toContain('2026-09-15T18:30:00.000Z');
    expect(html).toContain('<time');
    // Unprovided, it uses the default zone rather than the machine's.
    expect(html).toContain('14:30');
  });
});

describe('figures do not depend on the reader’s OS settings', () => {
  it('formats the same on any machine', () => {
    expect(DISPLAY_LOCALE).toBe('en-US');
    expect(formatNumber(1234.5)).toBe('1,234.50');
    expect(formatNumber(1234.5, { decimals: 0 })).toBe('1,235');
  });

  it('still refuses to invent a value', () => {
    expect(formatNumber(null)).toBe(ABSENT);
    expect(formatNumber(undefined)).toBe(ABSENT);
    expect(formatNumber(Number.NaN)).toBe(ABSENT);
  });
});

interface Row {
  ticker: string;
  sharpe: number | null;
  note: string;
}

const COLUMNS: CsvColumn<Row>[] = [
  { label: 'ticker', value: (r) => r.ticker },
  { label: 'sharpe', value: (r) => r.sharpe },
  { label: 'note', value: (r) => r.note },
];

describe('an export that agrees with the screen', () => {
  it('writes an absent number as an empty cell, never as zero', () => {
    const csv = toCsv([{ ticker: 'SM', sharpe: null, note: 'x' }], COLUMNS);
    const [, row] = csv.split('\r\n');
    expect(row).toBe('SM,,x');
    // A zero here would be averaged by a spreadsheet as a real measurement.
    expect(row).not.toContain(',0,');
  });

  it('does not export the em dash the screen shows', () => {
    // "—" in a numeric column is text, and text poisons every SUM below it.
    const csv = toCsv([{ ticker: 'SM', sharpe: null, note: 'x' }], COLUMNS);
    expect(csv).not.toContain(ABSENT);
  });

  it('exports raw numbers, not the reading aid', () => {
    const csv = toCsv([{ ticker: 'SM', sharpe: 1234.5, note: 'x' }], COLUMNS);
    expect(csv).toContain('1234.5');
    expect(csv).not.toContain('1,234.5');
  });

  it('quotes and escapes anything that would break a row', () => {
    const csv = toCsv(
      [{ ticker: 'SM', sharpe: 1, note: 'said "buy", then sold\nnext line' }],
      COLUMNS,
    );
    expect(csv).toContain('"said ""buy"", then sold\nnext line"');
  });

  it('neutralises a cell a spreadsheet would run as a formula', () => {
    const csv = toCsv([{ ticker: 'SM', sharpe: 1, note: '=1+1' }], COLUMNS);
    expect(csv).toContain("'=1+1");
    expect(csv).not.toMatch(/,=1\+1/);
  });

  it('names the file so two exports can be told apart', () => {
    const name = csvFilename('strategy results', new Date(INSTANT));
    expect(name).toBe('sentinel-strategy-results-2026-09-15-18-30-00Z.csv');
  });

  it('writes a header even with no rows', () => {
    expect(toCsv([], COLUMNS)).toBe('ticker,sharpe,note');
  });
});
