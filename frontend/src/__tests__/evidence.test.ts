import { describe, expect, it } from 'vitest';
import { evidenceKey, gradeLine, okData, optionsWords, sectionFailed, shortWords, type ProducerGrade, type Section } from '../lib/evidence';

const sec = <T,>(status: Section<T>['status'], data: T | null): Section<T> => ({ status, data, note: null });

describe('sections', () => {
  it('returns data only when ok', () => {
    expect(okData(sec('ok', 5))).toBe(5);
    expect(okData(sec('missing', null))).toBeNull();
    expect(okData(sec('error', null))).toBeNull();
    expect(okData(undefined)).toBeNull();
  });
  it('tells a failed source from an empty one', () => {
    expect(sectionFailed(sec('error', null))).toBe(true);
    expect(sectionFailed(sec('missing', null))).toBe(false);
    expect(sectionFailed(undefined)).toBe(false);
  });
  it('builds the key with the ticker escaped', () => {
    expect(evidenceKey('BRK.B')).toBe('/stocks/BRK.B/evidence');
    expect(evidenceKey('a/b')).toBe('/stocks/a%2Fb/evidence');
  });
});

describe('optionsWords', () => {
  it('words the volumes and the put to call ratio', () => {
    const w = optionsWords({ live: { call_volume: 12000, put_volume: 18000, put_call_ratio: 1.5 } });
    expect(w[0]).toBe('12,000 call contracts and 18,000 put contracts traded.');
    expect(w[1]).toContain('More puts than calls (1.50 puts per call)');
  });
  it('uses only the fields that are present', () => {
    expect(optionsWords({ live: { put_call_ratio: 0.5 } })).toEqual(['More calls than puts (0.50 puts per call): more bets on a rise.']);
    expect(optionsWords({ live: {} })).toEqual([]);
    expect(optionsWords(null)).toEqual([]);
    expect(optionsWords({ live: { put_call_ratio: 1 } })[0]).toContain('About as many');
  });
});

describe('shortWords', () => {
  const rows = [
    { settlement_date: '2026-11-14', days_to_cover: 2.46, change_pct: -3.2 },
    { settlement_date: '2026-10-31', days_to_cover: 2.9, change_pct: 1 },
  ];
  it('words the latest report only, never as a signal', () => {
    const w = shortWords({ short_interest: rows });
    expect(w[0]).toBe('Short sellers would need about 2.5 days of normal trading to buy their shares back (report for 2026-11-14).');
    expect(w[1]).toBe('Shares sold short fell 3.2% since the previous report.');
    expect(w.join(' ')).not.toMatch(/buy signal|sell signal|bullish|bearish/i);
  });
  it('uses only the fields that are present', () => {
    expect(shortWords({ short_interest: [{ change_pct: 4 }] })).toEqual(['Shares sold short rose 4.0% since the previous report.']);
    expect(shortWords({ short_interest: [] })).toEqual([]);
    expect(shortWords(null)).toEqual([]);
  });
});

describe('gradeLine', () => {
  const g: ProducerGrade = { rule: 'x', family: 'f', graded: 'rule', verdict: 'moves more', n: 624, mean_ratio: 2, t: 3, q: 0.01 };
  it('says what was checked and what was not', () => {
    const line = gradeLine(g);
    expect(line).toContain('This kind of alert: moves more over five sessions (624 past cases)');
    expect(line).toContain('not direction or profit');
  });
  it('names a family grade and handles a missing sample', () => {
    expect(gradeLine({ ...g, graded: 'family', n: null })).toContain('Alerts from the same family');
    expect(gradeLine({ ...g, n: null })).toContain('few past cases');
  });
});
