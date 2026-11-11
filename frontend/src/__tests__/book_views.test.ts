import { describe, expect, it } from 'vitest';
import { instrumentWords, metricWords, shockSentence, siteWords, sortExposure, unusualOil, unusualWords } from '../lib/bookViews';

const row = (instrument: string, book_move: number, flag = false) => ({ instrument, shock: '-5%', book_move, names: null, flag });
const oil = (site: string, z: number | null, over = {}) => ({ site, metric: 'dark_events', day: '2026-11-04', value: 9, history_days: 30, coverage_gap: false, z, ...over });

describe('book exposure words', () => {
  it('names instruments and keeps unknown ones as stored', () => {
    expect(instrumentWords('SPY (market)')).toBe('The stock market (S&P 500)');
    expect(instrumentWords('GC=F')).toBe('Gold');
    expect(instrumentWords('CL=F')).toBe('Oil (WTI crude)');
    expect(instrumentWords('XYZ')).toBe('XYZ');
  });

  it('says what a shock does in a sentence, with the sign in words', () => {
    expect(shockSentence(row('SPY (market)', -0.01551))).toBe('The stock market (S&P 500) -5%: the book loses 1.55%');
    expect(shockSentence(row('US10Y', 0.00237))).toContain('gains 0.24%');
    expect(shockSentence(row('GLD', 0))).toContain('does not move');
  });

  it('puts flagged rows first, then the biggest moves', () => {
    const sorted = sortExposure([row('A', 0.001), row('B', -0.02), row('C', 0.0005, true)]);
    expect(sorted.map((r) => r.instrument)).toEqual(['C', 'B', 'A']);
  });
});

describe('oil route words', () => {
  it('turns codes into plain words', () => {
    expect(siteWords('bab_el_mandeb')).toBe('Bab El Mandeb');
    expect(metricWords('dark_events')).toBe('ships switching off their transponder');
    expect(metricWords('new_metric')).toBe('new metric');
  });

  it('describes how unusual a reading is, and admits when it cannot', () => {
    expect(unusualWords(5.9)).toBe('far above its usual level');
    expect(unusualWords(-2.3)).toBe('well below its usual level');
    expect(unusualWords(0.4)).toBe('near its usual level');
    expect(unusualWords(null)).toBe('not enough history to compare');
  });

  it('lists only readings at least two sigma away, biggest first, never coverage gaps', () => {
    const list = unusualOil({ episodes: [oil('a', 2.2), oil('b', -5.9), oil('c', 0.5), oil('d', 7, { coverage_gap: true }), oil('e', null)] });
    expect(list.map((s) => s.site)).toEqual(['b', 'a']);
    expect(unusualOil(null)).toEqual([]);
  });
});
