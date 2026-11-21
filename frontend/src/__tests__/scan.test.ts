import { describe, expect, it } from 'vitest';
import { buildingWait, isBuilding, warmingSeconds, eventWords, groupSentence, moveStep, screenerUrl, sessionNote, signedPct, volumeWords, weatherFigures, weatherSentence, type BreadthDay, type HeatGroup } from '../lib/scan';

const day = (share_up: number | null): BreadthDay => ({
  day: '2026-11-09', n: 2400, advancers: 2000, decliners: 400, unchanged: 0, share_up, new_52w_highs: 80, new_52w_lows: 5, share_above_50d: 0.62, share_above_200d: 0.7,
});

describe('weatherSentence', () => {
  it('bands the share of stocks up', () => {
    expect(weatherSentence(day(0.84))).toBe('Most stocks rose: a broad rally.');
    expect(weatherSentence(day(0.6))).toBe('More stocks rose than fell.');
    expect(weatherSentence(day(0.5))).toBe('About as many rose as fell: a mixed day.');
    expect(weatherSentence(day(0.4))).toBe('More stocks fell than rose.');
    expect(weatherSentence(day(0.15))).toBe('Most stocks fell: a broad sell-off.');
  });
  it('is null without a share', () => {
    expect(weatherSentence(day(null))).toBeNull();
    expect(weatherSentence(null)).toBeNull();
  });
});

describe('weatherFigures', () => {
  it('lists the three figures with plain labels', () => {
    const f = weatherFigures(day(0.84));
    expect(f.map((x) => x.value)).toEqual(['84%', '80 / 5', '62%']);
  });
  it('leaves out figures that are null', () => {
    expect(weatherFigures({ ...day(0.5), share_above_50d: null, new_52w_highs: null }).map((x) => x.label)).toEqual(['Stocks up']);
  });
});

describe('heat map wording', () => {
  const g: HeatGroup = { key: 'energy', label: 'Energy', n: 40, n_priced: 38, change_pct: 1.61, change_pct_median: 1.2, breadth_up_share: 0.758, volume_vs_20d_median: 1.1, top: [], bottom: [] };
  it('steps moves in fixed bands, with sign', () => {
    expect([0.1, 0.5, 1.5, 4, -0.1, -0.5, -1.5, -4].map(moveStep)).toEqual([0, 1, 2, 3, 0, -1, -2, -3]);
    expect(moveStep(null)).toBe(0);
  });
  it('words a group', () => {
    expect(groupSentence(g)).toBe('Energy rose 1.61%, with 76% of its stocks up.');
    expect(groupSentence({ ...g, change_pct: null })).toBeNull();
    expect(groupSentence({ ...g, change_pct: -1.62, breadth_up_share: null })).toBe('Energy fell 1.62%.');
  });
  it('says what the move is measured over', () => {
    expect(sessionNote('regular')).toContain('Live');
    expect(sessionNote('after_hours')).toContain('last completed session');
    expect(signedPct(-1.62)).toBe('−1.62%');
  });
});

describe('screener helpers', () => {
  it('builds the url with a size floor only when set', () => {
    expect(screenerUrl('rel_volume', 'desc', 0)).toBe('/stocks/screener?sort=rel_volume&dir=desc&limit=100');
    expect(screenerUrl('change_pct', 'asc', 1e10, 50)).toBe('/stocks/screener?sort=change_pct&dir=asc&limit=50&min_cap=10000000000');
  });
  it('adds a sector filter, escaped', () => {
    expect(screenerUrl('change_pct', 'desc', 0, 100, 'Consumer Staples')).toBe('/stocks/screener?sort=change_pct&dir=desc&limit=100&sector=Consumer%20Staples');
  });
  it('calls an estimated earnings date expected', () => {
    expect(eventWords({ kind: 'earnings', date: '2027-02-03', estimated: true }, (d) => d)).toBe('Earnings expected 2027-02-03');
  });
  it('words volume and events, empty when unknown', () => {
    expect(volumeWords(4.44)).toBe('4.4×');
    expect(volumeWords(null)).toBe('');
    expect(eventWords({ kind: 'earnings', date: '2026-11-17' }, (d) => d)).toBe('Earnings 2026-11-17');
    expect(eventWords(null, (d) => d)).toBe('');
  });
});

describe('warmingSeconds', () => {
  it('reads a 503 with Retry-After as warming', () => {
    expect(warmingSeconds({ response: { status: 503, headers: { 'retry-after': '30' } } })).toBe(30);
    expect(warmingSeconds({ response: { status: 503, headers: { 'Retry-After': '2' } } })).toBe(5);
    expect(warmingSeconds({ response: { status: 503, headers: { 'retry-after': '900' } } })).toBe(120);
  });
  it('treats every other error as a real failure', () => {
    expect(warmingSeconds({ response: { status: 503, headers: {} } })).toBeNull();
    expect(warmingSeconds({ response: { status: 500, headers: { 'retry-after': '30' } } })).toBeNull();
    expect(warmingSeconds(new Error('x'))).toBeNull();
    expect(warmingSeconds(null)).toBeNull();
  });
});

describe('a route still being built', () => {
  it('treats any 503 as building, even when Retry-After did not come through', () => {
    expect(isBuilding({ response: { status: 503, headers: {} } })).toBe(true);
    expect(buildingWait({ response: { status: 503, headers: {} } })).toBe(20);
    expect(buildingWait({ response: { status: 503, headers: { 'retry-after': '30' } } })).toBe(30);
  });
  it('leaves every other error alone', () => {
    expect(isBuilding({ response: { status: 500 } })).toBe(false);
    expect(buildingWait(new Error('x'))).toBeNull();
    expect(buildingWait(null)).toBeNull();
  });
});
