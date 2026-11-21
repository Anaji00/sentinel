import { describe, expect, it } from 'vitest';
import { digestLines, digestSummary, feedLines, gutterParts, groupByDate, hoursAgoIso, hourWords, type Changes, type EarningsRow } from '../lib/digest';

const day = (iso: string) => iso.slice(0, 10);
const base: Changes = {
  since: '2026-11-12T00:00:00Z', as_of: '2026-11-13T00:00:00Z', total: 5,
  sections: {
    new_calls: { status: 'ok', count: 1, items: [{ day: '2026-11-12', horizon: 'monthly', ticker: 'AAPL', side: 'short', validated: false }] },
    filings: { status: 'ok', count: 1, items: [{ ticker: 'NVDA', form_type: '8-K', filed_at: '2026-11-12T17:59:39Z', headline: 'Results' }] },
    divergence_alerts: { status: 'ok', count: 1, items: [{ ticker: 'TSLA', detected_at: '2026-11-12T15:00:00Z', regime: { label: 'unexplained dislocation' } }] },
    price_alerts_fired: { status: 'ok', count: 1, items: [{ id: 1, ticker: 'NVDA', direction: 'above', level: 170, triggered_at: '2026-11-12T20:00:00Z', triggered_price: 171.5 }] },
    model: { as_of: '2026-11-06', generated_at: '2026-11-12T19:01:00Z', newer: true },
  },
};

describe('digestLines', () => {
  it('writes each change as a plain line', () => {
    const t = digestLines(base, day).map((l) => l.text);
    expect(t).toContain('NVDA rose above $170.00, reaching $171.50.');
    expect(t).toContain('New one-month idea: bet against AAPL (model still being tested).');
    expect(t).toContain('NVDA filed Company news (8-K) on 2026-11-12: Results');
    expect(t).toContain('TSLA began moving away from its peers on 2026-11-12: unexplained dislocation.');
    expect(t).toContain('The model published a new run, with scores dated 2026-11-06.');
  });
  it('words a news-driven idea and drops a headline that repeats the filing', () => {
    const c: Changes = { ...base, sections: { ...base.sections, new_calls: { status: 'ok', count: 1, items: [{ day: '2026-11-13', horizon: 'events', ticker: 'AVGO', side: 'short' }] }, filings: { status: 'ok', count: 1, items: [{ ticker: 'AVGO', form_type: '8-K', filed_at: '2026-11-13T10:00:00Z', headline: 'Broadcom Inc. filed Form 8-K' }] } } };
    const t = digestLines(c, day).map((l) => l.text);
    expect(t).toContain('New news-driven idea: bet against AVGO (model still being tested).');
    expect(t).toContain('AVGO filed Company news (8-K) on 2026-11-13.');
  });
  it('puts the newest first and lines with no time last', () => {
    const lines = digestLines(base, day);
    const times = lines.map((l) => (l.at ? Date.parse(l.at) : -Infinity));
    expect(times).toEqual([...times].sort((a, b) => b - a));
    expect(lines[0].text).toContain('NVDA rose above $170.00');
  });
  it('gives the gutter a date, and a time only when there is one', () => {
    const clock = () => '5:55 PM';
    expect(gutterParts('2026-11-12T22:55:00Z', day, clock)).toEqual({ date: '2026-11-12', time: '5:55 PM' });
    expect(gutterParts('2026-11-12T00:00:00Z', day, clock)).toEqual({ date: '2026-11-12', time: null });
    expect(gutterParts(null, day, clock)).toBeNull();
    expect(gutterParts('soon', day, clock)).toBeNull();
  });
  it('says a failed source could not be read, never zero', () => {
    const c: Changes = { ...base, sections: { ...base.sections, filings: { status: 'error', count: null, items: [] } } };
    const lines = digestLines(c, day);
    const failed = lines.find((l) => l.failed);
    expect(failed?.text).toContain('Could not read filings');
    expect(failed?.text).toContain('not the same as nothing changing');
    expect(digestSummary(c).failed).toBe(1);
  });
  it('is empty with nothing', () => {
    expect(digestLines(null, day)).toEqual([]);
    expect(digestSummary(undefined)).toEqual({ total: null, failed: 0 });
  });
});

describe('feedLines', () => {
  const feed = {
    since: '2026-11-12T00:00:00Z', as_of: '2026-11-13T00:00:00Z', failed: [] as string[],
    items: [
      { id: 'call:2026-11-12:monthly:AAPL:short', kind: 'call' as const, ticker: 'AAPL', at: '2026-11-12', at_precision: 'day' as const, title: 'AAPL: new one-month idea, bet against' },
      { id: 'filing:1', kind: 'filing' as const, ticker: 'NVDA', at: '2026-11-12T17:59:39Z', at_precision: 'second' as const, title: 'NVDA filed Form 8-K' },
    ],
  };
  it('keeps the backend id and orders newest first, a day-only item at midnight', () => {
    const l = feedLines(feed);
    expect(l.map((x) => x.key)).toEqual(['filing:1', 'call:2026-11-12:monthly:AAPL:short']);
    expect(l[1].at).toBe('2026-11-12T00:00:00Z');
    expect(l[0].group).toBe('Filings');
  });
  it('words the backend events label for a person', () => {
    const l = feedLines({ since: 'x', as_of: null, failed: [], items: [{ id: 'a', kind: 'call', ticker: 'AVGO', at: '2026-11-19', at_precision: 'day', title: 'Short idea on AVGO (events)' }] });
    expect(l[0].text).toBe('Short idea on AVGO (news-driven)');
  });
  it('reports a source it could not read, never as zero changes', () => {
    const l = feedLines({ ...feed, items: [], failed: ['filings'] });
    expect(l).toHaveLength(1);
    expect(l[0].failed).toBe(true);
    expect(l[0].text).toContain('not the same as nothing changing');
  });
  it('handles no feed', () => {
    expect(feedLines(null)).toEqual([]);
  });
});

describe('earnings calendar', () => {
  const row = (ticker: string, date: string, hour: EarningsRow['hour'] = null): EarningsRow => ({ ticker, date, hour, eps_estimate: null, revenue_estimate: null, estimated: false, market_cap: null });
  it('groups by date in date order, keeping the day order', () => {
    const g = groupByDate([row('B', '2026-11-18'), row('A', '2026-11-17'), row('C', '2026-11-17')]);
    expect(g.map((x) => x.date)).toEqual(['2026-11-17', '2026-11-18']);
    expect(g[0].rows.map((r) => r.ticker)).toEqual(['A', 'C']);
  });
  it('words the report time', () => {
    expect(hourWords('bmo')).toBe('before the open');
    expect(hourWords('amc')).toBe('after the close');
    expect(hourWords(null)).toBe('time not stated');
  });
  it('computes the default cut-off', () => {
    expect(hoursAgoIso(Date.parse('2026-11-13T12:00:00Z'), 24)).toBe('2026-11-12T12:00:00.000Z');
  });
});
