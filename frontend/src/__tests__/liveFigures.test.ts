import { describe, expect, it } from 'vitest';
import { accountChip, clockNy, quoteCaption, sessionWords, sliceHorizon, trackLine, type Quote, type TrackSlice } from '../lib/liveFigures';

const q: Quote = {
  ticker: 'NVDA', price: 166.6, as_of: '2026-11-12T22:55:00+00:00', age_sec: 84, source: 'minute_bar', session: 'after_hours',
  delayed: false, change_pct: 0.25, prev_close: 166.19, change_base_date: '2026-11-12',
};
const since = (iso: string) => iso;
const money = (v: number) => `$${Math.round(v)}`;

describe('quote wording', () => {
  it('words the session and the New York time', () => {
    expect(sessionWords('after_hours')).toBe('After hours');
    expect(sessionWords(null)).toBe('');
    expect(clockNy('2026-11-12T22:55:00+00:00')).toBe('5:55 PM New York time');
    expect(clockNy(null)).toBe('');
  });
  it('captions a trade with its base, session and time', () => {
    expect(quoteCaption(q, 'Nov 12')).toBe('latest trade, change from the Nov 12 close · After hours · as of 5:55 PM New York time');
  });
  it('says when a quote is late, and calls a close a close', () => {
    expect(quoteCaption({ ...q, delayed: true }, null)).toContain('as of 5:55 PM New York time, delayed');
    expect(quoteCaption({ ...q, source: 'daily_close', as_of: null, session: null }, 'Nov 11')).toBe('last close, change from the Nov 11 close');
  });
});

describe('trackLine', () => {
  const slice: TrackSlice = {
    horizon: 'monthly', direction: null, n_calls: 345, n_closed: 237, stopped: 228, share_right: 0.18, mean_net_bps_after_costs: -555.6,
    median_net_bps: -598.6, live: { n: 345, since: '2026-10-08' }, outcome_spread: { p10_bps: -1498.7, p50_bps: -598.6, p90_bps: 477.1, n: 345 },
  };
  it('always shows the sample size and start beside a rate', () => {
    const t = trackLine(slice, since)!;
    expect(t.headline).toBe('Past monthly ideas: right 18% of the time over 345 ideas since 2026-10-08');
    expect(t.detail).toContain('between −15.0% and +4.8%');
    expect(t.detail).toContain('228 of 237 finished ideas were stopped out');
  });
  it('says when a record is one short stretch, with the dates', () => {
    const t = trackLine({ ...slice, n_dates: 6, first_date: '2026-10-08', last_date: '2026-10-14' }, since)!;
    expect(t.headline).toBe('Past monthly ideas: right 18% of the time over 345 ideas, from only 6 days (2026-10-08 to 2026-10-14)');
  });
  it('keeps the since wording when the ideas span many days', () => {
    expect(trackLine({ ...slice, n_dates: 40, first_date: '2026-10-08', last_date: '2026-11-12' }, since)!.headline).toContain('since 2026-10-08');
  });
  it('says how many open ideas had no price yet', () => {
    expect(trackLine({ ...slice, n_open_unmarked: 3 }, since)!.detail).toContain('3 open ideas have no price yet and are not in the share right.');
    expect(trackLine({ ...slice, n_open_unmarked: 1 }, since)!.detail).toContain('1 open idea has no price yet and is not in the share right.');
    expect(trackLine({ ...slice, n_open_unmarked: 0 }, since)!.detail).not.toContain('no price yet');
  });
  it('refuses a rate on a small sample', () => {
    expect(trackLine({ ...slice, n_calls: 12 }, since)!.headline).toContain('too few to judge (12 so far');
  });
  it('is null with nothing to show', () => {
    expect(trackLine(null, since)).toBeNull();
    expect(trackLine({ ...slice, n_calls: 0 }, since)).toBeNull();
  });
  it('maps holding days to a horizon', () => {
    expect([1, 5, 21].map(sliceHorizon)).toEqual(['daily', 'weekly', 'monthly']);
  });
});

describe('accountChip', () => {
  it('shows value, change and the base date', () => {
    const c = accountChip({ equity: 101394.71, day_change: 28.38, day_change_pct: 0.03, base_date: '2026-11-11', positions_count: 173, as_of: null }, money, since)!;
    expect(c.text).toBe('$101395 +0.03%');
    expect(c.tone).toBe('positive');
    expect(c.title).toContain('173 positions');
    expect(c.title).toContain('since the 2026-11-11 record');
  });
  it('shows no change when it is missing, and nothing without equity', () => {
    expect(accountChip({ equity: 100, day_change: null, day_change_pct: null, base_date: null, positions_count: null, as_of: null }, money, since)!.text).toBe('$100');
    expect(accountChip({ equity: null, day_change: null, day_change_pct: null, base_date: null, positions_count: null, as_of: null }, money, since)).toBeNull();
  });
});
