import { describe, expect, it } from 'vitest';
import { furthestBehind, stockRecordLine } from '../lib/stockRecord';
import type { TrackSlice } from '../lib/liveFigures';

const since = (iso: string) => iso;
const slice = (over: Partial<TrackSlice>): TrackSlice => ({
  horizon: 'monthly', direction: null, n_calls: 40, n_closed: 30, stopped: 20, share_right: 0.3, mean_net_bps_after_costs: -100, median_net_bps: -90,
  live: { n: 40, since: '2026-10-08' }, ...over,
});

describe('stockRecordLine', () => {
  it('names the stock in the headline', () => {
    const l = stockRecordLine('NVDA', slice({}), since)!;
    expect(l.headline).toBe('Past monthly ideas on NVDA: right 30% of the time over 40 ideas since 2026-10-08');
  });
  it('says plainly when there are no past ideas', () => {
    expect(stockRecordLine('NVDA', slice({ n_calls: 0 }), since)!.headline).toBe('No past ideas on NVDA.');
  });
  it('refuses a rate on a handful of ideas', () => {
    expect(stockRecordLine('NVDA', slice({ n_calls: 4 }), since)!.headline).toContain('too few to judge');
  });
  it('is null without a slice', () => {
    expect(stockRecordLine('NVDA', undefined, since)).toBeNull();
  });
});

describe('furthestBehind', () => {
  it('finds the day of the widest gap between the book and the index', () => {
    const nav = [
      { day: 'a', nav: 100, spy_nav: 100 },
      { day: 'b', nav: 99, spy_nav: 100 },
      { day: 'c', nav: 96, spy_nav: 100 },
      { day: 'd', nav: 99, spy_nav: 100 },
    ];
    const f = furthestBehind(nav)!;
    expect(f.day).toBe('c');
    expect(f.index).toBe(2);
    expect(f.gapPct).toBeCloseTo(-4);
  });
  it('is null when never behind, or too short', () => {
    expect(furthestBehind([{ day: 'a', nav: 100, spy_nav: 100 }, { day: 'b', nav: 101, spy_nav: 100 }, { day: 'c', nav: 102, spy_nav: 100 }])).toBeNull();
    expect(furthestBehind([{ day: 'a', nav: 100, spy_nav: 100 }])).toBeNull();
    expect(furthestBehind(undefined)).toBeNull();
  });
});
