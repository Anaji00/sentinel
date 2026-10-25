import { describe, it, expect } from 'vitest';
import {
  pickTopCalls,
  rankWords,
  rightWayOdds,
  expectedMove,
  type Idea,
  type Recommendations,
} from '../lib/ideas';
import { sparkPaths } from '../components/ui/Sparkline';
import { edgeShare } from '../components/ui/ConvictionMeter';
import { ideaFor } from '../components/TickerHero';
import { optionHeadline } from '../components/IdeaCard';

/**
 * The cards lead with what a person acts on: the strongest sized calls, the
 * odds the right way round for a short, and a chart that never invents data.
 */

const idea = (ticker: string, direction: 1 | -1, bps: number, weight: number, p = 0.55): Idea =>
  ({
    ticker,
    direction,
    percentile: 0.99,
    action: direction > 0 ? 'BUY' : 'SELL SHORT',
    entry: 10,
    stop: 9,
    exit: '',
    expected_excess_bps: null,
    realized_vol: null,
    cp_iv_spread: null,
    option: null,
    forecast: {
      expected_excess_bps: bps,
      vol_annual: 0.3,
      prob_beats_spy: p,
      stock_weight: weight,
      edge_after_costs: true,
      target: null,
    },
  }) as Idea;

const rec = (horizons: Record<string, Idea[]>): Recommendations =>
  ({
    horizons: Object.fromEntries(
      Object.entries(horizons).map(([h, ideas]) => [
        h,
        { horizon_days: h === 'monthly' ? 21 : 5, validated: false, ideas },
      ]),
    ),
    intraday: [],
  }) as unknown as Recommendations;

describe('top calls', () => {
  it('ranks sized ideas by expected move and shows each stock once', () => {
    const r = rec({
      monthly: [idea('AAA', 1, 300, 0.05), idea('BBB', -1, -500, 0.05), idea('CCC', 1, 900, 0)],
      weekly: [idea('AAA', 1, 100, 0.05), idea('DDD', -1, -200, 0.05)],
    });
    const top = pickTopCalls(r, 3).map((c) => `${c.idea.ticker}:${c.horizon}`);
    expect(top).toEqual(['BBB:monthly', 'AAA:monthly', 'DDD:weekly']);
    expect(pickTopCalls(undefined)).toEqual([]);
  });

  it('takes buys and shorts in turn, filling from one side when the other runs out', () => {
    const r = rec({
      monthly: [
        idea('S1', -1, -900, 0.05),
        idea('S2', -1, -800, 0.05),
        idea('S3', -1, -700, 0.05),
        idea('L1', 1, 100, 0.05),
      ],
    });
    expect(pickTopCalls(r, 4).map((c) => c.idea.ticker)).toEqual(['S1', 'L1', 'S2', 'S3']);
  });

  it('says a rank in words from the right end', () => {
    expect(rankWords(0.995)).toBe('Top 1%');
    expect(rankWords(0.95)).toBe('Top 5%');
    expect(rankWords(0.03)).toBe('Bottom 3%');
    expect(rankWords(0)).toBe('Bottom 1%');
  });

  it('reads the odds the right way round for a short', () => {
    expect(rightWayOdds(idea('X', 1, 100, 0.05, 0.6))).toBeCloseTo(0.6);
    expect(rightWayOdds(idea('X', -1, -100, 0.05, 0.35))).toBeCloseTo(0.65);
    expect(expectedMove(idea('X', 1, 290, 0.05))).toBeCloseTo(2.9);
  });

  it('finds the stock page its call, the longest horizon first', () => {
    const r = rec({ weekly: [idea('AAA', 1, 100, 0.05)], monthly: [idea('AAA', 1, 300, 0.05)] });
    expect(ideaFor(r, 'AAA')?.horizon).toBe('monthly');
    expect(ideaFor(r, 'ZZZ')).toBeNull();
  });
});

describe('card visuals', () => {
  it('draws nothing from too little data and stays inside its box', () => {
    expect(sparkPaths([1], 100, 40)).toEqual({ line: '', area: '' });
    const { line } = sparkPaths([1, 3, 2], 100, 40);
    const ys = [...line.matchAll(/,(-?[\d.]+)/g)].map((m) => Number(m[1]));
    expect(Math.min(...ys)).toBeGreaterThanOrEqual(0);
    expect(Math.max(...ys)).toBeLessThanOrEqual(40);
  });

  it('fills the conviction bar from a coin flip, not from zero', () => {
    expect(edgeShare(0.5)).toBe(0);
    expect(edgeShare(0.4)).toBe(0);
    expect(edgeShare(0.625)).toBeCloseTo(0.5);
    expect(edgeShare(0.9)).toBe(1);
  });

  it('names an options trade in one line', () => {
    const o = {
      structure: 'bull_call_debit',
      expiry: '2026-11-20',
      legs: [
        { strike: 235, right: 'C' },
        { strike: 237.5, right: 'C' },
      ],
    } as unknown as Parameters<typeof optionHeadline>[0];
    expect(optionHeadline(o)).toBe('Call spread: bet on a rise $235/$237.50 · 2026-11-20');
  });
});
