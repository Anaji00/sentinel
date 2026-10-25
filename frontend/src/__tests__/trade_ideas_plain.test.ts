import { describe, it, expect } from 'vitest';
import { ideaDetails, plainIdea, plainOption } from '../components/TradeIdeas';

/**
 * The plain-English lines under each trade idea must say what to do, the most
 * that can be lost, and never more confidence than the model has earned.
 */

const leg = (right: 'C' | 'P', side: 'buy' | 'sell', strike: number, mid: number) => ({
  symbol: 'X',
  right,
  side,
  strike,
  delta: 0.4,
  iv: 0.4,
  mid,
});

const base = {
  expiry: '2026-11-20',
  dte: 43,
  prob_profit_by_delta: 0.4,
  iv_regime: 'fair',
  atm_iv: 0.4,
  realized_vol: 0.4,
};

describe('plain trade ideas', () => {
  it('describes a call purchase with its maximum loss and breakeven', () => {
    const text = plainOption('MU', {
      ...base,
      structure: 'long_call',
      legs: [leg('C', 'buy', 1115, 39.63)],
      net: -3963,
      max_loss: 3963,
      max_gain: null,
      breakeven: 1154.63,
    });
    expect(text).toContain('Buy one $1,115.00 call');
    expect(text).toContain('The most you can lose is that $3,963');
    expect(text).toContain('above $1,154.63');
  });

  it('describes a credit spread by what you collect and the line you need held', () => {
    const text = plainOption('ADI', {
      ...base,
      structure: 'bull_put_credit',
      legs: [leg('P', 'sell', 400, 9), leg('P', 'buy', 390, 6)],
      net: 300,
      max_loss: 700,
      max_gain: 300,
      breakeven: 397,
    });
    expect(text).toContain('collecting about $300');
    expect(text).toContain('stays above $400.00');
    expect(text).toContain('most you can lose: $700');
  });

  it('never presents a supported model as validated, and says the edge is small', () => {
    const idea = {
      ticker: 'MU',
      direction: 1 as const,
      percentile: 0.99,
      action: 'BUY',
      entry: 1000,
      stop: 920,
      exit: 'after 5 trading days',
      expected_excess_bps: 13,
      realized_vol: 0.47,
      cp_iv_spread: null,
      option: null,
    };
    const text = plainIdea(idea, 5, 13, false);
    expect(text).toContain('not yet fully validated');
    // A buy is judged against the average stock, not the S&P 500 (9 Oct 2026).
    expect(text).toContain('beat the average stock by about 0.13%');
    expect(text).toContain('not a promise for this one');
    expect(text).toContain('$920.00');
  });
});

describe('event-led ideas', () => {
  it('names the evidence and says the model is not behind it', () => {
    const idea = {
      ticker: 'ABC', direction: 1, percentile: null, action: 'buy', entry: 20, stop: 18.5, exit: '5 days',
      expected_excess_bps: null, realized_vol: 0.3, cp_iv_spread: null, option: null,
      evidence: ['insider clusters, 90d: 3 (research-backed)', 'block buy: 12 event(s) (research-backed)'],
    } as unknown as Parameters<typeof plainIdea>[0];
    const text = plainIdea(idea, 5, null, false);
    expect(text).toContain('points up: insider clusters');
    expect(text).toContain('The model is not behind this one');
  });
});

describe('idea card details', () => {
  const base = {
    ticker: 'XYZ', direction: 1, percentile: 0.97, action: 'BUY', entry: 50, stop: 46, exit: '21 days',
    expected_excess_bps: 40, realized_vol: 0.35, cp_iv_spread: null, option: null,
  };

  it('gives the model reasons, the earnings warning, the board and the risk in dollars', () => {
    const idea = {
      ...base,
      reasons: ['has risen strongly over the past year', 'is cheap for its earnings'],
      earnings: { next_estimated: '2026-10-28', last: '2026-07-29', inside_hold: true },
      board: { headline: null, confidence: null, for: ['insider buying'], against: ['block sell', 'filing'] },
      risk: { loss_at_stop_usd: 800, cost_bps: 4.2, vol_sized_usd: 4500 },
    } as unknown as Parameters<typeof ideaDetails>[0];
    const d = Object.fromEntries(ideaDetails(idea).map((x) => [x.label, x]));
    expect(d.Why.text).toBe('XYZ has risen strongly over the past year; is cheap for its earnings.');
    expect(d.Earnings.tone).toBe('warn');
    expect(d.Earnings.text).toContain('2026-10-28');
    // More evidence against than for is flagged.
    expect(d['Other signals'].tone).toBe('warn');
    expect(d['Other signals'].text).toContain('against: block sell; filing');
    expect(d.Risk.text).toContain('loses about $800');
    expect(d.Risk.text).toContain('hold about $4,500');
  });

  it('says nothing it does not know', () => {
    const idea = base as unknown as Parameters<typeof ideaDetails>[0];
    expect(ideaDetails(idea)).toEqual([]);
  });
});
