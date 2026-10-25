import { describe, it, expect } from 'vitest';
import { verdictClass, verdictSide } from '../components/SignalBoard';
import { groupSignals, type ActiveSignal } from '../components/TradingSignals';

/**
 * The signal pages colour a lean by its direction and read one rule firing on
 * many stocks as one card.
 */

describe('signal board verdicts', () => {
  it('reads the direction from the headline', () => {
    expect(verdictSide('Leans bullish (higher confidence)')).toBe(1);
    expect(verdictSide('Leans bearish (moderate confidence)')).toBe(-1);
    expect(verdictSide('No clear lean')).toBe(0);
    expect(verdictSide(undefined)).toBe(0);
  });

  it('never paints a bullish and a bearish call alike', () => {
    for (const c of ['higher', 'moderate', 'low']) {
      const up = verdictClass('Leans bullish', c);
      const down = verdictClass('Leans bearish', c);
      expect(up).toContain('positive');
      expect(down).toContain('negative');
      expect(up).not.toContain('negative');
    }
    expect(verdictClass('No clear lean', 'none')).toContain('ink-mute');
  });
});

const sig = (id: string, families: string[], horizon: string, direction: 'long' | 'short', p: number): ActiveSignal => ({
  id,
  instrument: id.toUpperCase(),
  horizon,
  direction,
  probability: p,
  expected_bps: 134,
  cost_bps: 10,
  created_at: '2026-10-20T13:00:00Z',
  expires_at: '2026-10-25T13:00:00Z',
  entry_price: null,
  evidence: [],
  families,
});

describe('trade signal groups', () => {
  it('folds one rule on many stocks into one group, strongest first', () => {
    const groups = groupSignals([
      sig('spy', ['stock_news_negative'], '5d', 'short', 0.69),
      sig('fxi', ['stock_news_negative'], '5d', 'short', 0.69),
      sig('hlt', ['8k_delisting_notice'], '30d', 'short', 0.77),
      sig('gm', ['stock_news_negative'], '5d', 'long', 0.6),
    ]);
    expect(groups.map((g) => [g.families[0], g.direction, g.signals.length])).toEqual([
      ['8k_delisting_notice', 'short', 1],
      ['stock_news_negative', 'short', 2],
      ['stock_news_negative', 'long', 1],
    ]);
    expect(groups[1].probability).toBeCloseTo(0.69);
  });

  it('treats the same families in any order as one reason', () => {
    const groups = groupSignals([
      sig('a', ['x', 'y'], '5d', 'long', 0.6),
      sig('b', ['y', 'x'], '5d', 'long', 0.7),
    ]);
    expect(groups).toHaveLength(1);
    expect(groups[0].signals[0].instrument).toBe('B');
  });
});
