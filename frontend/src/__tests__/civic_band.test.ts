import { describe, it, expect } from 'vitest';
import { asCount, topVendors, type MoneyTotal } from '../components/WhoIsWatching';
import { recordWords } from '../components/TradeIdeas';
import { flukeChance, VERDICT_WORD } from '../components/TradingSignals';
import { triggerWords, unusualWord } from '../components/SignalBoard';

/**
 * The home page's civic band reads the API's counts as they arrive, and the
 * stock pages say in words what used to be statistics.
 */

describe('who is watching', () => {
  it('reads a count the API sends as a string', () => {
    expect(asCount('343965888')).toBe(343965888);
    expect(asCount(2278)).toBe(2278);
    expect(asCount(null)).toBeNull();
    expect(asCount('n/a')).toBeNull();
  });

  it('ranks contract vendors by money, ignoring grants', () => {
    const totals: MoneyTotal[] = [
      { kind: 'contract', matched: 'Axon', awards: 569, total: 513e6 },
      { kind: 'grant', matched: 'license plate reader', awards: 166, total: 64e6 },
      { kind: 'contract', matched: 'Palantir', awards: 331, total: 3_996e6 },
      { kind: 'contract', matched: 'Nobody', awards: 0, total: 0 },
    ];
    const top = topVendors(totals);
    expect(top.map((v) => v.matched)).toEqual(['Palantir', 'Axon']);
    expect(top[0].share).toBe(1);
    expect(top[1].share).toBeCloseTo(513 / 3996);
    expect(topVendors(undefined)).toEqual([]);
  });
});

describe('plain words for statistics', () => {
  it('grades a track record without the Sharpe ratio', () => {
    expect(recordWords(2.1)).toBe('strong');
    expect(recordWords(1.0)).toBe('good');
    expect(recordWords(0.3)).toBe('modest');
    expect(recordWords(-0.2)).toBe('weak');
    expect(recordWords(null)).toBe('not yet measured');
  });

  it('says how likely a pattern is a fluke', () => {
    expect(flukeChance(0.004)).toBe('under 1%');
    expect(flukeChance(0.12)).toBe('12%');
    expect(VERDICT_WORD.proven).toBe('Works');
    expect(VERDICT_WORD.below_costs).toBe('Too small to trade');
  });

  it('names what set a stock off and how unusual it is', () => {
    expect(triggerWords(['market_anomaly', 'equity_block', 'new_thing'])).toBe(
      'unusual price move, large block trade, new thing',
    );
    expect(unusualWord(0.95)).toBe('Very high');
    expect(unusualWord(0.8)).toBe('High');
    expect(unusualWord(0.3)).toBe('Low');
  });
});
