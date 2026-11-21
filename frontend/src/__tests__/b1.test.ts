import { describe, expect, it } from 'vitest';
import { netBetaWords, sectorWords, sideWords } from '../lib/b1';

describe('netBetaWords', () => {
  it('words the size of the market exposure with its sign', () => {
    expect(netBetaWords(0.23)).toBe('+0.23: a little');
    expect(netBetaWords(-0.05)).toBe('−0.05: almost none');
    expect(netBetaWords(0.8)).toBe('+0.80: a lot');
    expect(netBetaWords(null)).toBeNull();
  });
});

describe('sideWords', () => {
  it('shows how many names were measured beside an average', () => {
    expect(sideWords('Bought', { names: 20, measured: 18, mean_beta: 1.12, median_vol: 0.31 })).toBe('Bought: 20 names; for the 18 measured, their average market sensitivity is 1.12, typical yearly swing 31%.');
  });
  it('says none measured instead of showing an average', () => {
    expect(sideWords('Bet against', { names: 20, measured: 0, mean_beta: null, median_vol: null })).toBe('Bet against: 20 names, none measured yet.');
    expect(sideWords('Bet against', { names: 5, measured: 3, mean_beta: null, median_vol: null })).toContain('none measured yet');
  });
});

describe('sectorWords', () => {
  it('lists the largest first and caps the list', () => {
    expect(sectorWords([['Energy', 2], ['Financials', 9], ['Utilities', 1], ['Health Care', 5]], 3)).toBe('Financials 9, Health Care 5, Energy 2');
    expect(sectorWords(undefined)).toBe('');
  });
});
