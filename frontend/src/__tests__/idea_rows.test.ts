import { describe, expect, it } from 'vitest';
import { ideaFine, ideaSentence } from '../components/IdeaRow';
import type { Idea } from '../lib/ideas';

const idea = (direction: 1 | -1, bps: number | null, prob: number | null, industryPct: number | null = 0.97): Idea =>
  ({
    ticker: 'AAA', direction, percentile: 0.99, industry_percentile: industryPct,
    forecast: { expected_excess_bps: bps, vol_annual: 0.3, prob_beats_spy: prob, stock_weight: 0.05, edge_after_costs: true, target: null },
  }) as unknown as Idea;

describe('ideaSentence', () => {
  it('states the expected move against the S&P 500 with a real minus', () => {
    expect(ideaSentence(idea(1, 90, 0.52), 21, false)).toBe('The model expects +0.9%, better than the S&P 500 over the next 21 trading days.');
    expect(ideaSentence(idea(-1, -150, 0.43), 21, false)).toBe('The model expects −1.5%, worse than the S&P 500 over the next 21 trading days.');
    expect(ideaSentence(idea(1, 90, 0.5), 1, false)).toContain('over the next day.');
  });
  it('says no forecast on a plateau or with no value', () => {
    expect(ideaSentence(idea(1, 32.5, 0.5), 21, true)).toContain('No forecast for this stock');
    expect(ideaSentence(idea(1, null, null), 21, false)).toContain('No forecast for this stock');
  });
});

describe('ideaFine', () => {
  it('gives the rank and the odds the right way round for a short', () => {
    expect(ideaFine(idea(-1, -100, 0.43), false)).toBe('Top 3% of its industry · chance it goes our way 57%');
  });
  it('drops the odds on a plateau', () => {
    expect(ideaFine(idea(1, 32.5, 0.6), true)).toBe('Top 3% of its industry');
  });
});
