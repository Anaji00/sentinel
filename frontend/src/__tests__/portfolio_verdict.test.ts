import { describe, expect, it } from 'vitest';
import { LEVEL_BAND, MIN_SESSIONS, portfolioVerdict } from '../lib/portfolioVerdict';

describe('portfolio verdict', () => {
  it('says too early when nothing is scored', () => {
    expect(portfolioVerdict(null, 10).word).toBe('Too early');
    expect(portfolioVerdict(1.2, 0).word).toBe('Too early');
    expect(portfolioVerdict(undefined, undefined).tone).toBe('neutral');
  });

  it('follows the sign of the gap', () => {
    expect(portfolioVerdict(1.7, 90)).toMatchObject({ word: 'Ahead of the S&P 500', tone: 'positive' });
    expect(portfolioVerdict(-1.72, 90)).toMatchObject({ word: 'Behind the S&P 500', tone: 'negative' });
  });

  it('calls a tiny gap level, at the band edge and inside it', () => {
    expect(portfolioVerdict(LEVEL_BAND - 0.01, 90).tone).toBe('neutral');
    expect(portfolioVerdict(-(LEVEL_BAND - 0.01), 90).word).toBe('Level with the S&P 500');
    expect(portfolioVerdict(LEVEL_BAND, 90).tone).toBe('positive');
  });

  it('warns that a short record proves little, whatever the gap', () => {
    const v = portfolioVerdict(-1.72, 14);
    expect(v.word).toBe('Behind the S&P 500');
    expect(v.note).toContain('too early to tell skill from luck');
    expect(portfolioVerdict(1, 1).note).toContain('1 trading day ');
    expect(portfolioVerdict(1, MIN_SESSIONS).note).toBe(`Over ${MIN_SESSIONS} trading days.`);
  });
});
