import { describe, expect, it } from 'vitest';
import { companyNewsTone, entityLabel } from '../lib/eventBadge';

describe('companyNewsTone', () => {
  it('reads tone from either payload name', () => {
    expect(companyNewsTone({ source: 'alpaca_news', financial_data: { tone: 0.91 } })).toEqual({
      label: 'Positive tone',
      tone: 0.91,
    });
    expect(companyNewsTone({ source: 'alpaca_news', domain_data: { tone: -0.95 } })).toEqual({
      label: 'Negative tone',
      tone: -0.95,
    });
  });

  it('never falls back to a severity label for company news without a tone', () => {
    expect(companyNewsTone({ source: 'alpaca_news' })).toEqual({ label: 'Company news', tone: null });
    expect(companyNewsTone({ source: 'alpaca_news', domain_data: { tone: 'high' } })).toEqual({
      label: 'Company news',
      tone: null,
    });
  });

  it('leaves every other source to its anomaly score', () => {
    expect(companyNewsTone({ source: 'sec_edgar', financial_data: { tone: 0.95 } })).toBeNull();
    expect(companyNewsTone({})).toBeNull();
  });
});

describe('entityLabel', () => {
  it('prefers a name, then falls back to the ticker', () => {
    expect(entityLabel({ primary_entity_name: 'Nvidia', primary_entity_id: 'NVDA' })).toBe('Nvidia');
    expect(entityLabel({ primary_entity_id: 'NVDA' })).toBe('NVDA');
  });

  it('is empty rather than inventing an unknown entity', () => {
    expect(entityLabel({})).toBe('');
  });
});
