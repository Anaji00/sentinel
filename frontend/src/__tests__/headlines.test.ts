import { describe, expect, it } from 'vitest';
import { callsHeadline, intelHeadline, marketHeadline } from '../lib/headlines';

describe('calls headline', () => {
  it('counts only strong evidence and says which way most of it points', () => {
    const h = callsHeadline([
      { net: -7, confidence: 'higher' },
      { net: -3, confidence: 'higher' },
      { net: 4, confidence: 'higher' },
      { net: 5, confidence: 'moderate' },
      { net: 0, confidence: 'higher' },
    ]);
    expect(h).toMatchObject({ value: '3', tone: 'negative' });
    expect(h?.detail).toBe('1 point up, 2 point down. Tap one to see why.');
  });

  it('says so when nothing is strong, and nothing while loading', () => {
    expect(callsHeadline([])?.value).toBe('0');
    expect(callsHeadline([{ net: 2, confidence: 'low' }])?.tone).toBe('neutral');
    expect(callsHeadline(undefined)).toBeNull();
  });

  it('uses the singular for one stock', () => {
    expect(callsHeadline([{ net: 2, confidence: 'higher' }])?.label).toBe('stock with strong evidence today');
  });
});

describe('markets headline', () => {
  it('shows the signed S&P 500 move with its mood', () => {
    expect(marketHeadline({ spy_change_pct: 0.876, regime: 'stressed' })).toEqual({
      value: '+0.88%', label: 'S&P 500 today', detail: 'A stressed market.', tone: 'positive',
    });
    expect(marketHeadline({ spy_change_pct: -1.2, regime: 'calm' })?.value).toBe('−1.20%');
  });

  it('is absent without a figure rather than showing zero', () => {
    expect(marketHeadline({ regime: 'calm' })).toBeNull();
    expect(marketHeadline(null)).toBeNull();
    expect(marketHeadline({ spy_change_pct: 0.2 })?.detail).toBe('Market mood not measured.');
  });
});

describe('intel headline', () => {
  it('counts zones and dark ships', () => {
    const h = intelHeadline(13, 2983, 468);
    expect(h?.value).toBe('13');
    expect(h?.detail).toBe('468 of 2,983 tracked ships have switched off their transponder.');
    expect(h?.tone).toBe('negative');
  });

  it('says ship tracking is silent instead of showing zero dark ships', () => {
    expect(intelHeadline(2, undefined, undefined)?.detail).toBe('Ship tracking is not reporting right now.');
    expect(intelHeadline(1, 0, 0)?.detail).toBe('Ship tracking is not reporting right now.');
    expect(intelHeadline(0, 100, 5)?.tone).toBe('neutral');
    expect(intelHeadline(undefined, 100, 5)).toBeNull();
  });
});
