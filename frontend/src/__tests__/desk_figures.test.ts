import { describe, expect, it } from 'vitest';
import { bookFigure, openSituations } from '../lib/ribbon';
import { Z_LIMIT, isNotable, zX } from '../lib/zWhisker';

describe('book figure', () => {
  it('shows the book against the S&P 500 with the gap deciding the colour', () => {
    expect(bookFigure({ return_pct: -2.37, spy_return_pct: -0.65, sessions: 15 })).toMatchObject({ text: '−2.37% vs S&P −0.65%', tone: 'negative' });
    expect(bookFigure({ return_pct: 3, spy_return_pct: 1 })?.tone).toBe('positive');
    expect(bookFigure({ return_pct: 1.0, spy_return_pct: 0.9 })?.tone).toBe('neutral');
  });

  it('is absent without a measured return, and shows the book alone without an index figure', () => {
    expect(bookFigure(null)).toBeNull();
    expect(bookFigure({})).toBeNull();
    expect(bookFigure({ return_pct: Number.NaN })).toBeNull();
    expect(bookFigure({ return_pct: 1.5 })?.text).toBe('+1.50%');
  });
});

describe('open situations', () => {
  it('counts unresolved divergences once per stock and ignores other rules', () => {
    const alerts = [
      { correlation_id: '1', description: 'AAA moved', tags: ['divergence'], situation: { stage: 'CONFIRMED' } },
      { correlation_id: '2', description: 'AAA moved again', tags: ['divergence'], situation: { stage: 'DEVELOPING' } },
      { correlation_id: '3', description: 'BBB moved', tags: ['divergence'], situation: { stage: 'RESOLVED' } },
      { correlation_id: '4', description: 'CCC moved', tags: ['MACRO'] },
      { correlation_id: '5', description: 'DDD moved', situation: { stage: 'ESCALATING' } },
    ];
    expect(openSituations(alerts)).toBe(2);
    expect(openSituations([])).toBe(0);
    expect(openSituations(null)).toBe(0);
  });
});

describe('gap gauge geometry', () => {
  it('puts zero in the middle and clamps to the ends', () => {
    expect(zX(0, 64)).toBe(32);
    expect(zX(-Z_LIMIT, 64)).toBe(0);
    expect(zX(99, 64)).toBe(64);
    expect(zX(2.5, 64)).toBeCloseTo(52, 5);
  });

  it('colours only gaps of two or more', () => {
    expect(isNotable(1.9)).toBe(false);
    expect(isNotable(-2)).toBe(true);
    expect(isNotable(null)).toBe(false);
  });
});

import { alertTicker, openSituationList } from '../lib/divergence';

describe('desk list', () => {
  it('finds the stock from the alert text', () => {
    expect(alertTicker({ description: 'IRM moved -4.32% since the last five sessions' })).toBe('IRM');
    expect(alertTicker({ description: 'BRK.B moved' })).toBe('BRK.B');
    expect(alertTicker({ description: 'the market moved' })).toBeNull();
    expect(alertTicker({})).toBeNull();
  });

  it('lists open situations newest first, one per stock, without resolved ones or other rules', () => {
    const a = (id: string, t: string, at: string, stage = 'CONFIRMED', tags = ['divergence']) => ({ correlation_id: id, detected_at: at, description: `${t} moved`, tags, situation: { stage } });
    const list = openSituationList([
      a('1', 'AAA', '2026-11-11T10:00:00Z'),
      a('2', 'AAA', '2026-11-11T12:00:00Z', 'ESCALATING'),
      a('3', 'BBB', '2026-11-11T11:00:00Z', 'RESOLVED'),
      { correlation_id: '4', detected_at: '2026-11-11T09:00:00Z', description: 'CCC moved', tags: ['MACRO'], situation: null },
      a('5', 'DDD', '2026-11-11T08:00:00Z'),
    ]);
    expect(list.map((x) => [x.ticker, x.correlation_id])).toEqual([['AAA', '2'], ['DDD', '5']]);
    expect(openSituationList(null)).toEqual([]);
  });
});
