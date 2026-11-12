import { describe, expect, it } from 'vitest';
import { expectedMoveWords, groupHeldBack, quotaLine, type HeldBackRow, type QuotaModel } from '../lib/heldBack';

const row = (over: Partial<HeldBackRow>): HeldBackRow => ({
  signal: 'A news article reads clearly negative',
  expected_move: 'fall',
  window: 'next day',
  held_back_by: 'liquidity bar',
  explanation: 'Too thinly traded.',
  when: '2026-11-10T10:00:00Z',
  ...over,
});

describe('groupHeldBack', () => {
  it('folds repeated windows into one line, newest time kept', () => {
    const g = groupHeldBack([row({}), row({ window: 'next month', when: '2026-11-11T09:00:00Z' })]);
    expect(g).toHaveLength(1);
    expect(g[0].windows).toEqual(['next day', 'next month']);
    expect(g[0].when).toBe('2026-11-11T09:00:00Z');
  });
  it('keeps different reasons apart, newest first', () => {
    const g = groupHeldBack([row({}), row({ held_back_by: 'borrow check', when: '2026-11-11T00:00:00Z' })]);
    expect(g.map((x) => x.heldBackBy)).toEqual(['borrow check', 'liquidity bar']);
  });
  it('handles no rows', () => {
    expect(groupHeldBack(undefined)).toEqual([]);
  });
  it('words the move', () => {
    expect(expectedMoveWords('rise')).toMatch(/rise/);
  });
});

const model = (over: Partial<QuotaModel>): QuotaModel => ({
  model: 'm',
  provider: 'p',
  status: 'ok',
  cooling_down_seconds: null,
  requests_this_minute: 1,
  requests_per_minute_limit: 10,
  requests_today: 20,
  requests_per_day_limit: 100,
  share_of_limit_used: 0.2,
  ...over,
});

describe('quotaLine', () => {
  it('reads a normal model', () => {
    expect(quotaLine(model({}))).toBe('20 of 100 requests today.');
  });
  it('treats a null limit as no limit, not zero', () => {
    expect(quotaLine(model({ requests_per_day_limit: null }))).toBe('20 requests today, no daily limit.');
  });
  it('says when a model is paused or unused', () => {
    expect(quotaLine(model({ status: 'cooling down', cooling_down_seconds: 300 }))).toMatch(/5 minutes/);
    expect(quotaLine(model({ status: 'no key set' }))).toMatch(/not in use/);
  });
});
