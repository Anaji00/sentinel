import { describe, expect, it } from 'vitest';
import { HIT_RATE_CAUTION, MIN_GRADED, orderCounts, orderStatusWords, tooFewToGrade } from '../lib/trackWords';

describe('grading threshold', () => {
  it('calls fewer than twenty resolved calls too few', () => {
    expect(tooFewToGrade(0)).toBe(true);
    expect(tooFewToGrade(undefined)).toBe(true);
    expect(tooFewToGrade(MIN_GRADED - 1)).toBe(true);
    expect(tooFewToGrade(MIN_GRADED)).toBe(false);
  });

  it('warns that a hit rate alone proves little', () => {
    expect(HIT_RATE_CAUTION).toContain('70%');
    expect(HIT_RATE_CAUTION).toContain('gap to what chance');
  });
});

describe('order status words', () => {
  it('never calls an unconfirmed order rejected or failed', () => {
    const w = orderStatusWords('unknown');
    expect(w.text).toBe('Not confirmed, being checked');
    expect(w.tone).toBe('pending');
    expect(w.text.toLowerCase()).not.toMatch(/reject|fail|refus/);
  });

  it('shows a duplicate quietly and a refusal as one', () => {
    expect(orderStatusWords('duplicate')).toEqual({ text: 'Already sent today', tone: 'neutral' });
    expect(orderStatusWords('rejected').tone).toBe('bad');
    expect(orderStatusWords('some_new_state').text).toBe('some new state');
    expect(orderStatusWords('').text).toBe('Sent');
  });

  it('counts refused and unconfirmed orders separately', () => {
    expect(orderCounts([{ status: 'rejected' }, { status: 'unknown' }, { status: 'unknown' }, { status: 'duplicate' }])).toEqual({ rejected: 1, unconfirmed: 2 });
  });
});

import { aboutPeriods, independentUnit, skillLine } from '../lib/trackWords';

describe('skill against chance', () => {
  const weekly = { hit_called: 0.351, chance_hit: 0.5, n_independent: 3.6, skill_verdict: 'too few' };

  it('says too few, with the hit rate beside chance and the independent weeks', () => {
    expect(skillLine(weekly, 'weekly')).toBe('hit 35% against 50% by chance; only about 4 independent weeks so far, too few to grade (needs 20)');
  });

  it('never says worse than chance unless the verdict does', () => {
    expect(skillLine(weekly, 'weekly')).not.toContain('worse than chance');
    expect(skillLine({ ...weekly, skill_verdict: 'worse than chance', n_independent: 30 }, 'weekly')).toContain('worse than chance');
    expect(skillLine({ ...weekly, skill_verdict: 'skill', hit_called: 0.62, n_independent: 40 }, 'weekly')).toContain('better than chance');
    expect(skillLine({ ...weekly, skill_verdict: 'none', n_independent: 40 }, 'weekly')).toContain('no better than chance');
  });

  it('uses the horizon for the unit and rounds periods', () => {
    expect(independentUnit('daily')).toBe('days');
    expect(independentUnit('monthly')).toBe('months');
    expect(independentUnit('mystery')).toBe('periods');
    expect(aboutPeriods(0.4)).toBe('less than 1');
    expect(aboutPeriods(10.4)).toBe('about 10');
    expect(aboutPeriods(null)).toBe('few');
  });

  it('falls back to the plain rules when the route sent no chance fields', () => {
    expect(skillLine(undefined, 'weekly')).toBeNull();
    expect(skillLine({}, 'weekly')).toBeNull();
    expect(skillLine({ skill_verdict: 'too few' }, 'daily')).toBe('hit rate not available; only few independent days so far, too few to grade (needs 20)');
  });
});
