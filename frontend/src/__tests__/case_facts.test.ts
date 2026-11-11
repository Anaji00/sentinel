import { describe, expect, it } from 'vitest';
import { groupFacts, sourceWords } from '../lib/caseFacts';

const f = (id: string, kind: string, text = `fact ${id}`) => ({ id, kind, text, as_of: '2026-11-04', source: 'daily_bars' });

describe('case facts', () => {
  it('groups by kind in a fixed order with unknown kinds last', () => {
    const groups = groupFacts({ facts: [f('F1', 'customer'), f('F2', 'rank'), f('F3', 'zeta'), f('F4', 'rank'), f('F5', 'move')] });
    expect(groups.map((g) => g.kind)).toEqual(['rank', 'move', 'customer', 'zeta']);
    expect(groups[0].facts.map((x) => x.id)).toEqual(['F2', 'F4']);
    expect(groups[0].label).toBe('Model rank');
  });

  it('drops malformed facts and tolerates a missing file', () => {
    expect(groupFacts(null)).toEqual([]);
    expect(groupFacts({})).toEqual([]);
    expect(groupFacts({ facts: [{ id: '', kind: 'rank', text: 'x' }, { id: 'F1', kind: 'rank', text: '' }] })).toEqual([]);
  });

  it('names a source in words and shows an unknown one as stored', () => {
    expect(sourceWords('factor_model')).toBe('the monthly factor model');
    expect(sourceWords('new_feed')).toBe('new feed');
    expect(sourceWords(null)).toBe('an unrecorded source');
  });
});
