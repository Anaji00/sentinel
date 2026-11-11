import { describe, expect, it } from 'vitest';
import { confidenceWords, votesWords } from '../components/CallsWorkspace';

describe('calls page words', () => {
  it('says the votes out loud, with direction from the sign', () => {
    expect(votesWords(-7, 7)).toBe('7 of 7 signals point down');
    expect(votesWords(4, 9)).toBe('4 of 9 signals point up');
    expect(votesWords(1, 1)).toBe('1 of 1 signal points up');
  });

  it('puts confidence in plain words and says nothing for none', () => {
    expect(confidenceWords('higher')).toBe('strong evidence');
    expect(confidenceWords('moderate')).toBe('some evidence');
    expect(confidenceWords('low')).toBe('weak evidence');
    expect(confidenceWords('none')).toBe('');
    expect(confidenceWords(undefined)).toBe('');
  });
});
