import { describe, expect, it } from 'vitest';
import { plainForm } from '../lib/plainFiling';

describe('plainForm', () => {
  it('names the common forms', () => {
    expect(plainForm('SCHEDULE 13G')).toBe('Large stake, passive');
    expect(plainForm('SCHEDULE 13D/A')).toBe('Large stake, with plans (update)');
    expect(plainForm('8-K')).toBe('Company news');
    expect(plainForm('4')).toBe('Insider trade');
    expect(plainForm('424B2')).toBe('Share sale paperwork');
  });
  it('keeps a code it does not know, and handles none', () => {
    expect(plainForm('XYZ-9')).toBe('XYZ-9');
    expect(plainForm('')).toBe('Filing');
    expect(plainForm(null)).toBe('Filing');
  });
});
