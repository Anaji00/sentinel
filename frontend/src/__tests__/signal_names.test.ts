import { describe, it, expect } from 'vitest';
import { familyName, horizonWords, FAMILY_NAME } from '../lib/signalNames';

describe('signal names', () => {
  it('names known families plainly and makes new ones readable', () => {
    expect(familyName('8k_dilution')).toBe('New shares sold privately');
    expect(familyName('insider_cluster')).toBe('Several insiders buying');
    expect(familyName('8k_new_thing')).toBe('Filing new thing');
    expect(familyName('brand_new_family')).toBe('Brand new family');
    expect(Object.values(FAMILY_NAME).every((v) => v.length > 0 && v.length <= 40)).toBe(true);
  });

  it('says horizons in words', () => {
    expect(horizonWords('5d')).toBe('next 5 days');
    expect(horizonWords('1d')).toBe('next day');
    expect(horizonWords('3h')).toBe('next 3 hours');
    expect(horizonWords('1w')).toBe('next week');
    expect(horizonWords('monthly')).toBe('monthly');
  });
});
