import { describe, it, expect } from 'vitest';
import { oversightLine } from '../components/CivicOversight';
import { intensityWord, sectionFromHash, SECTIONS } from '../components/CivicWatch';

describe('oversight in plain words', () => {
  it('names who must approve, or says nobody has to', () => {
    expect(oversightLine({ place: 'Oakland', level: 'city', year: 2018 })).toContain('the city council must approve');
    expect(oversightLine({ place: 'Santa Clara County', level: 'county', year: null })).toContain('the county board');
    expect(oversightLine(null)).toContain('without a public vote');
  });
});

describe('the watch page', () => {
  it('opens a section from the sidebar hash and ignores anything else', () => {
    expect(sectionFromHash('#money')).toBe('money');
    expect(sectionFromHash('#protect')).toBe('protect');
    expect(sectionFromHash('#nope')).toBeNull();
    expect(sectionFromHash('')).toBeNull();
    expect(SECTIONS.map((s) => s.id)).toEqual(['feed', 'accountability', 'money', 'protect']);
  });

  it('describes surveillance intensity in words', () => {
    expect(intensityWord(10)).toBe('Heavy');
    expect(intensityWord(6)).toBe('Significant');
    expect(intensityWord(3)).toBe('Moderate');
    expect(intensityWord(0)).toBe('Light');
  });
});
