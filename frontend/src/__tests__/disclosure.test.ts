import { describe, expect, it } from 'vitest';
import { OPEN_PREFIX, readOpen, writeOpen, type OpenStore } from '../lib/disclosure';

const memory = (): OpenStore & { data: Record<string, string> } => {
  const data: Record<string, string> = {};
  return { data, getItem: (k) => (k in data ? data[k] : null), setItem: (k, v) => void (data[k] = v) };
};
const broken: OpenStore = {
  getItem: () => {
    throw new Error('blocked');
  },
  setItem: () => {
    throw new Error('full');
  },
};

describe('remembered open state', () => {
  it('starts at the default and remembers what was chosen', () => {
    const s = memory();
    expect(readOpen('a', false, s)).toBe(false);
    writeOpen('a', true, s);
    expect(s.data[OPEN_PREFIX + 'a']).toBe('1');
    expect(readOpen('a', false, s)).toBe(true);
    writeOpen('a', false, s);
    expect(readOpen('a', true, s)).toBe(false);
  });

  it('keeps each section separate and ignores values it did not write', () => {
    const s = memory();
    writeOpen('a', true, s);
    expect(readOpen('b', false, s)).toBe(false);
    s.data[OPEN_PREFIX + 'c'] = 'maybe';
    expect(readOpen('c', true, s)).toBe(true);
  });

  it('works with no key, no storage, or storage that throws', () => {
    expect(readOpen(undefined, true, memory())).toBe(true);
    expect(readOpen('a', true, null)).toBe(true);
    expect(readOpen('a', false, broken)).toBe(false);
    expect(() => writeOpen('a', true, broken)).not.toThrow();
    expect(() => writeOpen(undefined, true, memory())).not.toThrow();
  });
});
