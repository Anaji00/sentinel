import { describe, expect, it } from 'vitest';
import { FIRST_LOOK_STEPS, hasSeenFirstLook, markFirstLookSeen } from '../lib/firstLook';

const mem = () => {
  const m: Record<string, string> = {};
  return { getItem: (k: string) => m[k] ?? null, setItem: (k: string, v: string) => void (m[k] = v) };
};

describe('first look', () => {
  it('shows until it is dismissed, then not again', () => {
    const s = mem();
    expect(hasSeenFirstLook(s)).toBe(false);
    markFirstLookSeen(s);
    expect(hasSeenFirstLook(s)).toBe(true);
  });

  it('survives missing or broken storage', () => {
    const broken = { getItem: () => { throw new Error('x'); }, setItem: () => { throw new Error('x'); } };
    expect(hasSeenFirstLook(null)).toBe(false);
    expect(hasSeenFirstLook(broken)).toBe(false);
    expect(() => markFirstLookSeen(broken)).not.toThrow();
    expect(() => markFirstLookSeen(null)).not.toThrow();
  });

  it('keeps to four short steps in the drawer order', () => {
    expect(FIRST_LOOK_STEPS.map((s) => s.title)).toEqual(['At the top', 'What changed', 'Bull and bear case', 'More about it']);
    for (const s of FIRST_LOOK_STEPS) expect(s.text.length).toBeLessThan(200);
  });
});
