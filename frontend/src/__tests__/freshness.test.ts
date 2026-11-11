import { beforeEach, describe, expect, it } from 'vitest';
import { agoWords, isOverdue, recordUpdate, resetFreshness, subscribeUpdates, updatedAt } from '../lib/freshness';

describe('freshness', () => {
  beforeEach(() => resetFreshness());

  it('knows nothing until a source answers, then remembers when', () => {
    expect(updatedAt('/a')).toBeNull();
    recordUpdate('/a', 1_000);
    expect(updatedAt('/a')).toBe(1_000);
    recordUpdate('/a', 5_000);
    expect(updatedAt('/a')).toBe(5_000);
    expect(updatedAt('/b')).toBeNull();
  });

  it('tells subscribers, and stops when they leave', () => {
    let n = 0;
    const off = subscribeUpdates(() => n++);
    recordUpdate('/a', 1);
    off();
    recordUpdate('/a', 2);
    expect(n).toBe(1);
  });

  it('says the age in plain words', () => {
    expect(agoWords(2_000)).toBe('just now');
    expect(agoWords(42_000)).toBe('42 s ago');
    expect(agoWords(120_000)).toBe('2 min ago');
    expect(agoWords(3 * 3_600_000)).toBe('3 h ago');
    expect(agoWords(72 * 3_600_000)).toBe('3 days ago');
    expect(agoWords(-5_000)).toBe('just now');
  });

  it('calls a panel overdue only when far behind its own polling', () => {
    expect(isOverdue(100_000, 30_000)).toBe(true);
    expect(isOverdue(90_000, 30_000)).toBe(false);
    expect(isOverdue(50_000, 30_000)).toBe(false);
    expect(isOverdue(200_000, 300_000)).toBe(false);
    expect(isOverdue(1_000_000, 300_000)).toBe(true);
  });
});
