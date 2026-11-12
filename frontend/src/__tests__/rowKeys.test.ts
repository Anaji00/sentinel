import { describe, expect, it } from 'vitest';
import { nextRowIndex } from '../lib/rowKeys';

describe('nextRowIndex', () => {
  it('steps down and up, stopping at the ends', () => {
    expect(nextRowIndex('ArrowDown', 0, 3)).toBe(1);
    expect(nextRowIndex('ArrowDown', 2, 3)).toBe(2);
    expect(nextRowIndex('ArrowUp', 1, 3)).toBe(0);
    expect(nextRowIndex('ArrowUp', 0, 3)).toBe(0);
  });
  it('accepts j and k', () => {
    expect(nextRowIndex('j', 0, 3)).toBe(1);
    expect(nextRowIndex('k', 2, 3)).toBe(1);
  });
  it('jumps with Home and End', () => {
    expect(nextRowIndex('Home', 2, 5)).toBe(0);
    expect(nextRowIndex('End', 0, 5)).toBe(4);
  });
  it('starts from the first or last row when none is current', () => {
    expect(nextRowIndex('ArrowDown', -1, 3)).toBe(0);
    expect(nextRowIndex('ArrowUp', -1, 3)).toBe(2);
  });
  it('ignores other keys and empty lists', () => {
    expect(nextRowIndex('a', 0, 3)).toBeNull();
    expect(nextRowIndex('ArrowDown', 0, 0)).toBeNull();
  });
});
