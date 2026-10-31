import { describe, it, expect } from 'vitest';
import { flashClass, flashDirection } from '../components/ui/useFlash';

describe('live value flash', () => {
  it('flashes only on a real change', () => {
    expect(flashDirection(undefined, 10)).toBeNull(); // first sight
    expect(flashDirection(10, 10)).toBeNull();
    expect(flashDirection(10, null)).toBeNull();
    expect(flashDirection(10, Number.NaN)).toBeNull();
    expect(flashDirection(10, 10.5)).toBe('up');
    expect(flashDirection(10, 9.5)).toBe('down');
  });

  it('maps to the classes globals.css defines', () => {
    expect(flashClass('up')).toBe('flash-up');
    expect(flashClass('down')).toBe('flash-down');
    expect(flashClass(null)).toBe('');
  });
});
