import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { THEME_BOOT, THEME_KEY, parseTheme } from '../lib/theme';

describe('theme', () => {
  it('defaults to dark for anything but an explicit light', () => {
    expect(parseTheme('light')).toBe('light');
    expect(parseTheme('dark')).toBe('dark');
    expect(parseTheme(null)).toBe('dark');
    expect(parseTheme('sepia')).toBe('dark');
  });

  it('the boot script reads the same key the toggle writes', () => {
    expect(THEME_BOOT).toContain(`'${THEME_KEY}'`);
    // It is inlined as a string: it has to parse on its own.
    expect(() => new Function(THEME_BOOT)).not.toThrow();
  });

  it('every token the dark theme sets, the light theme sets too', () => {
    // A token missing from the light block keeps its dark value: dark text on
    // a light page, the classic unreadable-theme bug.
    const css = readFileSync(new URL('../app/globals.css', import.meta.url), 'utf-8');
    const block = (sel: RegExp) => {
      const start = css.search(sel);
      return start < 0 ? '' : css.slice(start, css.indexOf('}', start));
    };
    const names = (b: string) => new Set([...b.matchAll(/(--[a-z-]+):/g)].map((m) => m[1]));
    const dark = names(block(/:root \{\s*\/\* Surfaces/));
    const light = names(block(/:root\[data-theme="light"\] \{/));
    expect(dark.size).toBeGreaterThan(15);
    const missing = [...dark].filter((t) => !light.has(t) && !['--radius', '--font-mono'].includes(t));
    expect(missing).toEqual([]);
  });
});
