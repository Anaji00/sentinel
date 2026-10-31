import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Components colour through the role tokens (positive, negative, caution,
 * info, civic, accent, ink, line, the surfaces), never through Tailwind's hue
 * scales.
 *
 * Measured before the sweep: 172 raw hue classes plus 41 on the old cyan alias,
 * so "good" was emerald in one panel and green in the next, a blue
 * informational badge looked clickable, and a light theme could not be added
 * without editing every one of them. Dimming scrims (`bg-black/80`) are allowed:
 * they darken whatever is behind a dialog, in either theme.
 */

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const HUES =
  'red|green|emerald|teal|lime|amber|yellow|orange|sky|blue|cyan|indigo|violet|purple|fuchsia|pink|rose|slate|gray|zinc|neutral|stone';
const UTILS = 'text|bg|border|ring|stroke|fill|shadow|decoration|accent|outline|divide|placeholder|from|via|to';
// `(?:-[trblxyse])?` covers side-specific borders and rings (border-t-cyan-400),
// which the first version of this pattern let through.
const RAW_HUE = new RegExp(
  `(?<![\\w-])(?:[a-z-]+:)*(?:${UTILS})(?:-[trblxyse])?-(?:${HUES})-\\d{2,3}(?:/\\d+)?(?![\\w-])`,
  'g',
);
const RAW_WHITE = /(?<![\w-])(?:[a-z-]+:)*(?:text|bg|border)-white(?:\/\d+)?(?![\w-])/g;

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const full = path.join(dir, name);
    if (statSync(full).isDirectory()) return name === '__tests__' ? [] : sources(full);
    return full.endsWith('.tsx') ? [full] : [];
  });
}

describe('colour tokens', () => {
  it('no component uses a raw Tailwind hue class', () => {
    const offenders = sources(SRC).flatMap((file) => {
      const text = readFileSync(file, 'utf-8');
      const hits = [...(text.match(RAW_HUE) ?? []), ...(text.match(RAW_WHITE) ?? [])];
      return hits.map((h) => `${path.relative(SRC, file)}: ${h}`);
    });
    expect(offenders).toEqual([]);
  });

  it('the pattern catches what it is meant to', () => {
    // A guard that matches nothing passes forever.
    const sample = 'text-emerald-400 hover:bg-sky-500/20 to-blue-600 border-t-cyan-400 text-white bg-positive/10 bg-black/80';
    const hits = [...(sample.match(RAW_HUE) ?? []), ...(sample.match(RAW_WHITE) ?? [])];
    expect(hits).toEqual(['text-emerald-400', 'hover:bg-sky-500/20', 'to-blue-600', 'border-t-cyan-400', 'text-white']);
  });
});
