import React from 'react';
import { describe, it, expect } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { readFileSync } from 'node:fs';

import { EmptyState } from '../components/ui/EmptyState';
import { PanelSkeleton } from '../components/ui/Skeleton';
import { PALETTE, directionColor, readToken } from '../lib/palette';

const render = (el: React.ReactElement) => renderToStaticMarkup(el);

/**
 * These cover the distinctions the UI audit found collapsed.
 *
 * Measured with the backend unreachable, before this work: `/intelligence`
 * rendered seventeen characters of text inside fifteen kilobytes of skeleton,
 * and `/operations` rendered three headings and nothing else. A reader could
 * not tell loading from empty from broken, and all three are different things
 * to know.
 */
describe('a panel with nothing to show says which nothing it is', () => {
  it('distinguishes loading, empty and error', () => {
    const loading = render(<EmptyState kind="loading" title="Loading…" />);
    const empty = render(<EmptyState kind="empty" title="No failed events" />);
    const error = render(<EmptyState kind="error" title="Could not reach the gateway" />);

    expect(loading).toContain('animate-spin');
    expect(empty).not.toContain('animate-spin');

    // Only the error asks to be announced. A quiet feed is not an alert.
    expect(error).toContain('role="alert"');
    expect(empty).not.toContain('role="alert"');
    expect(loading).not.toContain('role="alert"');
  });

  it('carries the reason, which is what turns a blank box into an answer', () => {
    const html = render(
      <EmptyState
        kind="empty"
        title="No failed events"
        detail="The good state, and worth telling apart from an unreachable table."
      />,
    );
    expect(html).toContain('worth telling apart from an unreachable table');
  });
});

describe('the skeleton names itself and gives up eventually', () => {
  it('renders the title every caller was already passing', () => {
    // `PanelSkeleton` accepted `title` and discarded it, so a loading dashboard
    // was four anonymous grey rectangles per panel.
    const html = render(<PanelSkeleton title="Loading Radar…" />);
    expect(html).toContain('Loading Radar…');
    expect(html).toContain('aria-label="Loading Radar…"');
  });

  it('announces itself politely rather than as an alert', () => {
    const html = render(<PanelSkeleton title="Loading Movers…" />);
    expect(html).toContain('role="status"');
    expect(html).toContain('aria-live="polite"');
  });

  it('has a default for callers that pass nothing', () => {
    expect(render(<PanelSkeleton />)).toContain('Loading');
  });
});

describe('one palette, one meaning per colour', () => {
  it('gives up, down and flat three different answers', () => {
    expect(directionColor(1.4)).toBe(PALETTE.positive);
    expect(directionColor(-1.4)).toBe(PALETTE.negative);
    // Zero is neither. A flat series is not a gain, and colouring it as one is
    // the same overclaim as a warm-up score reported as a measurement.
    expect(directionColor(0)).toBe(PALETTE.textMuted);
  });

  it('treats an absent or non-finite value as unmeasured, not as flat', () => {
    expect(directionColor(null)).toBe(PALETTE.textMuted);
    expect(directionColor(undefined)).toBe(PALETTE.textMuted);
    expect(directionColor(Number.NaN)).toBe(PALETTE.textMuted);
  });

  it('resolves to the literal when there is no DOM to read a token from', () => {
    // Server render has no getComputedStyle; the compiled value is the fallback
    // rather than an empty string, which would paint nothing.
    expect(readToken('accent')).toBe(PALETTE.accent);
  });
});

describe('the design system is the source of colour', () => {
  it('keeps the JS palette in step with the stylesheet', () => {
    const css = readFileSync(new URL('../app/globals.css', import.meta.url), 'utf-8');

    // Charts set colour through SVG attributes, which CSS cannot reach, so the
    // values live in two files. Two copies of a value drift -- this is the
    // check that says so. Three different cyans were in use before this.
    const pairs: Array<[string, string]> = [
      ['--accent', PALETTE.accent],
      ['--positive', PALETTE.positive],
      ['--negative', PALETTE.negative],
      ['--caution', PALETTE.caution],
      ['--info', PALETTE.info],
      ['--bg-base', PALETTE.bgBase],
      ['--bg-raised', PALETTE.bgRaised],
      ['--text-primary', PALETTE.textPrimary],
    ];

    for (const [token, jsValue] of pairs) {
      const match = css.match(new RegExp(`${token}:\\s*([^;]+);`));
      expect(match, `${token} is missing from globals.css`).toBeTruthy();
      expect(match![1].trim().toLowerCase()).toBe(jsValue.toLowerCase());
    }
  });
});
