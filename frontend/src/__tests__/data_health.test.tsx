import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import React from 'react';
import {
  GRACE_MS,
  failingFor,
  recordFailure,
  recordSuccess,
  resetDataHealth,
  snapshot,
  spinnerState,
  subscribe,
} from '../lib/dataHealth';
import { DataHealthStrip } from '../components/ui/DataHealthStrip';
import { LoadingLines } from '../components/ui/Skeleton';

beforeEach(() => resetDataHealth());

describe('data health', () => {
  it('counts a source only after it has kept failing for the grace period', () => {
    recordFailure('/a', 1_000);
    expect(failingFor(snapshot(), 1_000 + GRACE_MS - 1)).toBe(0);
    expect(failingFor(snapshot(), 1_000 + GRACE_MS)).toBe(1);
  });

  it('keeps the first failure time across repeats, so retries do not restart the clock', () => {
    recordFailure('/a', 1_000);
    recordFailure('/a', 5_000);
    expect(snapshot()['/a']).toEqual({ since: 1_000, count: 2 });
  });

  it('clears a source on its next success, and only that one', () => {
    recordFailure('/a', 0);
    recordFailure('/b', 0);
    recordSuccess('/a');
    expect(Object.keys(snapshot())).toEqual(['/b']);
    recordSuccess('/never-failed');
    expect(Object.keys(snapshot())).toEqual(['/b']);
  });

  it('tells listeners when something changes, and not after they leave', () => {
    const seen = vi.fn();
    const stop = subscribe(seen);
    recordFailure('/a', 0);
    recordSuccess('/a');
    expect(seen).toHaveBeenCalledTimes(2);
    stop();
    recordFailure('/a', 0);
    expect(seen).toHaveBeenCalledTimes(2);
  });

  it('turns the spinner into an error when sources fail, and into a slow notice only when nothing has', () => {
    expect(spinnerState(0, false)).toBe('loading');
    expect(spinnerState(0, true)).toBe('slow');
    expect(spinnerState(2, false)).toBe('failed');
    expect(spinnerState(1, true)).toBe('failed');
  });

  it('shows nothing when all is well', () => {
    expect(renderToStaticMarkup(<DataHealthStrip />)).toBe('');
  });

  it('keeps the shared spinner an ordinary spinner before anything has failed', () => {
    const html = renderToStaticMarkup(<LoadingLines label="Loading prices" />);
    expect(html).toContain('Loading prices');
    expect(html).not.toContain('Try again');
  });
});
