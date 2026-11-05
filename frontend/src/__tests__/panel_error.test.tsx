import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import React from 'react';
import { failedWithout } from '../lib/dataHealth';
import { PanelError } from '../components/ui/PanelError';

describe('failedWithout', () => {
  it('is true only when the request failed and there is nothing older to show', () => {
    expect(failedWithout(new Error('x'), undefined)).toBe(true);
    expect(failedWithout(new Error('x'), null)).toBe(true);
    expect(failedWithout(new Error('x'), [])).toBe(false);
    expect(failedWithout(new Error('x'), { rows: [] })).toBe(false);
    expect(failedWithout(undefined, undefined)).toBe(false);
  });
});

describe('PanelError', () => {
  it('names what could not be loaded and offers a retry as a real button', () => {
    const html = renderToStaticMarkup(<PanelError what="crypto activity" onRetry={() => undefined} />);
    expect(html).toContain('Could not load crypto activity.');
    expect(html).toContain('role="status"');
    expect(html).toContain('type="button"');
    expect(html).toContain('Try again');
  });

  it('shows no retry button when there is nothing to call', () => {
    expect(renderToStaticMarkup(<PanelError what="prices" />)).not.toContain('Try again');
  });

  it('has a one-line form for tight places', () => {
    const html = renderToStaticMarkup(<PanelError inline what="prices" onRetry={() => undefined} />);
    expect(html.startsWith('<span')).toBe(true);
    expect(html).toContain('Could not load prices.');
  });
});
