import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

// Stands in for the grid so the test can see whether it was mounted at all.
vi.mock('../components/CommandCenterGrid', () => ({
  CommandCenterGrid: () => <div data-probe="command-center-grid" />,
}));

import { OperatorConsole } from '../components/OperatorConsole';

/**
 * The folded console on the home page.
 *
 * A closed `<details>` still mounts its children, so the console's live socket
 * and four polling panels ran for every visitor who never opened it.
 */
describe('OperatorConsole', () => {
  it('mounts none of its panels while closed', () => {
    const html = renderToStaticMarkup(<OperatorConsole />);
    expect(html).toContain('Operator console');
    expect(html).not.toContain('command-center-grid');
  });

  it('starts closed', () => {
    const html = renderToStaticMarkup(<OperatorConsole />);
    expect(html).toMatch(/<details(?![^>]*\sopen)[^>]*>/);
  });
});
