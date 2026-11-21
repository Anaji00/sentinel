import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

/**
 * Operations and Audit trail are deployment tooling: offered to admins only,
 * the same rule as the account panel's Data sources tab. Checked here because
 * the browser checks were all made signed in as an admin.
 */

let role: string | null = 'VIEWER';

vi.mock('next/navigation', () => ({ usePathname: () => '/calls' }));
vi.mock('../components/ui/SessionContext', () => ({ useSession: () => ({ role }) }));
vi.mock('../components/ui/NavContext', () => ({ useNav: () => ({ drawerOpen: false, closeDrawer: () => {} }) }));

import { Sidebar } from '../components/ui/Sidebar';

const links = (html: string) => [...html.matchAll(/href="([^"]+)"/g)].map((m) => m[1]);

describe('sidebar by role', () => {
  // Scan was added on purpose (the screener, sector map and market breadth live there): twelve links for a viewer, fourteen for an admin.
  it('a viewer sees twelve links and no operator tooling', () => {
    role = 'VIEWER';
    const hrefs = links(renderToStaticMarkup(<Sidebar />));
    expect(hrefs).toEqual([
      '/', '/calls', '/markets', '/scan', '/performance', '/money',
      '/intel', '/osint', '/connections', '/filings', '/watch', '/agents', '/methodology',
    ]);
  });

  it('an admin also sees Operations and Audit trail', () => {
    role = 'ADMIN';
    const hrefs = links(renderToStaticMarkup(<Sidebar />));
    expect(hrefs).toContain('/operations');
    expect(hrefs).toContain('/audit');
    expect(hrefs).toHaveLength(15);
  });

  it('no role is not admin', () => {
    role = null;
    expect(links(renderToStaticMarkup(<Sidebar />))).not.toContain('/operations');
  });
});
