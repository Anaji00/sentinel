'use client';

/**
 * Primary navigation.
 *
 * Was a flat list of thirteen destinations in a rail that only revealed its
 * labels on hover (`w-16 hover:w-64`), so the navigation could not be read
 * without pointing at it -- unusable from a keyboard, and impossible to scan.
 * Thirteen peers also carry no hierarchy: "Options Flow" and "Methodology" sat
 * at the same level, so nothing told you where to start.
 *
 * Now grouped by the question being asked, with an explicit pinned/collapsed
 * toggle that persists, and active state matched by path prefix so nested
 * routes keep their parent highlighted.
 */

import React from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import {
  LayoutDashboard,
  ShieldAlert,
  ShieldCheck,
  Globe2,
  TrendingUp,
  Layers,
  Bitcoin,
  Map,
  LineChart,
  Gauge,
  Wallet,
  Search,
  ScrollText,
  FolderOpen,
  FileBarChart,
  Crosshair,
  Bot,
  FileText,
  BookOpen,
  PanelLeftClose,
  PanelLeftOpen,
} from 'lucide-react';

import { useNav } from './NavContext';

interface NavItem {
  href: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
}

interface NavGroup {
  label: string;
  items: NavItem[];
}

const NAV_GROUPS: NavGroup[] = [
  {
    label: 'Overview',
    items: [
      { href: '/', label: 'Command Center', icon: LayoutDashboard },
      { href: '/map', label: 'Global Map', icon: Map },
    ],
  },
  {
    label: 'Markets',
    items: [
      { href: '/charts', label: 'Market Charts', icon: LineChart },
      { href: '/options', label: 'Options Flow', icon: TrendingUp },
      { href: '/flow', label: 'Dark Pool & Sweeps', icon: Layers },
      { href: '/crypto', label: 'Crypto & Perps', icon: Bitcoin },
      { href: '/macro', label: 'Macro Matrix', icon: Globe2 },
      { href: '/filings', label: 'Filings & 13F', icon: FileText },
      // 228 stored backtests had no route to reach them from.
      { href: '/strategies', label: 'Strategy Results', icon: Gauge },
      // Positions, cash and one-day risk. All three endpoints have been
      // served since the project began and nothing ever called them.
      { href: '/book', label: 'Trading Book', icon: Wallet },
      // Stored bars, 13F consensus and the covered-call gate: three
      // unreachable routes answering one question about one symbol.
      { href: '/ticker', label: 'Ticker Inspector', icon: Search },
    ],
  },
  {
    label: 'Intelligence',
    items: [
      { href: '/intelligence', label: 'Intelligence Feed', icon: ShieldAlert },
      { href: '/osint', label: 'OSINT Matrix', icon: ShieldCheck },
      { href: '/agents', label: 'Agent Swarm', icon: Bot },
      // The three surfaces that need a person: the merge queue, the rule
      // feedback loop and the dead-letter backlog. All were served and
      // none was reachable.
      { href: '/operations', label: 'Operations', icon: Bot },
      // Five routes, an audit entry on creation, a status lifecycle and a
      // note thread -- and no screen anywhere until now.
      { href: '/cases', label: 'Cases', icon: FolderOpen },
      // A catalogue of three templates, and a generator. Neither had a
      // caller, so the catalogue described choices nobody could make.
      { href: '/reports', label: 'Briefs', icon: FileBarChart },
    ],
  },
  {
    label: 'Reference',
    items: [
      // Which of the platform's 57 hand-set weights are carrying it.
      { href: '/attribution', label: 'Signal Attribution', icon: Crosshair },
      { href: '/methodology', label: 'Methodology', icon: BookOpen },
      // The modal claimed a hash-chained ledger; nothing could open it.
      { href: '/audit', label: 'Audit Trail', icon: ScrollText },
    ],
  },
];

const STORAGE_KEY = 'sentinel.nav.expanded';

function isActive(pathname: string, href: string): boolean {
  // Prefix match, so a nested route keeps its parent lit.
  // Root is exact, otherwise it would match every route.
  return href === '/' ? pathname === '/' : pathname === href || pathname.startsWith(`${href}/`);
}

export function Sidebar() {
  const pathname = usePathname();
  const [expanded, setExpanded] = React.useState(true);
  const [ready, setReady] = React.useState(false);
  const { drawerOpen, closeDrawer } = useNav();

  // Read the stored preference after mount: localStorage is not available
  // during server render, and reading it inline would desynchronise hydration.
  React.useEffect(() => {
    try {
      const stored = window.localStorage.getItem(STORAGE_KEY);
      if (stored !== null) setExpanded(stored === '1');
    } catch {
      /* private mode; the default stands */
    }
    setReady(true);
  }, []);

  const toggle = React.useCallback(() => {
    setExpanded((prev) => {
      const next = !prev;
      try {
        window.localStorage.setItem(STORAGE_KEY, next ? '1' : '0');
      } catch {
        /* nothing to persist to; the session still works */
      }
      return next;
    });
  }, []);

  return (
    <>
      {/* Below `md` the drawer sits over the content, so a tap outside is the
          way out. Above it there is no overlay and this renders nothing. */}
      <div
        onClick={closeDrawer}
        aria-hidden="true"
        className={`fixed inset-0 z-30 bg-black/60 md:hidden transition-opacity duration-200 ${
          drawerOpen ? 'opacity-100' : 'pointer-events-none opacity-0'
        }`}
      />

      <aside
        className={`${expanded ? 'w-60' : 'w-16'} shrink-0 bg-[#0b0d12] border-r border-line
                  flex flex-col h-full z-40 select-none
                  transition-[width,transform] duration-200
                  max-md:fixed max-md:inset-y-0 max-md:left-0 max-md:w-60
                  max-md:pt-14
                  ${drawerOpen ? 'max-md:translate-x-0' : 'max-md:-translate-x-full'}`}
        aria-label="Primary"
        aria-hidden={!drawerOpen ? undefined : undefined}
        data-ready={ready}
        data-drawer-open={drawerOpen}
      >
        <nav className="flex-1 overflow-y-auto py-3">
          {NAV_GROUPS.map((group) => (
            <div key={group.label} className="mb-4 last:mb-0">
              {expanded && (
                <div className="px-4 pb-1.5 text-micro font-medium uppercase tracking-[0.12em] text-ink-mute">
                  {group.label}
                </div>
              )}
              {!expanded && <div className="mx-3 mb-2 border-t border-line/80" />}

              <ul className="space-y-0.5 px-2">
                {group.items.map((item) => {
                  const active = isActive(pathname, item.href);
                  const Icon = item.icon;
                  return (
                    <li key={item.href}>
                      <Link
                        href={item.href}
                        onClick={closeDrawer}
                        aria-current={active ? 'page' : undefined}
                        title={expanded ? undefined : item.label}
                        className={`group relative flex items-center gap-3 rounded-md px-3 py-2
                                  text-sm transition-colors outline-none
                                  focus-visible:ring-1 focus-visible:ring-cyan-400/60 ${
                                    active
                                      ? 'bg-cyan-500/10 text-cyan-200'
                                      : 'text-ink-dim hover:bg-overlay/50 hover:text-ink'
                                  }`}
                      >
                        {active && (
                          <span className="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-cyan-400" />
                        )}
                        <Icon
                          className={`h-4 w-4 shrink-0 ${active ? 'text-cyan-300' : 'text-ink-mute group-hover:text-ink-dim'}`}
                        />
                        {/* In the drawer the label always shows: a 240px overlay
                          has the room, and an icon-only drawer would be the
                          worst of both. */}
                        <span className={expanded ? 'truncate' : 'truncate max-md:inline hidden'}>
                          {item.label}
                        </span>
                      </Link>
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </nav>

        <button
          type="button"
          onClick={toggle}
          aria-expanded={expanded}
          aria-label={expanded ? 'Collapse navigation' : 'Expand navigation'}
          className="flex items-center gap-3 border-t border-line px-4 py-2.5 text-xs
                   text-ink-mute hover:text-ink hover:bg-overlay/40 transition-colors
                   outline-none focus-visible:ring-1 focus-visible:ring-cyan-400/60"
        >
          {expanded ? (
            <PanelLeftClose className="h-4 w-4" />
          ) : (
            <PanelLeftOpen className="h-4 w-4" />
          )}
          <span className={expanded ? '' : 'max-md:inline hidden'}>Collapse</span>
        </button>
      </aside>
    </>
  );
}
