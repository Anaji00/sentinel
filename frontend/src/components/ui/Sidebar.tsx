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
  LayoutDashboard, ShieldAlert, ShieldCheck, Globe2, TrendingUp, Layers,
  Bitcoin, Map, LineChart, Bot, FileText, BookOpen,
  PanelLeftClose, PanelLeftOpen,
} from 'lucide-react';

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
    ],
  },
  {
    label: 'Intelligence',
    items: [
      { href: '/intelligence', label: 'Intelligence Feed', icon: ShieldAlert },
      { href: '/osint', label: 'OSINT Matrix', icon: ShieldCheck },
      { href: '/agents', label: 'Agent Swarm', icon: Bot },
    ],
  },
  {
    label: 'Reference',
    items: [{ href: '/methodology', label: 'Methodology', icon: BookOpen }],
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
    <aside
      className={`${expanded ? 'w-60' : 'w-16'} shrink-0 bg-[#0b0d12] border-r border-slate-800
                  flex flex-col h-full z-40 select-none transition-[width] duration-200`}
      aria-label="Primary"
      data-ready={ready}
    >
      <nav className="flex-1 overflow-y-auto py-3">
        {NAV_GROUPS.map((group) => (
          <div key={group.label} className="mb-4 last:mb-0">
            {expanded && (
              <div className="px-4 pb-1.5 text-[10px] font-medium uppercase tracking-[0.12em] text-slate-600">
                {group.label}
              </div>
            )}
            {!expanded && <div className="mx-3 mb-2 border-t border-slate-800/80" />}

            <ul className="space-y-0.5 px-2">
              {group.items.map((item) => {
                const active = isActive(pathname, item.href);
                const Icon = item.icon;
                return (
                  <li key={item.href}>
                    <Link
                      href={item.href}
                      aria-current={active ? 'page' : undefined}
                      title={expanded ? undefined : item.label}
                      className={`group relative flex items-center gap-3 rounded-md px-3 py-2
                                  text-[13px] transition-colors outline-none
                                  focus-visible:ring-1 focus-visible:ring-cyan-400/60 ${
                        active
                          ? 'bg-cyan-500/10 text-cyan-200'
                          : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-100'
                      }`}
                    >
                      {active && (
                        <span className="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-cyan-400" />
                      )}
                      <Icon
                        className={`h-4 w-4 shrink-0 ${active ? 'text-cyan-300' : 'text-slate-500 group-hover:text-slate-300'}`}
                      />
                      {expanded && <span className="truncate">{item.label}</span>}
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
        className="flex items-center gap-3 border-t border-slate-800 px-4 py-2.5 text-[12px]
                   text-slate-500 hover:text-slate-200 hover:bg-slate-800/40 transition-colors
                   outline-none focus-visible:ring-1 focus-visible:ring-cyan-400/60"
      >
        {expanded ? <PanelLeftClose className="h-4 w-4" /> : <PanelLeftOpen className="h-4 w-4" />}
        {expanded && <span>Collapse</span>}
      </button>
    </aside>
  );
}
