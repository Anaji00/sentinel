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
  TrendingUp,
  Bitcoin,
  Map,
  LineChart,
  Gauge,
  Wallet,
  Search,
  ScrollText,
  FolderOpen,
  Crosshair,
  Bot,
  FileText,
  BookOpen,
  PanelLeftClose,
  PanelLeftOpen,
  Zap,
  Grid3x3,
  Eye,
  Radio,
  Scale,
  Banknote,
  Lock,
  Wrench,
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
  /** The civic group's heading carries the civic hue, so it reads as its own half. */
  civic?: boolean;
}

const NAV_GROUPS: NavGroup[] = [
  {
    label: 'Overview',
    items: [
      { href: '/', label: 'Home', icon: LayoutDashboard },
      { href: '/map', label: 'World Map', icon: Map },
    ],
  },
  {
    // Watching the watchers: who surveils the public, and how to push back.
    // Second, under Overview: half of what this platform is for.
    label: "Who's Watching",
    civic: true,
    // Each entry opens one part of the same page (#feed, #accountability...),
    // so the civic work reads as one place with five doors, not five pages.
    items: [
      { href: '/watch', label: 'Surveillance Map', icon: Eye },
      { href: '/watch#feed', label: 'What\'s Changing', icon: Radio },
      { href: '/watch#accountability', label: 'Accountability', icon: Scale },
      { href: '/watch#money', label: 'Follow the Money', icon: Banknote },
      { href: '/watch#protect', label: 'Protect Yourself', icon: Lock },
    ],
  },
  {
    // What to do today: the calls, the evidence behind them, one stock.
    label: 'Stocks',
    items: [
      // Every agent call on a stock, open and resolved, with its record.
      { href: '/predictions', label: 'Stock Calls', icon: Crosshair },
      // Every anomalous ticker with every signal, counted by evidence tier.
      { href: '/board', label: 'Signal Board', icon: Grid3x3 },
      // Signals only from families the reaction study has proven.
      { href: '/signals', label: 'Trade Signals', icon: Zap },
      // Stored bars, 13F consensus and the covered-call gate: three
      // unreachable routes answering one question about one symbol.
      { href: '/ticker', label: 'Look Up a Stock', icon: Search },
    ],
  },
  {
    // How the calls have done: the portfolio, every source's record, the tests.
    label: 'Track Record',
    items: [
      // Positions, cash and one-day risk, and every source graded on prices.
      { href: '/book', label: 'Portfolio & Record', icon: Wallet },
      // Backtests and what drives signals: one page, two views.
      { href: '/strategies', label: 'Research', icon: Gauge },
    ],
  },
  {
    label: 'Markets',
    items: [
      // Charts and the economy: one page, two views.
      { href: '/charts', label: 'Markets & Economy', icon: LineChart },
      // Options activity and big-money flow: one page, three views.
      { href: '/options', label: 'Options & Flow', icon: TrendingUp },
      { href: '/crypto', label: 'Crypto', icon: Bitcoin },
      { href: '/filings', label: 'Filings & Fund Holdings', icon: FileText },
    ],
  },
  {
    label: 'Intelligence',
    items: [
      // News & alerts and open-source intel: one page, two views.
      { href: '/intelligence', label: 'News & Intel', icon: ShieldAlert },
      { href: '/agents', label: 'AI Analysts', icon: Bot },
      // Cases and briefs: one page, two views.
      { href: '/cases', label: 'Investigations', icon: FolderOpen },
    ],
  },
  {
    label: 'Reference',
    items: [
      { href: '/methodology', label: 'How It Works', icon: BookOpen },
      // The three surfaces that need a person: the merge queue, the rule
      // feedback loop and the dead-letter backlog. All were served and
      // none was reachable.
      { href: '/operations', label: 'Operations', icon: Wrench },
      // The modal claimed a hash-chained ledger; nothing could open it.
      { href: '/audit', label: 'Audit Trail', icon: ScrollText },
    ],
  },
];

const STORAGE_KEY = 'sentinel.nav.expanded';

const HASHED = new Set(NAV_GROUPS.flatMap((g) => g.items.map((i) => i.href)).filter((h) => h.includes('#')));

/** Entries that open a part of a page (`/watch#money`) light only on that part;
 * the page's own entry lights only when none of its listed parts is open.
 * Null: an ordinary route, decided by isActive. */
function hashActive(pathname: string, hash: string, href: string): boolean | null {
  const [base, frag] = href.split('#');
  if (frag !== undefined) return pathname === base && hash === `#${frag}`;
  if (Array.from(HASHED).some((h) => h.startsWith(`${href}#`))) return pathname === href && !HASHED.has(`${href}${hash}`);
  return null;
}

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
  const [hash, setHash] = React.useState('');

  // Next's router changes the URL with pushState, which fires no hashchange;
  // re-read on every path change as well as on the event.
  React.useEffect(() => {
    const read = () => setHash(window.location.hash);
    read();
    window.addEventListener('hashchange', read);
    return () => window.removeEventListener('hashchange', read);
  }, [pathname]);

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
        className={`${expanded ? 'w-60' : 'w-16'} shrink-0 bg-inset border-r border-line
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
                <div
                  className={`px-4 pb-1.5 text-micro font-medium uppercase tracking-[0.12em] ${group.civic ? 'text-civic' : 'text-ink-mute'}`}
                >
                  {group.label}
                </div>
              )}
              {!expanded && <div className="mx-3 mb-2 border-t border-line/80" />}

              <ul className="space-y-0.5 px-2">
                {group.items.map((item) => {
                  const active = hashActive(pathname, hash, item.href) ?? isActive(pathname, item.href);
                  const [base, frag] = item.href.split('#');
                  const Icon = item.icon;
                  return (
                    <li key={item.href}>
                      <Link
                        href={item.href}
                        onClick={(e) => {
                          closeDrawer();
                          // Already on the page: change the hash in place (the
                          // page listens for it) rather than pushing a route.
                          if (pathname === base) {
                            e.preventDefault();
                            const next = frag ? `#${frag}` : '';
                            history.replaceState(null, '', `${base}${next}`);
                            setHash(next);
                            window.dispatchEvent(new HashChangeEvent('hashchange'));
                          }
                        }}
                        aria-current={active ? 'page' : undefined}
                        title={expanded ? undefined : item.label}
                        className={`group relative flex items-center gap-3 rounded-md px-3 py-2
                                  text-sm transition-colors outline-none
                                  focus-visible:ring-1 focus-visible:ring-accent/60 ${
                                    active
                                      ? 'bg-accent-dim text-ink'
                                      : 'text-ink-dim hover:bg-overlay/50 hover:text-ink'
                                  }`}
                      >
                        {active && (
                          <span className="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-accent" />
                        )}
                        <Icon
                          className={`h-4 w-4 shrink-0 ${active ? 'text-accent' : 'text-ink-mute group-hover:text-ink-dim'}`}
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
                   outline-none focus-visible:ring-1 focus-visible:ring-accent/60"
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
