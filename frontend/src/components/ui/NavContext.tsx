'use client';

/**
 * Whether the navigation drawer is open, shared by the header and the sidebar.
 *
 * The sidebar was `w-60 shrink-0` at every width, with an expand/collapse
 * preference and no responsive behaviour at all. Measured at a 279px viewport:
 * the sidebar took 240px and **the entire application was left 39 pixels**, with
 * no horizontal scroll to reach it -- the content was not cut off, it was
 * crushed. Collapsed it is still 64px, permanently, on a screen that small.
 *
 * Below `md` the sidebar now leaves the flow and becomes an overlay, so content
 * gets the full width and the navigation is reached from a control in the
 * header. Above `md` nothing changes: the stored expand/collapse preference is
 * the only thing driving width, exactly as before.
 *
 * The state lives here rather than in the layout because the layout is a server
 * component and the two consumers are siblings.
 */

import React from 'react';

interface NavState {
  /** The drawer is showing. Only meaningful below the `md` breakpoint. */
  drawerOpen: boolean;
  openDrawer: () => void;
  closeDrawer: () => void;
  toggleDrawer: () => void;
}

const NavContext = React.createContext<NavState | null>(null);

/** Matches Tailwind's `md`. One definition, used for the query and the guard. */
export const NAV_BREAKPOINT_PX = 768;

export function NavProvider({ children }: { children: React.ReactNode }) {
  const [drawerOpen, setDrawerOpen] = React.useState(false);

  const closeDrawer = React.useCallback(() => setDrawerOpen(false), []);
  const openDrawer = React.useCallback(() => setDrawerOpen(true), []);
  const toggleDrawer = React.useCallback(() => setDrawerOpen((o) => !o), []);

  // Escape closes it, because a full-screen overlay with no keyboard exit is a
  // trap for anyone not using a pointer.
  React.useEffect(() => {
    if (!drawerOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeDrawer();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [drawerOpen, closeDrawer]);

  // Widening past the breakpoint puts the sidebar back in the flow, so a drawer
  // left open would render as a duplicate overlay on top of it.
  React.useEffect(() => {
    if (typeof window === 'undefined' || !window.matchMedia) return;
    const mq = window.matchMedia(`(min-width: ${NAV_BREAKPOINT_PX}px)`);
    const sync = () => {
      if (mq.matches) setDrawerOpen(false);
    };
    sync();
    mq.addEventListener('change', sync);
    return () => mq.removeEventListener('change', sync);
  }, []);

  const value = React.useMemo(
    () => ({ drawerOpen, openDrawer, closeDrawer, toggleDrawer }),
    [drawerOpen, openDrawer, closeDrawer, toggleDrawer],
  );

  return <NavContext.Provider value={value}>{children}</NavContext.Provider>;
}

/**
 * The drawer state, or a no-op.
 *
 * Returning a working default rather than throwing: these components are also
 * mounted in tests and in isolation, and a navigation control is not worth
 * crashing a page over.
 */
export function useNav(): NavState {
  const ctx = React.useContext(NavContext);
  return (
    ctx ?? {
      drawerOpen: false,
      openDrawer: () => {},
      closeDrawer: () => {},
      toggleDrawer: () => {},
    }
  );
}
