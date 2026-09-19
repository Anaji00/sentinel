'use client';

/**
 * One place where fetching behaves, instead of thirty-nine.
 *
 * Measured before this: 39 `refreshInterval` values written by hand across the
 * components, six of them at three seconds, and `60000` spelled two different
 * ways. No `SWRConfig` anywhere, so there was no shared policy for retries,
 * deduplication, or what to do when a request comes back 401.
 *
 * That last one is the reason this file exists. The session cookie lasts 24
 * hours. When it expires, every panel independently discovers it, and each one
 * prints "Session expired" into its own empty state. There is no
 * `router.push('/login')` anywhere in the application -- I grepped for it --
 * so the operator is left looking at a dashboard of identical failures with no
 * offered way back. Thirty-nine dead ends rather than one recoverable event.
 *
 * Session loss is a global fact about the application, so it is handled once,
 * here, by the thing that finds out about it first.
 */

import React from 'react';
import { SWRConfig } from 'swr';
import { usePathname, useRouter } from 'next/navigation';
import { ApiError } from '../../lib/api';

/**
 * How often anything polls, by how fast the thing underneath actually moves.
 *
 * Named steps rather than numbers at call sites, so "this is a live feed" and
 * "this is configuration" are decisions that can be read, and so the six
 * panels that chose three seconds did so on purpose rather than by copying the
 * panel next to them.
 */
export const POLL = {
  /** Prices and the event stream: the things a person is watching change. */
  live: 5_000,
  /** Panels that summarise something that moves, but not tick by tick. */
  standard: 15_000,
  /** Health, inventories, counts. */
  slow: 60_000,
  /** Configuration, catalogues, anything that changes when someone deploys. */
  rare: 300_000,
} as const;

export function DataProvider({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();

  // One redirect per session loss, not one per panel. Thirty-nine hooks can
  // discover the same expired cookie within a few milliseconds of each other,
  // and thirty-nine `router.push` calls to the same place is a navigation
  // storm rather than a redirect.
  const redirecting = React.useRef(false);

  const onError = React.useCallback(
    (error: unknown) => {
      if (!(error instanceof ApiError) || !error.isUnauthenticated) return;
      if (redirecting.current) return;
      // Already on an auth screen: the login form's own 401 is how it reports
      // a wrong password, and bouncing that back to itself would clear the
      // message the operator needs to read.
      if (pathname?.startsWith('/login') || pathname?.startsWith('/signup')) return;

      redirecting.current = true;
      // Carry where they were, so signing back in returns them to the panel
      // they were reading rather than to the command centre.
      const next = encodeURIComponent(pathname || '/');
      router.push(`/login?next=${next}&reason=expired`);
    },
    [pathname, router],
  );

  return (
    <SWRConfig
      value={{
        onError,
        // A failed request retries, but not forever and not fast. The default
        // is five attempts on an exponential backoff; what matters here is the
        // exclusion below.
        errorRetryCount: 3,
        errorRetryInterval: 5_000,
        shouldRetryOnError: (error: unknown) => {
          if (error instanceof ApiError) {
            // 401 and 404 do not become true by asking again. Retrying an
            // expired session three times per panel is thirty-nine times three
            // requests that can only fail.
            if (error.isUnauthenticated || error.isMissing) return false;
          }
          return true;
        },
        // Two panels asking for the same endpoint within this window share one
        // request. Several do -- `/health/` is read by the status control and
        // by the state strip.
        dedupingInterval: 2_000,
        // Coming back to the tab should show current data; the poll alone can
        // leave a stale screen up for a full interval after a long absence.
        revalidateOnFocus: true,
        // Not on every reconnect blip: a flapping connection would otherwise
        // trigger a full refetch of every panel each time it flaps.
        revalidateOnReconnect: true,
        keepPreviousData: true,
      }}
    >
      {children}
    </SWRConfig>
  );
}
