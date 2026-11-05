'use client';

import React from 'react';
import { GRACE_MS, failingFor, serverSnapshot, snapshot, subscribe } from './dataHealth';

/** How many data sources have been failing for a few seconds. Zero when all is well. */
export function useFailingSources(graceMs: number = GRACE_MS): number {
  const all = React.useSyncExternalStore(subscribe, snapshot, serverSnapshot);
  const [now, setNow] = React.useState(() => Date.now());
  const any = Object.keys(all).length > 0;

  React.useEffect(() => {
    if (!any) return;
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), 2_000);
    return () => clearInterval(timer);
  }, [any]);

  return failingFor(all, now, graceMs);
}
