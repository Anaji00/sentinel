'use client';

/**
 * "Updated 2 min ago" for a panel, read from when its data source last answered (lib/freshness.ts). It ticks
 * on its own, turns cautionary when the panel has fallen far behind its polling, and shows nothing until the first
 * answer arrives. It states a fact about the data, not about the clock on the page.
 */

import React from 'react';
import { agoWords, isOverdue, noUpdate, subscribeUpdates, updatedAt } from '../../lib/freshness';

export function Updated({ source, pollMs = 300_000, className = '' }: { source: string; pollMs?: number; className?: string }) {
  const at = React.useSyncExternalStore(
    subscribeUpdates,
    () => updatedAt(source),
    noUpdate,
  );
  const [now, setNow] = React.useState(() => Date.now());
  React.useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 15_000);
    return () => clearInterval(t);
  }, []);
  if (at == null) return null;
  const age = Math.max(0, now - at);
  const late = isOverdue(age, pollMs);
  return (
    <span className={`text-micro ${late ? 'text-caution' : 'text-ink-mute'} ${className}`}>
      {late ? 'Last updated ' : 'Updated '}
      {agoWords(age)}
    </span>
  );
}

export default Updated;
