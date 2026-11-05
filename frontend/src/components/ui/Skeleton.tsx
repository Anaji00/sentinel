'use client';

import React from 'react';
import { Button } from './Button';
import { useSWRConfig } from 'swr';
import { spinnerState } from '../../lib/dataHealth';
import { useFailingSources } from '../../lib/useDataHealth';

interface SkeletonProps {
  className?: string;
  height?: string;
  width?: string;
}

export const Skeleton: React.FC<SkeletonProps> = ({ className = '', height, width }) => {
  return (
    <div
      style={{ height, width }}
      className={`animate-shimmer rounded bg-overlay/40 border border-line-strong/20 ${className}`}
    />
  );
};

/**
 * After this long, a skeleton stops claiming to be loading.
 *
 * Measured with the backend unreachable: every panel in the app shimmered
 * indefinitely. `/intelligence` rendered 15KB of markup and seventeen
 * characters of text — a page of grey boxes with no way to tell a slow load
 * from a dead gateway. A spinner that never resolves is not a loading state,
 * it is a lie with an animation on it.
 */
const PATIENCE_MS = 12_000;

/**
 * The placeholder shown while a panel's data or code is in flight.
 *
 * `title` was accepted and discarded. Every call site passes a real one —
 * "Loading Radar...", "Loading Movers...", "Stream Loading..." — and the
 * component rendered four anonymous grey rectangles instead, so a user looking
 * at a loading dashboard could not tell which panel was which.
 */
export const PanelSkeleton: React.FC<{ title?: string }> = ({ title = 'Loading…' }) => {
  const [patienceSpent, setPatienceSpent] = React.useState(false);

  React.useEffect(() => {
    const t = setTimeout(() => setPatienceSpent(true), PATIENCE_MS);
    return () => clearTimeout(t);
  }, []);

  if (patienceSpent) {
    return (
      <div className="w-full h-full p-4 glass-panel rounded-xl flex flex-col items-center justify-center gap-1.5 text-center">
        <p className="text-micro font-medium text-caution/90" role="alert">
          {title.replace(/\.\.\.$|…$/, '')} is not responding
        </p>
        <p className="max-w-xs text-micro leading-relaxed text-ink-mute">
          No data after {PATIENCE_MS / 1000} seconds. The panel is served by the API gateway — this
          usually means it is unreachable rather than slow.
        </p>
      </div>
    );
  }

  return (
    <div
      className="w-full h-full p-4 glass-panel rounded-xl flex flex-col gap-4"
      role="status"
      aria-live="polite"
      aria-label={title}
    >
      <div className="flex items-center justify-between border-b border-line pb-3">
        {/* The title the callers were already passing. */}
        <span className="text-micro font-medium text-ink-mute animate-pulse">{title}</span>
        <Skeleton height="20px" width="60px" />
      </div>
      <div className="flex-1 flex flex-col gap-3 animate-pulse">
        <Skeleton height="60px" width="100%" />
        <Skeleton height="60px" width="100%" />
        <Skeleton height="60px" width="100%" />
        <Skeleton height="60px" width="100%" />
      </div>
    </div>
  );
};

/**
 * A few shimmering lines for a section still loading, in place of the bare
 * "Loading…" text that read the same whether a request was slow or dead. After
 * the same patience as a panel it says so, quietly, instead of shimmering on.
 */
export const LoadingLines: React.FC<{ lines?: number; label?: string }> = ({ lines = 3, label = 'Loading' }) => {
  const [patienceSpent, setPatienceSpent] = React.useState(false);
  const failing = useFailingSources();
  const { mutate } = useSWRConfig();
  React.useEffect(() => {
    const t = setTimeout(() => setPatienceSpent(true), PATIENCE_MS);
    return () => clearTimeout(t);
  }, []);
  const state = spinnerState(failing, patienceSpent);
  if (state !== 'loading') {
    return (
      <p className="text-micro text-caution" role="status">
        {state === 'failed' ? 'Could not load this yet.' : 'This is taking longer than usual.'}{' '}
        <Button variant="link" type="button" onClick={() => void mutate(() => true)} className="underline hover:text-ink">
          Try again
        </Button>
      </p>
    );
  }
  return (
    <div className="space-y-2 animate-pulse" role="status" aria-label={label}>
      {Array.from({ length: lines }, (_, i) => (
        <Skeleton key={i} height="12px" width={i === lines - 1 ? '60%' : '100%'} />
      ))}
    </div>
  );
};
