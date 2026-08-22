import React from 'react';
import { PanelSkeleton } from '@/components/ui/Skeleton';

/**
 * Route-level loading state for every dashboard view.
 *
 * Next.js renders this automatically while a server component streams. It
 * reserves the same vertical space the loaded view occupies, so arrival does
 * not shift layout.
 */
export default function DashboardLoading() {
  return (
    <div className="flex h-full w-full flex-col gap-4" aria-busy="true" aria-live="polite">
      <div className="h-7 w-72 rounded bg-slate-800/60 animate-pulse" />
      <div className="grid grid-cols-12 gap-4 flex-1 min-h-0">
        <div className="col-span-12 lg:col-span-8 min-h-[280px]">
          <PanelSkeleton title="Loading intelligence surface…" />
        </div>
        <div className="col-span-12 lg:col-span-4 min-h-[280px]">
          <PanelSkeleton title="Loading telemetry…" />
        </div>
      </div>
      <span className="sr-only">Loading dashboard view</span>
    </div>
  );
}
