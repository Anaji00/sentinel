import React from 'react';
import { CommandCenterGrid } from '@/components/CommandCenterGrid';
import { TodayOverview } from '@/components/TodayOverview';

export default function DashboardOverviewPage() {
  return (
    <div className="h-full w-full overflow-y-auto space-y-6 pb-6">
      {/* Today first: what a person opens the app for. The live console --
          every feed, panel by panel -- follows for anyone checking the plumbing. */}
      <TodayOverview />
      {/* The plumbing, folded: for anyone checking every feed is flowing.
          Closed by default so the page leads with what a person came for. */}
      <details className="group rounded-2xl border border-line bg-raised">
        <summary className="flex cursor-pointer items-baseline justify-between gap-2 px-4 py-3">
          <span id="console-heading" className="text-sm font-semibold text-ink">
            Operator console{' '}
            <span className="text-ink-mute font-normal">· every feed as it arrives</span>
          </span>
          <span className="text-micro text-accent group-open:hidden">Open</span>
          <span className="text-micro text-accent hidden group-open:inline">Close</span>
        </summary>
        <div className="h-[85vh] min-h-[640px] border-t border-line overflow-hidden">
          <CommandCenterGrid />
        </div>
      </details>
    </div>
  );
}
