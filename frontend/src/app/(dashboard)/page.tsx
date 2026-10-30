import React from 'react';
import { OperatorConsole } from '@/components/OperatorConsole';
import { TodayOverview } from '@/components/TodayOverview';

export default function DashboardOverviewPage() {
  return (
    <div className="h-full w-full overflow-y-auto space-y-6 pb-6">
      {/* Today first: what a person opens the app for. The live console --
          every feed, panel by panel -- follows for anyone checking the plumbing. */}
      <TodayOverview />
      {/* Closed by default so the page leads with what a person came for, and
          mounts nothing until opened. */}
      <OperatorConsole />
    </div>
  );
}
