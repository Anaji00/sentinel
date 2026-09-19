import React from 'react';
import { CommandCenterGrid } from '@/components/CommandCenterGrid';

export default function DashboardOverviewPage() {
  return (
    <div className="flex h-full w-full flex-col">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Command centre</h1>
      <CommandCenterGrid />
    </div>
  );
}
