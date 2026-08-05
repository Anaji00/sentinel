import React from 'react';
import { CommandCenterGrid } from '@/components/CommandCenterGrid';

export default function DashboardOverviewPage() {
  return (
    <div className="flex h-full w-full flex-col">
      {/* Hydrated Client Grid with react-grid-layout */}
      <CommandCenterGrid />
    </div>
  );
}
