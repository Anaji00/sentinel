import React from 'react';
import DarkPoolFlowPanel from '@/components/DarkPoolFlowPanel';

export default function DarkPoolFlowPage() {
  return (
    <div className="h-full w-full p-2 overflow-hidden">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Dark pool & sweeps</h1>
      <DarkPoolFlowPanel />
    </div>
  );
}
