import React from 'react';
import TickerInspector from '@/components/TickerInspector';

export default function TickerPage() {
  return (
    <div className="h-full w-full overflow-hidden p-2">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Ticker inspector</h1>
      <TickerInspector />
    </div>
  );
}
