import React from 'react';
import SignalAttribution from '@/components/SignalAttribution';

export default function AttributionPage() {
  return (
    <div className="h-full w-full overflow-hidden p-2">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Signal attribution</h1>
      <SignalAttribution />
    </div>
  );
}
