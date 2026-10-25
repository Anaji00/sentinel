import React from 'react';
import TickerInspector from '@/components/TickerInspector';
import TickerHero from '@/components/TickerHero';
import TickerSurveillance from '@/components/TickerSurveillance';

export default function TickerPage() {
  return (
    <div className="h-full w-full overflow-y-auto p-2 space-y-4">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Look up a stock</h1>
      {/* useSearchParams needs a Suspense boundary under the app router. */}
      <React.Suspense fallback={null}>
        <TickerHero />
        {/* For a listed surveillance company: what it sells to police and who pays it. */}
        <TickerSurveillance />
        <TickerInspector />
      </React.Suspense>
    </div>
  );
}
