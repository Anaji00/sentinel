import React from 'react';
import StrategyPerformance from '@/components/StrategyPerformance';

export default function StrategiesPage() {
  return (
    <div className="h-full w-full overflow-hidden p-2">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Strategy results</h1>
      <StrategyPerformance />
    </div>
  );
}
