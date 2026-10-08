import React from 'react';
import StockPredictions from '@/components/StockPredictions';

export default function PredictionsPage() {
  return (
    <div className="h-full w-full overflow-hidden p-2">
      <h1 className="sr-only">Stock predictions</h1>
      <StockPredictions />
    </div>
  );
}
