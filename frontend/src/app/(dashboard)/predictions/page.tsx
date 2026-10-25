import React from 'react';
import StockPredictions from '@/components/StockPredictions';
import { PageHeader } from '@/components/ui/PageHeader';

export default function PredictionsPage() {
  return (
    <div className="h-full w-full overflow-y-auto p-2 space-y-4">
      <PageHeader
        title="Stock calls"
        description="Today's buys and sells for the next day, week and month, with how far each should move, how sure the model is and the options trade worth making. Below: every call the AI analysts and the model have made, and how often each was right."
        related={[
          { href: '/board', label: 'Signal board' },
          { href: '/book', label: 'Paper portfolio' },
          { href: '/agents', label: 'AI analysts' },
        ]}
      />
      <StockPredictions />
    </div>
  );
}
