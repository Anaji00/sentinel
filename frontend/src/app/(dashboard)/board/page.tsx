import React from 'react';
import SignalBoard from '@/components/SignalBoard';
import StockNews from '@/components/StockNews';
import { FreshnessStrip } from '@/components/TrackRecord';
import { PageHeader } from '@/components/ui/PageHeader';

export default function SignalBoardPage() {
  return (
    <div className="h-full w-full overflow-y-auto p-2 space-y-4">
      <PageHeader
        title="Signal board"
        description="Every stock with unusual activity in the last three days, the strongest leans each way, and everything the platform knows about each one."
        related={[
          { href: '/signals', label: 'Trade signals' },
          { href: '/predictions', label: 'Stock calls' },
          { href: '/book', label: 'Portfolio & track record' },
          { href: '/ticker', label: 'Look up a stock' },
        ]}
      />
      <FreshnessStrip />
      <SignalBoard />
      {/* The model portfolio and every source's track record moved to /book:
          this page answers "what is moving and why", that one "how has it done". */}
      <StockNews />
    </div>
  );
}
