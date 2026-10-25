import React from 'react';
import TradingSignals from '@/components/TradingSignals';
import { PageHeader } from '@/components/ui/PageHeader';

export default function SignalsPage() {
  return (
    <div className="h-full w-full overflow-y-auto p-2 space-y-4">
      <PageHeader
        title="Trade signals"
        description="Only patterns that have reliably moved prices across years of data. Each says which way, for how long, and how its expected move compares with the cost of trading it."
        related={[{ href: '/board', label: 'Signal board' }, { href: '/predictions', label: 'Stock calls' }, { href: '/strategies?view=drivers', label: 'What drives signals' }, { href: '/methodology', label: 'How it works' }]}
      />
      <TradingSignals />
    </div>
  );
}
