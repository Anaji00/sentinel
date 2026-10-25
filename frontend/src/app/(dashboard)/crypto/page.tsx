import React from 'react';
import { CryptoAnalyticsIsland, CryptoPriceChartIsland } from './islands';
import { PageHeader } from '@/components/ui/PageHeader';

export default function CryptoPage() {
  return (
    <div className="flex h-full w-full flex-col p-2 gap-4 overflow-y-auto">
      <PageHeader
        title="Crypto"
        description="Bitcoin's price as it trades, plus what the futures market is paying to stay long or short, open interest, order flow and large wallet transfers."
        related={[
          { href: '/charts', label: 'Charts' },
          { href: '/charts?view=economy', label: 'The economy' },
        ]}
      />
      <CryptoPriceChartIsland />
      <div className="flex flex-col rounded-2xl border border-line bg-raised overflow-hidden min-h-[450px]">
        <CryptoAnalyticsIsland />
      </div>
    </div>
  );
}
