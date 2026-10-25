import React from 'react';
import AutoTradePanel from '@/components/AutoTradePanel';
import ModelPortfolio from '@/components/ModelPortfolio';
import { ModelHealth } from '@/components/ModelHealth';
import PaperTradingBook from '@/components/PaperTradingBook';
import PortfolioLab from '@/components/PortfolioLab';
import { PaperLedger, ProducerGrades, TrackRecord } from '@/components/TrackRecord';
import { PageHeader } from '@/components/ui/PageHeader';

export default function TradingBookPage() {
  return (
    <div className="h-full w-full overflow-y-auto p-2 space-y-4">
      <PageHeader
        title="Portfolio & track record"
        description="The model's own portfolio, measured every trading day against the S&P 500, how every source of calls has scored against prices, and a paper account for trying trades yourself. No real money moves."
        related={[
          { href: '/board', label: 'Signal board' },
          { href: '/predictions', label: 'Stock calls' },
          { href: '/strategies', label: 'Backtests' },
        ]}
      />
      {/* The model's own portfolio first -- the one being measured against
          the S&P 500 -- then how every source has scored, then the book for
          trades placed by hand. */}
      <ModelPortfolio />
      <AutoTradePanel />
      <PortfolioLab />
      <ModelHealth />
      <TrackRecord />
      <PaperLedger />
      <ProducerGrades />
      <div className="h-[80vh] min-h-[560px]">
        <PaperTradingBook />
      </div>
    </div>
  );
}
