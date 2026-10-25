import React from 'react';
import {
  BondYieldsChartIsland,
  MarketChartsIsland,
  PredictionMarketPanelIsland,
} from './islands';
import { OilStocks } from '@/components/OilStocks';
import { PageHeader } from '@/components/ui/PageHeader';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Markets: every price chart, and the economy behind them. Charts and The
 * Economy were two pages that both opened on the Treasury yield curve; they are
 * two views of one now (/macro redirects to ?view=economy).
 */
const VIEWS: View[] = [
  { key: 'prices', label: 'Prices', hint: 'Stocks, oil, gold, crypto and rates, refreshed every few seconds' },
  { key: 'economy', label: 'Economy', hint: 'Interest rates, oil, and what prediction markets expect' },
];

export default async function MarketsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="flex h-full w-full flex-col gap-4 overflow-y-auto p-2 text-ink">
      <PageHeader
        title="Markets"
        description={
          view.key === 'economy'
            ? 'Interest rates, oil stockpiles, and what betting markets expect from the Fed, elections and world events.'
            : 'Stocks, oil, gold, crypto and interest rates side by side, refreshed every few seconds. Click any chart to open it full size.'
        }
        related={[
          { href: '/ticker', label: 'Look up a stock' },
          { href: '/crypto', label: 'Crypto' },
          { href: '/predictions', label: 'Stock calls' },
        ]}
      >
        <ViewTabs base="/charts" views={VIEWS} active={view.key} />
      </PageHeader>
      {view.key === 'prices' ? (
        <MarketChartsIsland />
      ) : (
        <>
          <BondYieldsChartIsland />
          <OilStocks />
          {/* The AI advisor's parallel "portfolio" was here: its own weights,
              risk figures for one stock, and zero signals, contradicting the
              model portfolio and the stock calls. It stays in the operator
              console; readers get one answer, on Stock Calls. */}
          <div className="min-h-[450px]">
            <PredictionMarketPanelIsland />
          </div>
        </>
      )}
    </div>
  );
}
