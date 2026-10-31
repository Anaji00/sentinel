import React from 'react';
import { BondYieldsChartIsland, MarketChartsIsland, PredictionMarketPanelIsland } from '../charts/islands';
import { CryptoAnalyticsIsland, CryptoPriceChartIsland } from '../crypto/islands';
import { EquitiesFlowIsland, QuantRadarIsland } from '../options/islands';
import DarkPoolFlowPanel from '@/components/DarkPoolFlowPanel';
import { LiveTape } from '@/components/LiveTape';
import { OilStocks } from '@/components/OilStocks';
import { PageHeader } from '@/components/ui/PageHeader';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Markets: what were three pages (Markets & Economy, Crypto, Options & Flow),
 * which all read the same price series and the same live stream. One page,
 * one tape across the top, six views. The old addresses redirect here.
 */
const VIEWS: View[] = [
  { key: 'prices', label: 'Prices', hint: 'Stocks, oil, gold, crypto and rates, refreshed every few seconds' },
  { key: 'economy', label: 'Economy', hint: 'Interest rates, oil, and what prediction markets expect' },
  { key: 'crypto', label: 'Crypto', hint: "Bitcoin's price, futures funding, open interest and large transfers" },
  { key: 'flow', label: 'Big-money trades', hint: 'Block trades of $200k or more and multi-exchange options sweeps' },
  { key: 'radar', label: 'Unusual volume', hint: 'Over 11,000 stocks swept for unusual volume and options orders' },
  { key: 'indices', label: 'Index flow', hint: 'How the S&P 500 and Nasdaq 100 are moving today' },
];

const DESCRIPTION: Record<string, string> = {
  prices:
    'Stocks, oil, gold, crypto and interest rates side by side, refreshed every few seconds. Click any chart to open it full size.',
  economy: 'Interest rates, oil stockpiles, and what betting markets expect from the Fed, elections and world events.',
  crypto:
    "Bitcoin's price as it trades, plus what the futures market is paying to stay long or short, open interest, order flow and large wallet transfers.",
  flow: 'Unusually large trades as they print: block trades and options sweeps across exchanges.',
  radar: 'A radar sweeping over 11,000 stocks for unusual volume and options orders.',
  indices: 'How the S&P 500 and Nasdaq 100 are moving today.',
};

export default async function MarketsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="flex h-full w-full flex-col gap-4 overflow-y-auto p-2 text-ink">
      <LiveTape />
      <PageHeader
        title="Markets"
        description={DESCRIPTION[view.key]}
        related={[
          { href: '/calls', label: 'Calls' },
          { href: '/money', label: 'Follow the money' },
        ]}
      >
        <ViewTabs base="/markets" views={VIEWS} active={view.key} />
      </PageHeader>
      {view.key === 'prices' && <MarketChartsIsland />}
      {view.key === 'economy' && (
        <>
          <BondYieldsChartIsland />
          <OilStocks />
          <div className="min-h-[450px]">
            <PredictionMarketPanelIsland />
          </div>
        </>
      )}
      {view.key === 'crypto' && (
        <>
          <CryptoPriceChartIsland />
          <div className="flex min-h-[450px] flex-col overflow-hidden rounded-2xl border border-line bg-raised">
            <CryptoAnalyticsIsland />
          </div>
        </>
      )}
      {view.key === 'flow' && (
        <div className="h-[75vh] min-h-[520px]">
          <DarkPoolFlowPanel />
        </div>
      )}
      {view.key === 'radar' && (
        <div className="flex min-h-[450px] flex-col overflow-hidden rounded-2xl border border-line bg-raised">
          <QuantRadarIsland />
        </div>
      )}
      {view.key === 'indices' && <EquitiesFlowIsland />}
    </div>
  );
}
