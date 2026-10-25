import React from 'react';
import DarkPoolFlowPanel from '@/components/DarkPoolFlowPanel';
import { EquitiesFlowIsland, QuantRadarIsland } from './islands';
import { PageHeader } from '@/components/ui/PageHeader';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Options activity and big-money flow, one page: both answer "where is large,
 * unusual money going?", and sweeps appeared on each. It opens on the prints,
 * which always have data; the radar can rightly be empty for hours.
 */
const VIEWS: View[] = [
  { key: 'flow', label: 'Big-money trades', hint: 'Block trades of $200k or more and multi-exchange options sweeps' },
  { key: 'radar', label: 'Unusual volume radar', hint: 'Over 11,000 stocks swept for unusual volume and options orders' },
  { key: 'indices', label: 'Index flow', hint: 'How the S&P 500 and Nasdaq 100 are moving today' },
];

export default async function OptionsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="flex h-full w-full flex-col p-2 gap-4 overflow-y-auto">
      <PageHeader
        title="Options & big-money flow"
        description="Unusually large trades as they print, a radar sweeping over 11,000 stocks for unusual volume and options orders, and how the major indices are moving."
        related={[
          { href: '/board', label: 'Signal board' },
          { href: '/filings', label: 'Fund holdings' },
          { href: '/ticker', label: 'Look up a stock' },
        ]}
      >
        <ViewTabs base="/options" views={VIEWS} active={view.key} />
      </PageHeader>
      {view.key === 'flow' && (
        <div className="h-[75vh] min-h-[520px]">
          <DarkPoolFlowPanel />
        </div>
      )}
      {view.key === 'radar' && (
        <div className="flex flex-col rounded-2xl border border-line bg-raised overflow-hidden min-h-[450px]">
          <QuantRadarIsland />
        </div>
      )}
      {view.key === 'indices' && <EquitiesFlowIsland />}
    </div>
  );
}
