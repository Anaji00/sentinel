import React from 'react';
import CallsWorkspace from '@/components/CallsWorkspace';
import SignalBoard from '@/components/SignalBoard';
import StockSearch from '@/components/StockSearch';
import StockNews from '@/components/StockNews';
import StockPredictions from '@/components/StockPredictions';
import { FreshnessStrip } from '@/components/TrackRecord';
import TradingSignals from '@/components/TradingSignals';
import { PageHeader } from '@/components/ui/PageHeader';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Calls: what were Stock calls, Signal board and Trade signals. The workspace
 * at the top answers "what now"; the views below hold each old page's depth.
 * The old addresses redirect here.
 */
const VIEWS: View[] = [
  { key: 'board', label: 'Every unusual stock', hint: 'The full board: every stock with unusual activity and its votes' },
  { key: 'signals', label: 'Proven signals', hint: 'Only patterns that have reliably moved prices across years of data' },
  { key: 'record', label: 'How the calls did', hint: 'Every call the analysts and the model made, and how often each was right' },
];

export default async function CallsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="h-full w-full space-y-5 overflow-y-auto p-2">
      <PageHeader
        title="Calls"
        description="Every stock with unusual activity, the model's call on it, and the evidence behind both. Pick a stock on the left, or look up any other one below, to see why it is moving."
        related={[
          { href: '/performance', label: 'Performance' },
          { href: '/markets', label: 'Markets' },
          { href: '/methodology', label: 'How it works' },
        ]}
      />
      <StockSearch />
      <CallsWorkspace />
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-line pt-5">
        <h2 className="text-head font-semibold text-ink">{view.label}</h2>
        <ViewTabs base="/calls" views={VIEWS} active={view.key} />
      </div>
      {view.key === 'board' && (
        <>
          <FreshnessStrip />
          <SignalBoard />
          <StockNews />
        </>
      )}
      {view.key === 'signals' && <TradingSignals />}
      {view.key === 'record' && <StockPredictions withIdeas={false} />}
    </div>
  );
}
