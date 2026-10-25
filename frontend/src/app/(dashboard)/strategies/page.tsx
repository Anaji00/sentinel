import React from 'react';
import SignalAttribution from '@/components/SignalAttribution';
import StrategyPerformance from '@/components/StrategyPerformance';
import { PageHeader } from '@/components/ui/PageHeader';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Research: how strategies would have done on past prices, and which signals
 * actually carry information. Both answer "does this work?", so they are two
 * views of one page (/attribution redirects to ?view=drivers).
 */
const VIEWS: View[] = [
  { key: 'backtests', label: 'Backtests', hint: 'How each strategy would have done on past prices, after costs' },
  { key: 'drivers', label: 'What drives signals', hint: 'Which signals made a story more likely to matter' },
];

export default async function ResearchPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="flex h-full w-full flex-col gap-4 p-2">
      <PageHeader
        title="Research"
        description={
          view.key === 'drivers'
            ? 'Which signals actually make a story more likely to matter, measured against what happened afterwards. A signal that points the wrong way is flagged.'
            : 'How each trading strategy would have done on past prices, after costs. Sort by any column; a high return with a poor hit rate usually means one lucky trade.'
        }
        related={[{ href: '/signals', label: 'Trade signals' }, { href: '/methodology', label: 'How it works' }]}
      >
        <ViewTabs base="/strategies" views={VIEWS} active={view.key} />
      </PageHeader>
      <div className="flex-1 min-h-0">{view.key === 'drivers' ? <SignalAttribution /> : <StrategyPerformance />}</div>
    </div>
  );
}
