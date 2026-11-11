import React from 'react';
import Desk from '@/components/Desk';
import { OperatorConsole } from '@/components/OperatorConsole';
import { TodayOverview } from '@/components/TodayOverview';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Home opens on the desk: stocks moving against their peers on the left, the one you pick in the middle, its bull and bear case
 * on the right. The earlier overview (today's top calls, your stocks, who is watching you) is one tab away and unchanged.
 */
const VIEWS: View[] = [
  { key: 'desk', label: 'Desk', hint: 'Stocks moving against their peers, and the one you pick' },
  { key: 'overview', label: 'Today overview', hint: "Today's top calls, your stocks, and who is watching you" },
];

export default async function DashboardOverviewPage({ searchParams }: { searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="h-full w-full space-y-3 overflow-y-auto pb-6">
      <div className="flex justify-end">
        <ViewTabs base="/" views={VIEWS} active={view.key} />
      </div>
      {view.key === 'desk' ? (
        <Desk />
      ) : (
        <div className="space-y-6">
          {/* Today first: what a person opens the app for. The live console follows for anyone checking the plumbing. */}
          <TodayOverview />
          <OperatorConsole />
        </div>
      )}
    </div>
  );
}
