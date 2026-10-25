import React from 'react';
import CaseBoard from '@/components/CaseBoard';
import ReportBuilder from '@/components/ReportBuilder';
import { PageHeader } from '@/components/ui/PageHeader';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Investigations: follow a story over time as a case, and write up what the
 * platform recorded as a brief. Two pages that pointed at each other are two
 * views of one (/reports redirects to ?view=briefs).
 */
const VIEWS: View[] = [
  { key: 'cases', label: 'Cases', hint: 'Follow a story over time' },
  { key: 'briefs', label: 'Briefs', hint: 'Turn a window of activity into a written brief' },
];

export default async function InvestigationsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="flex h-full w-full flex-col gap-4 p-2">
      <PageHeader
        title="Investigations"
        description={
          view.key === 'briefs'
            ? 'Turn a window of activity into a written brief: pick a template and a time range. It only reports what the platform actually recorded.'
            : 'Open a case to follow a story over time: attach events, add notes, and move it from open to resolved. Every change is recorded.'
        }
        related={[{ href: '/intelligence', label: 'News & intel' }, { href: '/watch', label: 'Surveillance map' }]}
      >
        <ViewTabs base="/cases" views={VIEWS} active={view.key} />
      </PageHeader>
      <div className="flex-1 min-h-0">{view.key === 'briefs' ? <ReportBuilder /> : <CaseBoard />}</div>
    </div>
  );
}
