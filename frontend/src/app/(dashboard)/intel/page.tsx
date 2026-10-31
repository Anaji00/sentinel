import React from 'react';
import CaseBoard from '@/components/CaseBoard';
import IntelConsole from '@/components/IntelConsole';
import OsintThreatMatrix from '@/components/OsintThreatMatrix';
import ReportBuilder from '@/components/ReportBuilder';
import { PageHeader } from '@/components/ui/PageHeader';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Intel: what were News & intel, World map and Investigations. The console
 * (map and feed on one time window) is the page; open-source intel, cases
 * and briefs are views of it. The old addresses redirect here.
 */
const VIEWS: View[] = [
  { key: 'console', label: 'Map & feed', hint: 'Where and when, on one time window' },
  { key: 'osint', label: 'Open-source intel', hint: 'Public-source stories ranked by how unusual they are' },
  { key: 'cases', label: 'Cases', hint: 'Follow a story over time' },
  { key: 'briefs', label: 'Briefs', hint: 'Turn a window of activity into a written brief' },
];

const DESCRIPTION: Record<string, string> = {
  console:
    'Ships, aircraft, conflict zones and every event the platform noticed, on one map and one feed, narrowed by the same time window.',
  osint:
    'Stories from public sources, ranked by how unusual they are: sanctions hits, ships switching off their transponders, and news no one else is reporting yet.',
  cases: 'Open a case to follow a story over time: attach events, add notes, and move it from open to resolved. Every change is recorded.',
  briefs:
    'Turn a window of activity into a written brief: pick a template and a time range. It only reports what the platform actually recorded.',
};

export default async function IntelPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="flex h-full w-full flex-col gap-4 overflow-y-auto p-2">
      <PageHeader
        title="Intel"
        description={DESCRIPTION[view.key]}
        related={[
          { href: '/watch', label: 'Surveillance map (US)' },
          { href: '/money', label: 'Follow the money' },
        ]}
      >
        <ViewTabs base="/intel" views={VIEWS} active={view.key} />
      </PageHeader>
      {view.key === 'console' && <IntelConsole />}
      {view.key === 'osint' && <OsintThreatMatrix />}
      {view.key === 'cases' && <CaseBoard />}
      {view.key === 'briefs' && <ReportBuilder />}
    </div>
  );
}
