import React from 'react';
import OsintThreatMatrix from '@/components/OsintThreatMatrix';
import { IntelligenceFeedIsland } from './islands';
import { PageHeader } from '@/components/ui/PageHeader';
import { pickView, ViewTabs, type View } from '@/components/ui/ViewTabs';

/**
 * Server Component. The shell renders on the server; only the live feed is a
 * client island.
 *
 * News & alerts and open-source intel were two pages reading the same stream
 * from two angles; they are two views of one page now (/osint redirects here).
 */
const VIEWS: View[] = [
  { key: 'feed', label: 'News & alerts', hint: 'Everything the platform noticed, as it arrives' },
  { key: 'osint', label: 'Open-source intel', hint: 'Public-source stories ranked by how unusual they are' },
];

export default async function IntelligencePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const view = pickView(VIEWS, (await searchParams).view);
  return (
    <div className="flex h-full w-full flex-col gap-4 p-2">
      <PageHeader
        title="News & intel"
        description={
          view.key === 'osint'
            ? 'Stories from public sources, ranked by how unusual they are: sanctions hits, ships switching off their transponders, and news no one else is reporting yet.'
            : 'Everything the platform noticed, as it arrives: news, ships, markets and prediction markets, with how many independent sources back each story.'
        }
        related={[
          { href: '/cases', label: 'Cases' },
          { href: '/cases?view=briefs', label: 'Briefs' },
          { href: '/map', label: 'World map' },
        ]}
      >
        <ViewTabs base="/intelligence" views={VIEWS} active={view.key} />
      </PageHeader>
      <div className="flex-1 min-h-0 relative">
        {view.key === 'osint' ? <OsintThreatMatrix /> : <IntelligenceFeedIsland />}
      </div>
    </div>
  );
}
