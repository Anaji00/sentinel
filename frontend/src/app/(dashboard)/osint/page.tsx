import React from 'react';
import OsintThreatMatrix from '@/components/OsintThreatMatrix';
import { PageHeader } from '@/components/ui/PageHeader';

/** Open-source intel for the world: public-source stories ranked by how unusual they are. */
export default function OsintPage() {
  return (
    <div className="flex h-full w-full flex-col gap-4 overflow-y-auto p-2">
      <PageHeader
        title="Open-source intel"
        description="Stories from public sources, ranked by how unusual they are: sanctions hits, ships switching off their transponders, and news no one else is reporting yet."
        related={[
          { href: '/intel', label: 'World map and feed' },
          { href: '/watch', label: 'Watchlight' },
        ]}
      />
      <OsintThreatMatrix />
    </div>
  );
}
