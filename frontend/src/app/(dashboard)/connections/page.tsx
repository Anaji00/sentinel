import React from 'react';
import Connections from '@/components/Connections';
import { PageHeader } from '@/components/ui/PageHeader';

export default function ConnectionsPage() {
  return (
    <div className="flex h-full w-full flex-col gap-5 overflow-y-auto p-2 text-ink">
      <PageHeader
        title="Connections"
        description="Who and what is linked to a company, ship, flag or place, and how two of them are joined. Only links the platform has stored are drawn."
        related={[
          { href: '/money', label: 'Follow the money' },
          { href: '/intel', label: 'Intel' },
        ]}
      />
      <React.Suspense fallback={null}>
        <Connections />
      </React.Suspense>
    </div>
  );
}
