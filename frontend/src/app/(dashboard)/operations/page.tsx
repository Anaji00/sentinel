import React from 'react';
import OperationsConsole from '@/components/OperationsConsole';
import { PageHeader } from '@/components/ui/PageHeader';

export default function OperationsPage() {
  return (
    <div className="h-full w-full overflow-y-auto p-2 space-y-4">
      <PageHeader
        title="Operations"
        description="The jobs that need a person: events that failed to process, companies that may be the same entity, and rules waiting for feedback."
        related={[{ href: '/audit', label: 'Audit trail' }, { href: '/agents', label: 'AI analysts' }]}
      />
      <OperationsConsole />
    </div>
  );
}
