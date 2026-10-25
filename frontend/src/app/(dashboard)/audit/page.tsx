import React from 'react';
import AuditTrail from '@/components/AuditTrail';
import { PageHeader } from '@/components/ui/PageHeader';

export default function AuditPage() {
  return (
    <div className="flex h-full w-full flex-col gap-4 p-2">
      <PageHeader
        title="Audit trail"
        description="A tamper-evident record of every order, flag and setting change. Each entry is chained to the one before it, so any edit breaks the chain."
        related={[{ href: '/operations', label: 'Operations' }, { href: '/account', label: 'Account' }]}
      />
      <div className="flex-1 min-h-0">
        <AuditTrail />
      </div>
    </div>
  );
}
