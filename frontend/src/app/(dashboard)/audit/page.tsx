import React from 'react';
import AuditTrail from '@/components/AuditTrail';

export default function AuditPage() {
  return (
    <div className="h-full w-full overflow-hidden p-2">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Audit trail</h1>
      <AuditTrail />
    </div>
  );
}
