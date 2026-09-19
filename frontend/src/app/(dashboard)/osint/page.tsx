import React from 'react';
import OsintThreatMatrix from '@/components/OsintThreatMatrix';

export default function OsintPage() {
  return (
    <div className="h-full w-full p-2 overflow-hidden">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">OSINT matrix</h1>
      <OsintThreatMatrix />
    </div>
  );
}
