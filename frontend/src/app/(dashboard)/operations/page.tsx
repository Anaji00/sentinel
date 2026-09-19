import React from 'react';
import OperationsConsole from '@/components/OperationsConsole';

export default function OperationsPage() {
  return (
    <div className="h-full w-full p-2 overflow-hidden">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Operations</h1>
      <OperationsConsole />
    </div>
  );
}
