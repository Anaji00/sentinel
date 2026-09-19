import React from 'react';
import SubscriptionPanel from '@/components/SubscriptionPanel';

export default function AccountPage() {
  return (
    <div className="h-full w-full p-4 overflow-auto">
      {/* One heading per page. The panel below carries its own chrome,
          so this is for the document outline and for anyone navigating
          by heading rather than by eye. */}
      <h1 className="sr-only">Account</h1>
      <SubscriptionPanel />
    </div>
  );
}
