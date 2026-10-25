import React, { Suspense } from 'react';
import CivicWatch from '@/components/CivicWatch';

export default function WatchPage() {
  return (
    <div className="h-full w-full p-2 overflow-y-auto">
      {/* CivicWatch reads ?town= from the URL, which needs a Suspense boundary. */}
      <Suspense fallback={null}>
        <CivicWatch />
      </Suspense>
    </div>
  );
}
