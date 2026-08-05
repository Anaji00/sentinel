'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

const IntelligenceFeed = dynamic(() => import('@/components/IntelligenceFeed'), {
  loading: () => <PanelSkeleton title="Loading Intelligence Stream..." />,
  ssr: false,
});

export default function IntelligencePage() {
  return (
    <div className="flex h-full w-full flex-col p-4">
      <h1 className="text-2xl font-semibold mb-4 text-slate-100 uppercase tracking-wider">Multi-Agent Global Threat & Risk Feed</h1>
      <div className="flex-1 min-h-0 relative">
        <IntelligenceFeed />
      </div>
    </div>
  );
}
