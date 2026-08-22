'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

/**
 * Client island for the intelligence view.
 *
 * `next/dynamic` with `ssr: false` may only be called from a Client Component.
 * Isolating it here keeps the page itself a Server Component, so the heading and
 * layout render on the server and paint immediately while only the live feed
 * hydrates on the client.
 */
const IntelligenceFeed = dynamic(() => import('@/components/IntelligenceFeed'), {
  loading: () => <PanelSkeleton title="Loading intelligence stream…" />,
  ssr: false,
});

export function IntelligenceFeedIsland() {
  return <IntelligenceFeed />;
}
