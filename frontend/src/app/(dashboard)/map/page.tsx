'use client';

import React, { Suspense } from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';
import { ErrorBoundary } from '@/components/ui/ErrorBoundary';
import { PageHeader } from '@/components/ui/PageHeader';

// Dynamically import GlobalMap without SSR as it uses browser APIs (canvas, WebGL)
const GlobalMap = dynamic(() => import('@/components/GlobalMap'), {
  loading: () => <PanelSkeleton title="Loading the world map…" />,
  ssr: false,
});

/**
 * The world map, inside the shell. It used to fill the whole window with a
 * single "Home" link, so the sidebar, search and every other page were one
 * click further away than from anywhere else.
 */
export default function GlobalMapPage() {
  return (
    <div className="flex h-full w-full flex-col gap-4 p-2">
      <PageHeader
        title="World map"
        description="Ships, aircraft, shipping chokepoints and conflict zones as they are reported, with the markets each one touches."
        related={[
          { href: '/intelligence', label: 'News & intel' },
          { href: '/watch', label: 'Surveillance map (US)' },
          { href: '/charts?view=economy', label: 'The economy' },
        ]}
      />
      <div className="relative flex-1 min-h-[calc(100vh-14rem)] overflow-hidden rounded-2xl border border-line bg-page">
        <ErrorBoundary fallbackTitle="The world map could not be drawn">
          <Suspense fallback={<PanelSkeleton title="Loading the world map…" />}>
            <GlobalMap />
          </Suspense>
        </ErrorBoundary>
      </div>
    </div>
  );
}
