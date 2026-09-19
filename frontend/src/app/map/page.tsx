'use client';

import React, { Suspense } from 'react';
import Link from 'next/link';
import { ArrowLeft } from 'lucide-react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

import { ErrorBoundary } from '@/components/ui/ErrorBoundary';

// Dynamically import GlobalMap without SSR as it uses browser APIs (canvas, WebGL)
const GlobalMap = dynamic(() => import('@/components/GlobalMap'), {
  loading: () => <PanelSkeleton title="Loading WebGL Map..." />,
  ssr: false,
});

export default function GlobalMapPage() {
  return (
    <div className="relative h-screen w-full bg-page overflow-hidden">
      {/* HUD Overlay */}
      <div className="absolute top-4 left-4 z-50">
        <Link
          href="/"
          className="flex items-center gap-2 px-4 py-2 bg-raised/80 hover:bg-overlay text-ink rounded-lg border border-line-strong transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          <span className="font-semibold text-sm">Command Center</span>
        </Link>
      </div>

      {/* WebGL Map Container */}
      <div className="absolute inset-0">
        <ErrorBoundary fallbackTitle="Global Map Render Failure">
          <Suspense fallback={<PanelSkeleton title="Global Telemetry Map Loading..." />}>
            <GlobalMap />
          </Suspense>
        </ErrorBoundary>
      </div>
    </div>
  );
}
