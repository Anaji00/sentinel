'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

/**
 * Client islands for the Markets page's options and flow views.
 *
 * `next/dynamic` with `ssr: false` may only be called from a Client Component.
 * Isolating it here keeps the page a Server Component, so the heading and
 * layout render on the server while only the live panels hydrate.
 */
const QuantRadarPanel = dynamic(() => import('@/components/QuantRadarPanel'), {
  loading: () => <PanelSkeleton title="Loading options radar…" />,
  ssr: false,
});

const EquitiesFlowChart = dynamic(() => import('@/components/charts/EquitiesFlowChart'), {
  loading: () => <PanelSkeleton title="Loading equities flow…" />,
  ssr: false,
});

export function EquitiesFlowIsland() {
  return <EquitiesFlowChart />;
}

export function QuantRadarIsland() {
  return <QuantRadarPanel />;
}
