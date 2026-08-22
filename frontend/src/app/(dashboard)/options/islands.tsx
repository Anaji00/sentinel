'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

/**
 * Client islands for the options view. See intelligence/islands.tsx for why
 * these live outside the page component.
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
