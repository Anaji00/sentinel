'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

/** Client island for the knowledge-graph explorer. */
const GraphExplorer = dynamic(() => import('@/components/GraphExplorer'), {
  loading: () => (
    <PanelSkeleton title="Initializing knowledge graph & correlation matrix…" />
  ),
  ssr: false,
});

export function GraphExplorerIsland() {
  return <GraphExplorer />;
}
