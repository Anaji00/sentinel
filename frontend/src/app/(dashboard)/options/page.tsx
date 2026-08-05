'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

const QuantRadarPanel = dynamic(() => import('@/components/QuantRadarPanel'), {
  loading: () => <PanelSkeleton title="Loading Options Radar..." />,
  ssr: false,
});

const EquitiesFlowChart = dynamic(() => import('@/components/charts/EquitiesFlowChart'), {
  loading: () => <PanelSkeleton title="Loading Equities Flow..." />,
  ssr: false,
});

export default function OptionsPage() {
  return (
    <div className="flex h-full w-full flex-col p-4 gap-4 overflow-y-auto font-mono">
      <div className="flex items-center justify-between mb-1">
        <h1 className="text-xl font-bold text-slate-100 uppercase tracking-wider">Advanced Options Flow & SPY/QQQ Market Surface</h1>
      </div>

      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-12">
          <EquitiesFlowChart />
        </div>
      </div>
      
      <div className="grid grid-cols-12 gap-4 flex-1 min-h-0">
        <div className="col-span-12 flex flex-col bg-[#0f1115] rounded-xl border border-slate-800 overflow-hidden relative min-h-[450px]">
          <QuantRadarPanel />
        </div>
      </div>
    </div>
  );
}
