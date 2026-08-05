'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

const GraphExplorer = dynamic(() => import('@/components/GraphExplorer'), {
  loading: () => <PanelSkeleton title="Loading Macro Graph..." />,
  ssr: false,
});

const FinancialAdvisorAdvice = dynamic(() => import('@/components/FinancialAdvisorAdvice'), {
  loading: () => <PanelSkeleton title="Loading Macro Advisor..." />,
  ssr: false,
});

const BondYieldsChart = dynamic(() => import('@/components/charts/BondYieldsChart'), {
  loading: () => <PanelSkeleton title="Loading Bond Yields..." />,
  ssr: false,
});

export default function MacroPage() {
  return (
    <div className="flex h-full w-full flex-col p-4 gap-4 overflow-y-auto font-mono">
      <div className="flex items-center justify-between mb-1">
        <h1 className="text-xl font-bold text-slate-100 uppercase tracking-wider">Fed & Macro Economic Exposure Matrix</h1>
      </div>

      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-12">
          <BondYieldsChart />
        </div>
      </div>
      
      <div className="grid grid-cols-12 gap-4 flex-1 min-h-0">
        <div className="col-span-8 flex flex-col bg-[#0f1115] rounded-xl border border-slate-800 overflow-hidden relative min-h-[450px]">
          <GraphExplorer />
        </div>
        
        <div className="col-span-4 flex flex-col bg-[#0f1115] rounded-xl border border-slate-800 overflow-hidden relative min-h-[450px]">
          <FinancialAdvisorAdvice />
        </div>
      </div>
    </div>
  );
}
