'use client';

import React, { Suspense } from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from './ui/Skeleton';

// Dynamic imports with ssr: false inside Client Component wrapper
const IntelligenceFeed = dynamic(() => import('./IntelligenceFeed'), {
  loading: () => <PanelSkeleton title="Loading Stream..." />,
  ssr: false,
});

const GraphExplorer = dynamic(() => import('./GraphExplorer'), {
  loading: () => <PanelSkeleton title="Loading Graph..." />,
  ssr: false,
});

const GlobalMap = dynamic(() => import('./GlobalMap'), {
  loading: () => <PanelSkeleton title="Loading Telemetry Map..." />,
  ssr: false,
});

const FinancialAdvisorAdvice = dynamic(() => import('./FinancialAdvisorAdvice'), {
  loading: () => <PanelSkeleton title="Loading Allocator..." />,
  ssr: false,
});

const QuantRadarPanel = dynamic(() => import('./QuantRadarPanel'), {
  loading: () => <PanelSkeleton title="Loading Radar & Agents..." />,
  ssr: false,
});

export default function DashboardClient() {
  return (
    <main className="flex-1 grid grid-cols-12 gap-3 p-3 min-h-0 overflow-hidden">
      {/* Column 1: Left Intelligence Feed & AI Scenarios (25%) */}
      <section className="col-span-3 h-full min-h-0 flex flex-col">
        <Suspense fallback={<PanelSkeleton title="Stream Loading..." />}>
          <IntelligenceFeed />
        </Suspense>
      </section>

      {/* Column 2: Center Telemetry Map & Knowledge Graph (50%) */}
      <section className="col-span-6 h-full min-h-0 flex flex-col gap-3">
        {/* Top Half: Global Telemetry Map */}
        <div className="h-1/2 w-full min-h-0 relative">
          <Suspense fallback={<PanelSkeleton title="Global Telemetry Map Loading..." />}>
            <GlobalMap />
          </Suspense>
        </div>

        {/* Bottom Half: Knowledge Graph Network */}
        <div className="h-1/2 w-full min-h-0 relative">
          <Suspense fallback={<PanelSkeleton title="Knowledge Graph Loading..." />}>
            <GraphExplorer />
          </Suspense>
        </div>
      </section>

      {/* Column 3: Right Quant Radar & Portfolio Allocator (25%) */}
      <section className="col-span-3 h-full min-h-0 flex flex-col gap-3">
        <div className="h-1/2 w-full min-h-0 relative">
          <Suspense fallback={<PanelSkeleton title="Radar Sweeps & Agent Processes Loading..." />}>
            <QuantRadarPanel />
          </Suspense>
        </div>
        <div className="h-1/2 w-full min-h-0 relative">
          <Suspense fallback={<PanelSkeleton title="Quantitative Allocator Loading..." />}>
            <FinancialAdvisorAdvice />
          </Suspense>
        </div>
      </section>
    </main>
  );
}
