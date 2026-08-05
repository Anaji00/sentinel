'use client';

import React, { Suspense } from 'react';
import dynamic from 'next/dynamic';
import { ResponsiveGridLayout } from 'react-grid-layout';

import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import { PanelSkeleton } from './ui/Skeleton';

const IntelligenceFeed = dynamic(() => import('./IntelligenceFeed'), {
  loading: () => <PanelSkeleton title="Loading Stream..." />,
  ssr: false,
});

const GraphExplorer = dynamic(() => import('./GraphExplorer'), {
  loading: () => <PanelSkeleton title="Loading Graph..." />,
  ssr: false,
});

const QuantRadarPanel = dynamic(() => import('./QuantRadarPanel'), {
  loading: () => <PanelSkeleton title="Loading Radar..." />,
  ssr: false,
});

const FinancialAdvisorAdvice = dynamic(() => import('./FinancialAdvisorAdvice'), {
  loading: () => <PanelSkeleton title="Loading Allocator..." />,
  ssr: false,
});

const DEFAULT_LAYOUT = [
  { i: 'intelligence', x: 0, y: 0, w: 4, h: 4, minW: 3, minH: 3 },
  { i: 'graph', x: 4, y: 0, w: 5, h: 4, minW: 4, minH: 3 },
  { i: 'radar', x: 9, y: 0, w: 3, h: 2, minW: 3, minH: 2 },
  { i: 'advisor', x: 9, y: 2, w: 3, h: 2, minW: 3, minH: 2 }
];

export function CommandCenterGrid() {
  const layouts = { lg: DEFAULT_LAYOUT };

  return (
    <div className="h-full w-full overflow-y-auto overflow-x-hidden p-2">
      <ResponsiveGridLayout
        className="layout"
        layouts={layouts}
        breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480, xxs: 0 }}
        cols={{ lg: 12, md: 10, sm: 6, xs: 4, xxs: 2 }}
        rowHeight={200}
        // @ts-ignore
        draggableHandle=".drag-handle"
        margin={[12, 12]}
      >
        <div key="intelligence" className="flex flex-col bg-[#0f1115] rounded-xl border border-slate-800 overflow-hidden">
          <div className="drag-handle h-8 bg-slate-900 border-b border-slate-800 flex items-center px-3 cursor-move">
            <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Intelligence Feed</span>
          </div>
          <div className="flex-1 min-h-0 relative">
            <Suspense fallback={<PanelSkeleton title="Stream Loading..." />}>
              <IntelligenceFeed />
            </Suspense>
          </div>
        </div>

        <div key="graph" className="flex flex-col bg-[#0f1115] rounded-xl border border-slate-800 overflow-hidden">
          <div className="drag-handle h-8 bg-slate-900 border-b border-slate-800 flex items-center px-3 cursor-move">
            <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Knowledge Graph</span>
          </div>
          <div className="flex-1 min-h-0 relative">
            <Suspense fallback={<PanelSkeleton title="Graph Loading..." />}>
              <GraphExplorer />
            </Suspense>
          </div>
        </div>

        <div key="radar" className="flex flex-col bg-[#0f1115] rounded-xl border border-slate-800 overflow-hidden">
          <div className="drag-handle h-8 bg-slate-900 border-b border-slate-800 flex items-center px-3 cursor-move">
            <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Quant Radar</span>
          </div>
          <div className="flex-1 min-h-0 relative">
            <Suspense fallback={<PanelSkeleton title="Radar Loading..." />}>
              <QuantRadarPanel />
            </Suspense>
          </div>
        </div>

        <div key="advisor" className="flex flex-col bg-[#0f1115] rounded-xl border border-slate-800 overflow-hidden">
          <div className="drag-handle h-8 bg-slate-900 border-b border-slate-800 flex items-center px-3 cursor-move">
            <span className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Financial Advisor</span>
          </div>
          <div className="flex-1 min-h-0 relative">
            <Suspense fallback={<PanelSkeleton title="Advisor Loading..." />}>
              <FinancialAdvisorAdvice />
            </Suspense>
          </div>
        </div>
      </ResponsiveGridLayout>
    </div>
  );
}
