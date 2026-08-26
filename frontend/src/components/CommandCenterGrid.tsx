'use client';

import React, { useState, Suspense } from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from './ui/Skeleton';

const IntelligenceFeed = dynamic(() => import('./IntelligenceFeed'), {
  loading: () => <PanelSkeleton title="Loading Stream..." />,
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

const BondYieldsChart = dynamic(() => import('./charts/BondYieldsChart'), {
  loading: () => <PanelSkeleton title="Loading Bond Yields..." />,
  ssr: false,
});

type ViewMode = 'all' | 'intelligence' | 'radar' | 'advisor' | 'charts';

export function CommandCenterGrid() {
  const [activeView, setActiveView] = useState<ViewMode>('all');
  const [visibleFeeds, setVisibleFeeds] = useState<{ [key: string]: boolean }>({
    intelligence: true,
    radar: true,
    advisor: true,
  });

  const toggleFeed = (key: string) => {
    setVisibleFeeds((prev) => {
      const next = { ...prev, [key]: !prev[key] };
      // Ensure at least one feed remains visible
      if (!Object.values(next).some(Boolean)) return prev;
      return next;
    });
  };

  const setViewMode = (mode: ViewMode) => {
    setActiveView(mode);
    if (mode === 'all') {
      setVisibleFeeds({ intelligence: true, radar: true, advisor: true });
    } else {
      setVisibleFeeds({
        intelligence: mode === 'intelligence',
        radar: mode === 'radar',
        advisor: mode === 'advisor',
        charts: mode === 'charts',
      });
    }
  };

  const visibleCount = Object.values(visibleFeeds).filter(Boolean).length;

  // Determine dynamic grid layout style based on visible feed count
  let gridStyleClass = "grid grid-cols-1 md:grid-cols-2 grid-rows-2 gap-3 h-full w-full";
  if (visibleCount === 1) {
    gridStyleClass = "grid grid-cols-1 grid-rows-1 gap-0 h-full w-full";
  } else if (visibleCount === 2) {
    gridStyleClass = "grid grid-cols-1 md:grid-cols-2 grid-rows-1 gap-3 h-full w-full";
  } else if (visibleCount === 3) {
    gridStyleClass = "grid grid-cols-1 md:grid-cols-3 grid-rows-1 gap-3 h-full w-full";
  }

  return (
    <div className="h-full w-full flex flex-col bg-[#05070c] p-3 space-y-2.5 font-mono overflow-hidden">
      {/* Top HUD Feed Selector & Dynamic View Toggles */}
      <div className="flex flex-wrap items-center justify-between gap-2 px-3.5 py-2 bg-[#0b0e17] rounded-xl border border-cyan-500/20 shrink-0 shadow-lg text-xs">
        <div className="flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-[#00f2fe] animate-pulse" />
          <span className="text-[#00f2fe] font-extrabold tracking-wider uppercase text-[11px]">
            COMMAND FEEDS ({visibleCount} ACTIVE)
          </span>
        </div>

        {/* View Mode Preset Buttons */}
        <div className="flex items-center gap-1.5 text-[10px] overflow-x-auto">
          {[
            { id: 'all', label: 'ALL FEEDS (4-GRID)', icon: '🎛️' },
            { id: 'intelligence', label: 'INTELLIGENCE STREAM', icon: '📡' },
            { id: 'radar', label: 'QUANT RADAR', icon: '⚡' },
            { id: 'advisor', label: 'PORTFOLIO ALLOCATOR', icon: '💼' },
            { id: 'charts', label: 'MARKET CHARTS', icon: '📈' },
          ].map((view) => (
            <button
              key={view.id}
              onClick={() => setViewMode(view.id as ViewMode)}
              className={`px-2.5 py-1 rounded-lg font-bold transition-all cursor-pointer flex items-center gap-1.5 whitespace-nowrap ${
                activeView === view.id
                  ? 'bg-[#00f2fe] text-[#06080d] border border-white'
                  : 'bg-slate-900 text-slate-400 hover:text-white border border-slate-800'
              }`}
            >
              <span>{view.icon}</span>
              <span>{view.label}</span>
            </button>
          ))}
        </div>

        {/* Individual Feed Toggles */}
        <div className="flex items-center gap-1 text-[9px] text-slate-400">
          <span className="uppercase font-bold mr-1">TOGGLE:</span>
          {[
            { key: 'intelligence', label: 'STREAM' },
            { key: 'radar', label: 'RADAR' },
            { key: 'advisor', label: 'ALLOCATOR' },
            { key: 'charts', label: 'CHARTS' },
          ].map((f) => (
            <button
              key={f.key}
              onClick={() => toggleFeed(f.key)}
              className={`px-2 py-0.5 rounded font-bold transition-all cursor-pointer border ${
                visibleFeeds[f.key]
                  ? 'bg-cyan-500/20 text-cyan-300 border-cyan-500/40'
                  : 'bg-slate-950 text-slate-600 border-slate-800 line-through opacity-60'
              }`}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>

      {/* Dynamic Screen Scaling Layout Grid */}
      <div className="flex-1 min-h-0 w-full relative">
        <div className={gridStyleClass}>
          {visibleFeeds.intelligence && (
            <div className="flex flex-col bg-[#0b0e17] rounded-xl border border-slate-800/80 hover:border-cyan-500/40 shadow-xl overflow-hidden min-h-0 h-full w-full transition-all">
              <div className="flex-1 min-h-0 relative">
                <Suspense fallback={<PanelSkeleton title="Stream Loading..." />}>
                  <IntelligenceFeed />
                </Suspense>
              </div>
            </div>
          )}


          {visibleFeeds.radar && (
            <div className="flex flex-col bg-[#0b0e17] rounded-xl border border-slate-800/80 hover:border-cyan-500/40 shadow-xl overflow-hidden min-h-0 h-full w-full transition-all">
              <div className="flex-1 min-h-0 relative">
                <Suspense fallback={<PanelSkeleton title="Radar Loading..." />}>
                  <QuantRadarPanel />
                </Suspense>
              </div>
            </div>
          )}

          {visibleFeeds.advisor && (
            <div className="flex flex-col bg-[#0b0e17] rounded-xl border border-slate-800/80 hover:border-cyan-500/40 shadow-xl overflow-hidden min-h-0 h-full w-full transition-all">
              <div className="flex-1 min-h-0 relative">
                <Suspense fallback={<PanelSkeleton title="Advisor Loading..." />}>
                  <FinancialAdvisorAdvice />
                </Suspense>
              </div>
            </div>
          )}

          {visibleFeeds.charts && (
            <div className="flex flex-col bg-[#0b0e17] rounded-xl border border-slate-800/80 hover:border-cyan-500/40 shadow-xl overflow-hidden min-h-0 h-full w-full transition-all">
              <div className="flex-1 min-h-0 relative">
                <Suspense fallback={<PanelSkeleton title="Charts Loading..." />}>
                  <BondYieldsChart />
                </Suspense>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
