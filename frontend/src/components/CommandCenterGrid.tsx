'use client';

import React, { useState, Suspense } from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from './ui/Skeleton';
import StateStrip from './StateStrip';
import { IconFlow, IconGraph, IconRadar, IconTarget, IconUp } from './ui/icons';

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

// The gainers and losers board. Everything under it -- the sweep, the sorted
// set, the per-ticker standing, the news and sector joins -- has been running
// and serving `/radar/movers`, and no component fetched it.
const MoversBoard = dynamic(() => import('./MoversBoard'), {
  loading: () => <PanelSkeleton title="Loading Movers..." />,
  ssr: false,
});

const BondYieldsChart = dynamic(() => import('./charts/BondYieldsChart'), {
  loading: () => <PanelSkeleton title="Loading Bond Yields..." />,
  ssr: false,
});

/** The panels, their labels and their marks -- declared once.
 *
 * Two separate literal arrays described the same five panels, one for the
 * presets and one for the toggles, and they had already drifted: the preset
 * list carried a `charts` entry that the `all` reset did not restore. */
const PANELS = [
  { key: 'intelligence', label: 'Intelligence', Icon: IconFlow },
  { key: 'radar', label: 'Radar', Icon: IconRadar },
  { key: 'movers', label: 'Movers', Icon: IconUp },
  { key: 'advisor', label: 'Portfolio', Icon: IconTarget },
  { key: 'charts', label: 'Charts', Icon: IconGraph },
] as const;

type PanelKey = (typeof PANELS)[number]['key'];

type Visibility = Record<PanelKey, boolean>;

// Charts starts closed: five panels inside a viewport-height grid gives each
// one under 300px, which is not enough for a chart to say anything.
const INITIAL: Visibility = {
  intelligence: true,
  radar: true,
  movers: true,
  advisor: true,
  charts: false,
};

export function CommandCenterGrid() {
  const [visibleFeeds, setVisibleFeeds] = useState<Visibility>(INITIAL);

  const toggleFeed = (key: PanelKey) => {
    setVisibleFeeds((prev) => {
      const next = { ...prev, [key]: !prev[key] };
      // Hiding the last one leaves an empty page whose only way back is the
      // control the operator just used, which is worse than refusing the click.
      if (!Object.values(next).some(Boolean)) return prev;
      return next;
    });
  };

  const showAll = () =>
    setVisibleFeeds(
      PANELS.reduce((acc, panel) => ({ ...acc, [panel.key]: true }), {} as Visibility),
    );

  const allVisible = PANELS.every((panel) => visibleFeeds[panel.key]);
  const visibleCount = Object.values(visibleFeeds).filter(Boolean).length;

  // Row count follows the panel count, and only the multi-column layouts pin
  // themselves to the viewport height.
  //
  // This was a fixed `grid-rows-2` at every width. Below `md` there is one
  // column, so four visible feeds needed four rows, got two, and the last two
  // rendered on top of the first two -- measured as an 79x16px overlap between
  // two different panels' text. A single column instead gets auto rows with a
  // sensible minimum and lets the page scroll, which is what a narrow screen
  // wants anyway.
  const SINGLE_COL = 'grid grid-cols-1 auto-rows-[minmax(20rem,auto)] gap-3 w-full md:h-full';

  // The event stream is the reason this page exists; the rest are context for
  // it. Equal quarters said otherwise. Above `lg` the feed keeps a full column
  // and the others share the remaining two, which also stops a three-line panel
  // header from wrapping inside a 350px box.
  let gridStyleClass = `${SINGLE_COL} md:grid-cols-2 md:grid-rows-2`;
  if (visibleCount === 1) {
    gridStyleClass = 'grid grid-cols-1 grid-rows-1 gap-0 h-full w-full';
  } else if (visibleCount === 2) {
    gridStyleClass = `${SINGLE_COL} md:grid-cols-2 md:grid-rows-1`;
  } else if (visibleCount === 3) {
    gridStyleClass = `${SINGLE_COL} md:grid-cols-3 md:grid-rows-1`;
  } else if (visibleCount >= 4) {
    gridStyleClass = `${SINGLE_COL} md:grid-cols-2 md:grid-rows-2 lg:grid-cols-3`;
  }

  return (
    <div className="min-h-full w-full flex flex-col bg-page p-2 sm:p-3 space-y-2.5 md:h-full md:overflow-hidden">
      <StateStrip />

      {/* One control, one source of truth.
          `visibleFeeds` is the state; "All" is a reset rather than a seventh
          mode, so no preset can disagree with what is actually shown. */}
      <div className="flex flex-wrap items-center gap-2 px-1 shrink-0">
        <span className="stat-label">Panels</span>
        <div className="flex flex-wrap items-center gap-1.5 text-micro">
          <button
            onClick={showAll}
            disabled={allVisible}
            className="rounded-md border border-line px-2.5 py-1 font-medium text-ink-dim transition-colors enabled:cursor-pointer enabled:hover:border-line-strong enabled:hover:text-ink disabled:opacity-40"
          >
            All
          </button>
          {PANELS.map((panel) => (
            <button
              key={panel.key}
              onClick={() => toggleFeed(panel.key)}
              aria-pressed={Boolean(visibleFeeds[panel.key])}
              className={`flex items-center gap-1.5 rounded-md border px-2.5 py-1 font-medium transition-colors cursor-pointer ${
                visibleFeeds[panel.key]
                  ? 'border-line-accent bg-accent-dim text-accent'
                  : 'border-line text-ink-mute hover:text-ink-dim'
              }`}
            >
              <panel.Icon />
              {panel.label}
            </button>
          ))}
        </div>
      </div>

      {/* Dynamic Screen Scaling Layout Grid */}
      <div className="flex-1 md:min-h-0 w-full relative">
        <div className={gridStyleClass}>
          {visibleFeeds.intelligence && (
            <div className="panel panel-interactive flex flex-col overflow-hidden min-h-0 h-full w-full md:row-span-2">
              <div className="flex-1 min-h-0 relative">
                <Suspense fallback={<PanelSkeleton title="Stream Loading..." />}>
                  <IntelligenceFeed />
                </Suspense>
              </div>
            </div>
          )}

          {visibleFeeds.movers && (
            <div className="flex flex-col rounded-xl overflow-hidden min-h-0 h-full w-full transition-all">
              <div className="flex-1 min-h-0 relative">
                <Suspense fallback={<PanelSkeleton title="Movers Loading..." />}>
                  <MoversBoard />
                </Suspense>
              </div>
            </div>
          )}

          {visibleFeeds.radar && (
            <div className="panel panel-interactive flex flex-col overflow-hidden min-h-0 h-full w-full">
              <div className="flex-1 min-h-0 relative">
                <Suspense fallback={<PanelSkeleton title="Radar Loading..." />}>
                  <QuantRadarPanel />
                </Suspense>
              </div>
            </div>
          )}

          {visibleFeeds.advisor && (
            <div className="panel panel-interactive flex flex-col overflow-hidden min-h-0 h-full w-full">
              <div className="flex-1 min-h-0 relative">
                <Suspense fallback={<PanelSkeleton title="Advisor Loading..." />}>
                  <FinancialAdvisorAdvice />
                </Suspense>
              </div>
            </div>
          )}

          {visibleFeeds.charts && (
            <div className="panel panel-interactive flex flex-col overflow-hidden min-h-0 h-full w-full">
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
