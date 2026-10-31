'use client';

/**
 * Where and when, together: the world map and the event feed side by side,
 * narrowed by one time slider. They were separate pages (World map, News &
 * intel), so a ship going dark and the market move it caused were never on
 * screen at once.
 */

import React, { useState } from 'react';
import dynamic from 'next/dynamic';
import { ErrorBoundary } from './ui/ErrorBoundary';
import { PanelSkeleton } from './ui/Skeleton';

const GlobalMap = dynamic(() => import('./GlobalMap'), {
  loading: () => <PanelSkeleton title="Loading the world map…" />,
  ssr: false,
});
const IntelligenceFeed = dynamic(() => import('./IntelligenceFeed'), {
  loading: () => <PanelSkeleton title="Loading intelligence stream…" />,
  ssr: false,
});

export const WINDOW_STEPS = [1, 3, 6, 12, 24] as const;

export default function IntelConsole() {
  const [step, setStep] = useState(WINDOW_STEPS.length - 1);
  const hours = WINDOW_STEPS[step];
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-4 rounded-2xl border border-line bg-raised px-4 py-3">
        <label htmlFor="intel-window" className="text-sm text-ink-dim">
          Show the last
        </label>
        <input
          id="intel-window"
          type="range"
          min={0}
          max={WINDOW_STEPS.length - 1}
          step={1}
          value={step}
          onChange={(e) => setStep(Number(e.target.value))}
          aria-valuetext={`${hours} hours`}
          className="w-56 accent-[var(--accent)]"
        />
        <span className="font-mono text-sm text-ink tabular">{hours} h</span>
        <span className="text-xs text-ink-mute">Narrows the map and the feed together.</span>
      </div>
      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1.6fr)_minmax(360px,1fr)]">
        <div className="relative h-[70vh] min-h-[520px] overflow-hidden rounded-2xl border border-line bg-page">
          <ErrorBoundary fallbackTitle="The world map could not be drawn">
            <GlobalMap windowHours={hours} />
          </ErrorBoundary>
        </div>
        <div className="relative h-[70vh] min-h-[520px] overflow-hidden rounded-2xl border border-line bg-raised">
          <IntelligenceFeed windowHours={hours} />
        </div>
      </div>
    </div>
  );
}
