'use client';

/**
 * How likely a call is to go the right way, on a bar centred on a coin flip:
 * the fill starts at 50%, so a 57% call reads as "a bit better than even",
 * not "more than half full".
 */

import React from 'react';
import { ABSENT, formatNumber } from '../../lib/format';

/** Fill share of the bar's right half for a probability (0 at 50%, 1 at 75%+). */
export function edgeShare(p: number): number {
  return Math.max(0, Math.min(1, (p - 0.5) / 0.25));
}

export function ConvictionMeter({
  p,
  label = 'Chance it goes our way',
}: {
  p: number | null;
  label?: string;
}) {
  const tone =
    p == null ? 'bg-ink-mute' : p >= 0.6 ? 'bg-positive' : p >= 0.53 ? 'bg-caution' : 'bg-ink-mute';
  return (
    <div className="space-y-1">
      <div className="flex items-baseline justify-between text-micro">
        <span className="text-ink-mute">{label}</span>
        <span className="font-mono text-ink">
          {p == null ? ABSENT : `${formatNumber(p * 100, { decimals: 0 })}%`}
        </span>
      </div>
      <div
        className="relative h-1.5 rounded-full bg-inset overflow-hidden"
        role="meter"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={p == null ? undefined : Math.round(p * 100)}
      >
        <div className="absolute inset-y-0 left-1/2 w-px bg-line-strong" />
        {p != null && (
          <div
            className={`absolute inset-y-0 left-1/2 rounded-r-full ${tone}`}
            style={{ width: `${edgeShare(p) * 50}%` }}
          />
        )}
      </div>
    </div>
  );
}

export default ConvictionMeter;
