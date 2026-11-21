'use client';

/**
 * A 60-day trend line sized for a table cell. The colour is direction (up or down), the last point is a dot, and the cell
 * keeps a text label for screen readers, since the move is already stated in words beside it.
 */

import React from 'react';
import { drawable, sparkPoints, trend, trendWords } from '../../lib/spark';

export function InlineSpark({ closes, w = 72, h = 20 }: { closes: readonly number[] | undefined; w?: number; h?: number }) {
  const v = drawable(closes);
  if (!v) return <span className="text-ink-mute" title="Not enough price history">—</span>;
  const dir = trend(v);
  const stroke = dir === 'up' ? 'var(--positive)' : dir === 'down' ? 'var(--negative)' : 'var(--text-muted)';
  const pts = sparkPoints(v, w, h).split(' ');
  const last = pts[pts.length - 1].split(',');
  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} role="img" aria-label={trendWords(v)} className="inline-block align-middle">
      <title>{trendWords(v)}</title>
      <polyline points={pts.join(' ')} fill="none" stroke={stroke} strokeWidth="1.25" strokeLinejoin="round" />
      <circle cx={last[0]} cy={last[1]} r="1.8" fill={stroke} />
    </svg>
  );
}

export default InlineSpark;
