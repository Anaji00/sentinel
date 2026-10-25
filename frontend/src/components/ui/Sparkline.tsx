'use client';

/**
 * A price line in a few dozen pixels: the shape of the last quarter at a
 * glance, coloured by which way it went over the stretch shown.
 */

import React, { useId } from 'react';

/** SVG paths for `values` in a `w` x `h` box: the line and the area under it. */
export function sparkPaths(
  values: number[],
  w: number,
  h: number,
  pad = 2,
): { line: string; area: string } {
  const pts = values.filter((v) => Number.isFinite(v));
  if (pts.length < 2) return { line: '', area: '' };
  const lo = Math.min(...pts);
  const hi = Math.max(...pts);
  const span = hi - lo || 1;
  const xy = pts.map((v, i) => [
    (i / (pts.length - 1)) * w,
    pad + (1 - (v - lo) / span) * (h - 2 * pad),
  ]);
  const line = xy.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  return { line, area: `${line} L${w},${h} L0,${h} Z` };
}

export function Sparkline({
  values,
  width = 120,
  height = 36,
  label,
  className = '',
}: {
  values: number[] | undefined;
  width?: number;
  height?: number;
  label?: string;
  className?: string;
}) {
  const id = useId();
  const v = (values ?? []).filter((x) => Number.isFinite(x));
  if (v.length < 2) return <div style={{ width, height }} className={className} aria-hidden />;
  const up = v[v.length - 1] >= v[0];
  const { line, area } = sparkPaths(v, width, height);
  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      width={width}
      height={height}
      className={`${up ? 'text-positive' : 'text-negative'} ${className}`}
      role="img"
      aria-label={label ?? `${up ? 'Up' : 'Down'} over the period shown`}
    >
      <defs>
        <linearGradient id={id} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor="currentColor" stopOpacity="0.28" />
          <stop offset="100%" stopColor="currentColor" stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={area} fill={`url(#${id})`} />
      <path d={line} fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" />
    </svg>
  );
}

export default Sparkline;
