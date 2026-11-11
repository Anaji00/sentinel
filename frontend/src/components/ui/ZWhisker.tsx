/**
 * How far a stock has moved from where its peers say it should be, as a 64px gauge: the middle is "in line", the end
 * marks are 4 usual-sized moves away, and the two short ticks are where an alert is raised (2.5). The dot is now; the
 * faint tick is the last reading. Colour appears only for a gap of 2 or more, so a calm gap does not compete.
 */

import React from 'react';
import { Z_TRIGGER, isNotable, zX } from '../../lib/zWhisker';

export function ZWhisker({ z, prior, width = 64 }: { z: number | null | undefined; prior?: number | null; width?: number }) {
  if (typeof z !== 'number' || !Number.isFinite(z)) return null;
  const h = 16;
  const mid = h / 2;
  const notable = isNotable(z);
  const dot = notable ? (z > 0 ? 'fill-positive' : 'fill-negative') : 'fill-ink-mute';
  const label = `Gap of ${z.toFixed(2)} usual-sized moves from its peers${prior != null ? `, previously ${prior.toFixed(2)}` : ''}`;
  return (
    <svg width={width} height={h} viewBox={`0 0 ${width} ${h}`} role="img" aria-label={label} className="shrink-0">
      <title>{label}</title>
      <line x1={0} x2={width} y1={mid} y2={mid} className="stroke-line-strong" strokeWidth={1} />
      <line x1={zX(0, width)} x2={zX(0, width)} y1={3} y2={h - 3} className="stroke-ink-mute" strokeWidth={1} />
      {[-Z_TRIGGER, Z_TRIGGER].map((t) => (
        <line key={t} x1={zX(t, width)} x2={zX(t, width)} y1={5} y2={h - 5} className="stroke-ink-mute" strokeWidth={1} />
      ))}
      {typeof prior === 'number' && Number.isFinite(prior) && <line x1={zX(prior, width)} x2={zX(prior, width)} y1={2} y2={h - 2} className="stroke-ink-mute" strokeOpacity={0.5} strokeWidth={1} />}
      <circle cx={zX(z, width)} cy={mid} r={3} className={dot} />
    </svg>
  );
}

export default ZWhisker;
