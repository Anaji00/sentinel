'use client';

/**
 * Map layers for conflict zones, navigational warnings and every tracked vessel.
 *
 * Rendered inside the GlobalMap's ZoomableGroup. Vessels are drawn as one SVG
 * path per class rather than one element per hull: ~2,400 hulls as individual
 * <Marker>s is ~7,000 DOM nodes re-rendered on every poll, which is why the map
 * used to cap itself at sixty. A click on a path is resolved to the nearest
 * hull in projected coordinates.
 */

import React, { useMemo } from 'react';
import { Marker, useMapContext, useZoomPanContext } from 'react-simple-maps';
import { ConflictZone, LightDetection, LiveVessel, NavWarning, ZONE_COLOURS } from '../../lib/conflict';
import { PALETTE } from '../../lib/palette';

interface Projected<T> {
  item: T;
  x: number;
  y: number;
}

function useProjected<T extends { lat: number; lon: number }>(items: T[]): Projected<T>[] {
  const { projection } = useMapContext();
  return useMemo(() => {
    const out: Projected<T>[] = [];
    for (const item of items) {
      const p = projection([item.lon, item.lat]);
      if (p && Number.isFinite(p[0]) && Number.isFinite(p[1])) {
        out.push({ item, x: p[0], y: p[1] });
      }
    }
    return out;
  }, [items, projection]);
}

function dotsPath(points: { x: number; y: number }[], r: number): string {
  const d = 2 * r;
  let s = '';
  for (const { x, y } of points) {
    s += `M${(x - r).toFixed(2)},${y.toFixed(2)}a${r},${r} 0 1,0 ${d},0a${r},${r} 0 1,0 ${-d},0`;
  }
  return s;
}

/** The SVG-local point under a mouse event, in the path's own coordinates. */
function localPoint(e: React.MouseEvent<SVGPathElement>): { x: number; y: number } | null {
  const el = e.currentTarget;
  const ctm = el.getScreenCTM();
  const svg = el.ownerSVGElement;
  if (!ctm || !svg) return null;
  const pt = svg.createSVGPoint();
  pt.x = e.clientX;
  pt.y = e.clientY;
  const local = pt.matrixTransform(ctm.inverse());
  return { x: local.x, y: local.y };
}

function nearest<T>(points: Projected<T>[], at: { x: number; y: number }, within: number): T | null {
  let best: Projected<T> | null = null;
  let bestD = within * within;
  for (const p of points) {
    const dx = p.x - at.x;
    const dy = p.y - at.y;
    const d2 = dx * dx + dy * dy;
    if (d2 <= bestD) {
      best = p;
      bestD = d2;
    }
  }
  return best ? best.item : null;
}

export function VesselDotsLayer({
  vessels,
  tankersOnly,
  darkOnly,
  onSelect,
}: {
  vessels: LiveVessel[];
  tankersOnly: boolean;
  darkOnly: boolean;
  onSelect: (v: LiveVessel) => void;
}) {
  const { k } = useZoomPanContext();
  const shown = useMemo(
    () => vessels.filter((v) => (!tankersOnly || v.tanker) && (!darkOnly || v.dark)),
    [vessels, tankersOnly, darkOnly],
  );
  const projected = useProjected(shown);

  // Constant on-screen size whatever the zoom.
  const r = 1.4 / Math.max(1, k);
  const ring = 3.2 / Math.max(1, k);

  const classes = useMemo(() => {
    const base: Projected<LiveVessel>[] = [];
    const tanker: Projected<LiveVessel>[] = [];
    const flagged: Projected<LiveVessel>[] = [];
    const dark: Projected<LiveVessel>[] = [];
    for (const p of projected) {
      if (p.item.dark) dark.push(p);
      if (p.item.sanctioned) flagged.push(p);
      else if (p.item.tanker) tanker.push(p);
      else base.push(p);
    }
    return { base, tanker, flagged, dark };
  }, [projected]);

  const handleClick = (e: React.MouseEvent<SVGPathElement>) => {
    const at = localPoint(e);
    if (!at) return;
    const hit = nearest(projected, at, 6 / Math.max(1, k));
    if (hit) onSelect(hit);
  };

  return (
    <g>
      <path d={dotsPath(classes.base, r)} fill={PALETTE.positive} fillOpacity={0.75} onClick={handleClick} className="cursor-pointer" />
      <path d={dotsPath(classes.tanker, r * 1.25)} fill={PALETTE.caution} onClick={handleClick} className="cursor-pointer" />
      <path d={dotsPath(classes.flagged, r * 1.5)} fill={PALETTE.negative} onClick={handleClick} className="cursor-pointer" />
      {/* Dark hulls: a hollow ring at the last known position. */}
      <path
        d={dotsPath(classes.dark, ring)}
        fill="none"
        stroke="#e879f9"
        strokeWidth={1 / Math.max(1, k)}
        onClick={handleClick}
        className="cursor-pointer"
        pointerEvents="stroke"
      />
    </g>
  );
}

export function ConflictZonesLayer({
  zones,
  selectedId,
  onSelect,
}: {
  zones: ConflictZone[];
  selectedId: string | null;
  onSelect: (z: ConflictZone) => void;
}) {
  const { projection } = useMapContext();
  const { k } = useZoomPanContext();
  const scale = 1 / Math.max(1, k);

  return (
    <g>
      {zones.map((z) => {
        const [minLat, minLon, maxLat, maxLon] = z.bbox;
        const tl = projection([minLon, maxLat]);
        const br = projection([maxLon, minLat]);
        if (!tl || !br) return null;
        const colours = ZONE_COLOURS[z.status] ?? ZONE_COLOURS.watch;
        const selected = z.zone_id === selectedId;
        const flashpoint = z.status === 'watch' || z.status === 'escalating';
        return (
          <g key={z.zone_id} onClick={() => onSelect(z)} className="cursor-pointer">
            <rect
              x={tl[0]}
              y={tl[1]}
              width={Math.max(0, br[0] - tl[0])}
              height={Math.max(0, br[1] - tl[1])}
              fill={colours.fill}
              stroke={colours.stroke}
              strokeWidth={(selected ? 2 : 1) * scale}
              strokeDasharray={flashpoint ? `${4 * scale} ${3 * scale}` : undefined}
              rx={2 * scale}
            >
              <title>{`${z.name} — ${colours.label}`}</title>
            </rect>
            <text
              x={tl[0] + 2 * scale}
              y={tl[1] - 2 * scale}
              style={{
                fontFamily: 'monospace',
                fill: colours.stroke,
                fontSize: `${7 * scale}px`,
                fontWeight: 'bold',
              }}
            >
              {z.name}
              {z.trend === 'rising' ? ' ▲' : ''}
            </text>
          </g>
        );
      })}
    </g>
  );
}

export function NavWarningsLayer({
  warnings,
  onSelect,
}: {
  warnings: NavWarning[];
  onSelect: (w: NavWarning) => void;
}) {
  const { k } = useZoomPanContext();
  const s = 1 / Math.max(1, k);
  return (
    <g>
      {warnings.map((w) => (
        <Marker key={w.id} coordinates={[w.centroid[1], w.centroid[0]]} onClick={() => onSelect(w)}>
          <path
            d={`M0,${-5 * s} L${4.5 * s},${3.5 * s} L${-4.5 * s},${3.5 * s} Z`}
            fill={w.military ? 'rgba(239, 68, 68, 0.55)' : 'rgba(245, 158, 11, 0.4)'}
            stroke={w.military ? PALETTE.negative : PALETTE.caution}
            strokeWidth={0.8 * s}
            className="cursor-pointer"
          >
            <title>{`${w.id} — ${w.area}`}</title>
          </path>
        </Marker>
      ))}
    </g>
  );
}

/** Ships seen by their lights at night (VIIRS): small crosses, no identity. */
export function LightDetectionsLayer({ detections }: { detections: LightDetection[] }) {
  const { k } = useZoomPanContext();
  const projected = useProjected(detections);
  const r = 1.8 / Math.max(1, k);
  const d = projected
    .map(({ x, y }) => `M${(x - r).toFixed(2)},${y.toFixed(2)}h${2 * r}M${x.toFixed(2)},${(y - r).toFixed(2)}v${2 * r}`)
    .join('');
  return (
    <path d={d} stroke="#fde047" strokeWidth={0.8 / Math.max(1, k)} fill="none" pointerEvents="none">
      <title>VIIRS night-time light detections: lit vessels, no transponder needed</title>
    </path>
  );
}
