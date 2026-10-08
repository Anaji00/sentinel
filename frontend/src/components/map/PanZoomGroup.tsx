'use client';

/**
 * Wheel-zoom and drag-pan for the world map, without d3-zoom.
 *
 * react-simple-maps' ZoomableGroup drives d3-zoom@2, which calls
 * `selection.interrupt()` -- a method d3-transition installs on *its* copy of
 * d3-selection. react-simple-maps selects the map with a different nested copy,
 * one d3-transition never patched, so the map crashed on mount with
 * "interrupt is not a function". This group does the same job with pointer
 * events and publishes the same ZoomPanContext, so layers that read
 * `useZoomPanContext().k` keep their constant on-screen sizes.
 */
import React from 'react';
import { ZoomPanProvider, useMapContext } from 'react-simple-maps';

interface PanZoomGroupProps {
  center: [number, number];
  minZoom: number;
  maxZoom: number;
  children: React.ReactNode;
}

interface View {
  x: number;
  y: number;
  k: number;
}

const WHEEL_STEP = 0.0015;

export function PanZoomGroup({ center, minZoom, maxZoom, children }: PanZoomGroupProps) {
  const { width, height, projection } = useMapContext();
  const groupRef = React.useRef<SVGGElement>(null);
  const drag = React.useRef<{ px: number; py: number; x: number; y: number; moved: boolean } | null>(null);

  const initial = React.useMemo<View>(() => {
    const p = projection(center) as [number, number] | null;
    if (!p) return { x: 0, y: 0, k: minZoom };
    return { x: width / 2 - p[0] * minZoom, y: height / 2 - p[1] * minZoom, k: minZoom };
    // The starting view is fixed by the map's own geometry.
  }, [projection, center, width, height, minZoom]);
  const [view, setView] = React.useState<View>(initial);

  // Client pixels -> SVG user units, so zoom anchors under the cursor at any page size.
  const toSvg = React.useCallback((clientX: number, clientY: number): [number, number] => {
    const svg = groupRef.current?.ownerSVGElement;
    const ctm = svg?.getScreenCTM();
    if (!svg || !ctm) return [clientX, clientY];
    const pt = new DOMPoint(clientX, clientY).matrixTransform(ctm.inverse());
    return [pt.x, pt.y];
  }, []);

  React.useEffect(() => {
    const rect = groupRef.current;
    if (!rect) return undefined;
    // Registered by hand because React's onWheel is passive and cannot stop the page scrolling.
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const [sx, sy] = toSvg(e.clientX, e.clientY);
      setView((v) => {
        const k = Math.min(maxZoom, Math.max(minZoom, v.k * Math.exp(-e.deltaY * WHEEL_STEP)));
        const f = k / v.k;
        return { k, x: sx - (sx - v.x) * f, y: sy - (sy - v.y) * f };
      });
    };
    rect.addEventListener('wheel', onWheel, { passive: false });
    return () => rect.removeEventListener('wheel', onWheel);
  }, [toSvg, minZoom, maxZoom]);

  const onPointerDown = (e: React.PointerEvent<SVGGElement>) => {
    const [px, py] = toSvg(e.clientX, e.clientY);
    drag.current = { px, py, x: view.x, y: view.y, moved: false };
  };
  const onPointerMove = (e: React.PointerEvent<SVGGElement>) => {
    const d = drag.current;
    if (!d) return;
    const [px, py] = toSvg(e.clientX, e.clientY);
    if (!d.moved) {
      // Capture only once this is a drag, so a plain click still reaches the marker under it.
      if (Math.hypot(px - d.px, py - d.py) < 3) return;
      d.moved = true;
      e.currentTarget.setPointerCapture(e.pointerId);
    }
    setView((v) => ({ ...v, x: d.x + px - d.px, y: d.y + py - d.py }));
  };
  const endDrag = () => {
    drag.current = null;
  };

  const transformString = `translate(${view.x} ${view.y}) scale(${view.k})`;
  return (
    <ZoomPanProvider value={{ x: view.x, y: view.y, k: view.k, transformString }}>
      {/* Handlers sit on the group so events from countries and markers bubble to them. */}
      <g
        ref={groupRef}
        style={{ cursor: 'grab', touchAction: 'none' }}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
        onDoubleClick={() => setView(initial)}
      >
        <rect width={width} height={height} fill="transparent" />
        <g transform={transformString}>{children}</g>
      </g>
    </ZoomPanProvider>
  );
}
