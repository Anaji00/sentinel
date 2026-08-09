'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { Activity, Radio } from 'lucide-react';

interface SeriesPoint {
  timestamp: string;
  price: number;
  volume: number;
  anomaly_score: number;
}

interface MarketSeriesResponse {
  symbols: string[];
  series: Record<string, SeriesPoint[]>;
}

export default function BondYieldsChart() {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  const { data } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=US10Y,US02Y,TLT,IEF&limit=50',
    fetcher,
    { refreshInterval: 3000 }
  );

  const us10ySeries = data?.series?.['US10Y'] || [];
  const us02ySeries = data?.series?.['US02Y'] || [];

  const hasData = us10ySeries.length > 0 || us02ySeries.length > 0;

  const latest10Y = us10ySeries[us10ySeries.length - 1]?.price || null;
  const latest2Y = us02ySeries[us02ySeries.length - 1]?.price || null;
  const spread = latest10Y !== null && latest2Y !== null ? latest10Y - latest2Y : null;
  const isInverted = spread !== null ? spread < 0 : false;

  const pointsCount = Math.max(us10ySeries.length, us02ySeries.length);

  // Normalize SVG Path Coordinates for Real Ticks Only
  const { path10Y, path2Y, pointsData } = useMemo(() => {
    if (!hasData) {
      return { path10Y: '', path2Y: '', pointsData: [] };
    }
    const p10: { x: number; y: number }[] = [];
    const p2: { x: number; y: number }[] = [];
    const combined: { time: string; y10: number | null; y2: number | null; spread: number | null }[] = [];

    const allPrices = [...us10ySeries.map(s => s.price), ...us02ySeries.map(s => s.price)];
    const minVal = Math.min(...allPrices) * 0.98 || 3.80;
    const maxVal = Math.max(...allPrices) * 1.02 || 4.80;
    const height = 180;
    const width = 600;

    for (let i = 0; i < pointsCount; i++) {
      const x = (i / Math.max(1, pointsCount - 1)) * width;
      const v10 = us10ySeries[i]?.price ?? null;
      const v2 = us02ySeries[i]?.price ?? null;

      if (v10 !== null) {
        const y10 = height - ((v10 - minVal) / (maxVal - minVal || 1)) * height;
        p10.push({ x, y: y10 });
      }

      if (v2 !== null) {
        const y2 = height - ((v2 - minVal) / (maxVal - minVal || 1)) * height;
        p2.push({ x, y: y2 });
      }

      combined.push({
        time: us10ySeries[i]?.timestamp || us02ySeries[i]?.timestamp || new Date().toISOString(),
        y10: v10,
        y2: v2,
        spread: v10 !== null && v2 !== null ? v10 - v2 : null,
      });
    }

    const path10YStr = p10.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
    const path2YStr = p2.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');

    return { path10Y: path10YStr, path2Y: path2YStr, pointsData: combined };
  }, [us10ySeries, us02ySeries, pointsCount, hasData]);

  const activeHoverPoint = hoverIndex !== null && pointsData[hoverIndex] ? pointsData[hoverIndex] : null;

  return (
    <Card
      title="U.S. TREASURY BOND YIELD CURVE & SPREAD TELEMETRY"
      badge={
        hasData ? (
          <Badge variant={isInverted ? 'warning' : 'live'} pulse>
            {isInverted ? 'YIELD CURVE INVERTED' : 'NORMAL CURVE'}
          </Badge>
        ) : (
          <Badge variant="warning" pulse>
            AWAITING LIVE DATA STREAM...
          </Badge>
        )
      }
      noPadding
    >
      <div className="p-4 space-y-3 font-mono">
        {/* Metric Summary Ribbon */}
        <div className="grid grid-cols-3 gap-3 bg-[#080b12] p-3 rounded-lg border border-cyan-500/20 text-xs">
          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">10Y TREASURY YIELD (US10Y)</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-cyan-400 font-extrabold text-base">
                {latest10Y !== null ? `${latest10Y.toFixed(2)}%` : 'AWAITING FEED...'}
              </span>
              <Activity className="w-3.5 h-3.5 text-cyan-400 animate-pulse" />
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">2Y TREASURY YIELD (US02Y)</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-purple-400 font-extrabold text-base">
                {latest2Y !== null ? `${latest2Y.toFixed(2)}%` : 'AWAITING FEED...'}
              </span>
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">2Y/10Y SPREAD</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className={`font-extrabold text-base ${spread !== null && spread < 0 ? 'text-rose-400' : 'text-emerald-400'}`}>
                {spread !== null ? `${spread > 0 ? '+' : ''}${(spread * 100).toFixed(1)} bps` : 'AWAITING FEED...'}
              </span>
            </div>
          </div>
        </div>

        {/* SVG Yield Chart or Awaiting Live Stream State */}
        {hasData ? (
          <div className="relative h-48 w-full bg-[#05070c] rounded-lg border border-slate-800 p-2 overflow-hidden">
            <svg
              viewBox="0 0 600 180"
              className="w-full h-full overflow-visible"
              onMouseLeave={() => setHoverIndex(null)}
            >
              <line x1="0" y1="45" x2="600" y2="45" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.5" />
              <line x1="0" y1="90" x2="600" y2="90" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.5" />
              <line x1="0" y1="135" x2="600" y2="135" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.5" />

              {path10Y && (
                <path d={path10Y} fill="none" stroke="#00f2fe" strokeWidth="2.5" strokeLinecap="round" />
              )}
              {path2Y && (
                <path d={path2Y} fill="none" stroke="#a855f7" strokeWidth="2.5" strokeLinecap="round" />
              )}
            </svg>
          </div>
        ) : (
          <div className="h-48 w-full bg-[#05070c] rounded-lg border border-dashed border-amber-500/30 flex flex-col items-center justify-center text-center p-4 space-y-2">
            <Radio className="w-6 h-6 text-amber-400 animate-pulse" />
            <span className="text-xs font-bold text-amber-400 uppercase tracking-widest">
              AWAITING LIVE DATA STREAM FOR TREASURY YIELDS
            </span>
            <p className="text-[10px] text-slate-500 max-w-sm">
              Backend live WebSocket and REST pollers are active. No hardcoded baselines. Ticks will render automatically upon database ingestion.
            </p>
          </div>
        )}
      </div>
    </Card>
  );
}
