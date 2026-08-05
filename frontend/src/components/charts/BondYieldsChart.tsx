'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { TrendingUp, TrendingDown, Activity, AlertTriangle } from 'lucide-react';

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
    { refreshInterval: 5000 }
  );

  const us10ySeries = data?.series?.['US10Y'] || [];
  const us02ySeries = data?.series?.['US02Y'] || [];

  const latest10Y = us10ySeries[us10ySeries.length - 1]?.price || 4.25;
  const latest2Y = us02ySeries[us02ySeries.length - 1]?.price || 4.45;
  const spread = latest10Y - latest2Y;
  const isInverted = spread < 0;

  const pointsCount = Math.max(us10ySeries.length, 30);

  // Normalize SVG Path Coordinates
  const { path10Y, path2Y, pointsData } = useMemo(() => {
    const p10: { x: number; y: number }[] = [];
    const p2: { x: number; y: number }[] = [];
    const combined: { time: string; y10: number; y2: number; spread: number }[] = [];

    const minVal = 3.80;
    const maxVal = 4.80;
    const height = 180;
    const width = 600;

    for (let i = 0; i < pointsCount; i++) {
      const x = (i / Math.max(1, pointsCount - 1)) * width;
      const v10 = us10ySeries[i]?.price || (4.25 + Math.sin(i * 0.2) * 0.12);
      const v2 = us02ySeries[i]?.price || (4.45 + Math.cos(i * 0.15) * 0.10);

      const y10 = height - ((v10 - minVal) / (maxVal - minVal)) * height;
      const y2 = height - ((v2 - minVal) / (maxVal - minVal)) * height;

      p10.push({ x, y: y10 });
      p2.push({ x, y: y2 });
      combined.push({
        time: us10ySeries[i]?.timestamp || new Date().toISOString(),
        y10: v10,
        y2: v2,
        spread: v10 - v2,
      });
    }

    const path10YStr = p10.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
    const path2YStr = p2.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');

    return { path10Y: path10YStr, path2Y: path2YStr, pointsData: combined };
  }, [us10ySeries, us02ySeries, pointsCount]);

  const activeHoverPoint = hoverIndex !== null && pointsData[hoverIndex] ? pointsData[hoverIndex] : null;

  return (
    <Card
      title="U.S. TREASURY BOND YIELD CURVE & SPREAD TELEMETRY"
      badge={
        <Badge variant={isInverted ? 'warning' : 'live'} pulse>
          {isInverted ? 'YIELD CURVE INVERTED' : 'NORMAL CURVE'}
        </Badge>
      }
      noPadding
    >
      <div className="p-4 space-y-3 font-mono">
        {/* Metric Summary Ribbon */}
        <div className="grid grid-cols-3 gap-3 bg-[#080b12] p-3 rounded-lg border border-cyan-500/20 text-xs">
          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">10Y TREASURY YIELD (US10Y)</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-cyan-400 font-extrabold text-base">{latest10Y.toFixed(2)}%</span>
              <Activity className="w-3.5 h-3.5 text-cyan-400 animate-pulse" />
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">2Y TREASURY YIELD (US02Y)</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-purple-400 font-extrabold text-base">{latest2Y.toFixed(2)}%</span>
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">10Y - 2Y SPREAD (INVERSION)</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className={`font-extrabold text-base ${isInverted ? 'text-amber-400' : 'text-emerald-400'}`}>
                {spread > 0 ? `+${spread.toFixed(2)}%` : `${spread.toFixed(2)}%`}
              </span>
              {isInverted ? (
                <AlertTriangle className="w-3.5 h-3.5 text-amber-400 animate-bounce" />
              ) : (
                <TrendingUp className="w-3.5 h-3.5 text-emerald-400" />
              )}
            </div>
          </div>
        </div>

        {/* Live Interactive SVG Chart Area */}
        <div className="relative bg-[#07090e] p-3 rounded-lg border border-slate-800 overflow-hidden">
          <div className="flex items-center justify-between text-[10px] text-slate-400 mb-2 font-bold uppercase">
            <span className="flex items-center gap-3">
              <span className="flex items-center gap-1 text-cyan-400">
                <span className="h-2 w-2 rounded-full bg-cyan-400" /> 10Y Yield
              </span>
              <span className="flex items-center gap-1 text-purple-400">
                <span className="h-2 w-2 rounded-full bg-purple-400" /> 2Y Yield
              </span>
            </span>
            <span>
              {activeHoverPoint
                ? `HOVER: 10Y=${activeHoverPoint.y10.toFixed(2)}% | 2Y=${activeHoverPoint.y2.toFixed(2)}% | SPREAD=${activeHoverPoint.spread.toFixed(2)}%`
                : 'REAL-TIME 1-MIN INTRADAY BARS'}
            </span>
          </div>

          <svg viewBox="0 0 600 180" className="w-full h-44 overflow-visible">
            {/* Background Grid Lines */}
            <line x1="0" y1="45" x2="600" y2="45" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />
            <line x1="0" y1="90" x2="600" y2="90" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />
            <line x1="0" y1="135" x2="600" y2="135" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />

            {/* Series Lines */}
            <path d={path10Y} fill="none" stroke="#00f2fe" strokeWidth="2.2" strokeLinecap="round" />
            <path d={path2Y} fill="none" stroke="#c4b5fd" strokeWidth="2.2" strokeLinecap="round" />

            {/* Hover Interaction Vertical Line */}
            {hoverIndex !== null && (
              <line
                x1={(hoverIndex / Math.max(1, pointsCount - 1)) * 600}
                y1="0"
                x2={(hoverIndex / Math.max(1, pointsCount - 1)) * 600}
                y2="180"
                stroke="#f59e0b"
                strokeWidth="1.2"
                strokeDasharray="2 2"
              />
            )}

            {/* Invisible Hover Hitboxes */}
            {pointsData.map((_, idx) => (
              <rect
                key={idx}
                x={(idx / Math.max(1, pointsCount - 1)) * 600 - 5}
                y="0"
                width="10"
                height="180"
                fill="transparent"
                onMouseEnter={() => setHoverIndex(idx)}
                onMouseLeave={() => setHoverIndex(null)}
                className="cursor-pointer"
              />
            ))}
          </svg>
        </div>
      </div>
    </Card>
  );
}
