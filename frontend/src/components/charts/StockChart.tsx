'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { TrendingUp, TrendingDown, Activity, RefreshCw } from 'lucide-react';

interface SeriesPoint {
  time: string;
  price: number;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
}

interface StockChartProps {
  ticker: string;
  height?: number;
}

export function StockChart({ ticker, height = 240 }: StockChartProps) {
  const [hoverPoint, setHoverPoint] = useState<SeriesPoint | null>(null);

  const cleanTicker = (ticker || 'SPY').toUpperCase();
  const { data, mutate, isValidating } = useSWR<{ series?: Record<string, SeriesPoint[]> }>(
    `/radar/market-series?symbols=${cleanTicker}&limit=60`,
    fetcher,
    { refreshInterval: 3000 }
  );

  const rawSeries = data?.series?.[cleanTicker] || [];
  const hasData = rawSeries.length > 0;

  const currentPrice = rawSeries[rawSeries.length - 1]?.price ?? null;
  const startPrice = rawSeries[0]?.price ?? currentPrice;
  const priceChange = (currentPrice !== null && startPrice !== null) ? currentPrice - startPrice : 0;
  const priceChangePct = (startPrice && startPrice > 0) ? (priceChange / startPrice) * 100 : 0;
  const isPositive = priceChange >= 0;

  // Compute SVG dimensions
  const svgWidth = 600;
  const svgHeight = height;
  const padding = 20;

  const minPrice = hasData ? Math.min(...rawSeries.map(p => p.price)) * 0.998 : 0;
  const maxPrice = hasData ? Math.max(...rawSeries.map(p => p.price)) * 1.002 : 100;
  const priceRange = Math.max(0.0001, maxPrice - minPrice);

  const points = rawSeries.map((pt, i) => {
    const x = padding + (i / Math.max(1, rawSeries.length - 1)) * (svgWidth - padding * 2);
    const y = svgHeight - padding - ((pt.price - minPrice) / priceRange) * (svgHeight - padding * 2);
    return { x, y, pt };
  });

  const pathD = points.reduce((acc, p, i) => `${acc} ${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`, '');

  return (
    <Card
      title={`${cleanTicker} REAL-TIME EQUITIES CHART`}
      badge={
        hasData ? (
          <Badge variant={isPositive ? 'live' : 'anomaly'} pulse>
            {isPositive ? '+' : ''}{priceChangePct.toFixed(2)}%
          </Badge>
        ) : (
          <Badge variant="warning" pulse>
            AWAITING TELEMETRY...
          </Badge>
        )
      }
    >
      <div className="p-4 space-y-3 font-mono text-xs">
        {/* Metric Header */}
        <div className="flex items-center justify-between bg-[#080b12] p-3 rounded-lg border border-slate-800">
          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">{cleanTicker} LAST PRICE</span>
            <div className="flex items-center gap-2 mt-0.5">
              <span className={`font-extrabold text-lg ${isPositive ? 'text-emerald-400' : 'text-rose-400'}`}>
                {currentPrice !== null ? `$${currentPrice.toFixed(2)}` : 'AWAITING TICK...'}
              </span>
              {isPositive ? (
                <TrendingUp className="w-4 h-4 text-emerald-400 animate-pulse" />
              ) : (
                <TrendingDown className="w-4 h-4 text-rose-400 animate-pulse" />
              )}
            </div>
          </div>

          <div className="text-right">
            <span className="text-slate-400 block text-[10px] uppercase font-bold">24H RANGE</span>
            <span className="text-slate-300 font-bold text-xs">
              ${minPrice.toFixed(2)} - ${maxPrice.toFixed(2)}
            </span>
          </div>

          <button
            onClick={() => mutate()}
            className="p-1.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 transition"
            title="Refresh series"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isValidating ? 'animate-spin' : ''}`} />
          </button>
        </div>

        {/* SVG Sparkline */}
        <div className="relative bg-[#050810] p-2 rounded-lg border border-slate-800/80 overflow-hidden">
          {hasData ? (
            <svg
              viewBox={`0 0 ${svgWidth} ${svgHeight}`}
              className="w-full h-[180px] overflow-visible"
              preserveAspectRatio="none"
            >
              <defs>
                <linearGradient id={`grad-${cleanTicker}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={isPositive ? '#10b981' : '#f43f5e'} stopOpacity="0.3" />
                  <stop offset="100%" stopColor={isPositive ? '#10b981' : '#f43f5e'} stopOpacity="0.0" />
                </linearGradient>
              </defs>

              {/* Area fill */}
              <path
                d={`${pathD} L ${svgWidth - padding} ${svgHeight - padding} L ${padding} ${svgHeight - padding} Z`}
                fill={`url(#grad-${cleanTicker})`}
              />

              {/* Main Line */}
              <path
                d={pathD}
                fill="none"
                stroke={isPositive ? '#10b981' : '#f43f5e'}
                strokeWidth="2"
                strokeLinecap="round"
              />

              {/* Interactive Dots */}
              {points.map((p, idx) => (
                <circle
                  key={idx}
                  cx={p.x}
                  cy={p.y}
                  r={hoverPoint === p.pt ? 5 : 2}
                  fill={isPositive ? '#10b981' : '#f43f5e'}
                  className="cursor-pointer transition-all hover:r-6"
                  onMouseEnter={() => setHoverPoint(p.pt)}
                  onMouseLeave={() => setHoverPoint(null)}
                />
              ))}
            </svg>
          ) : (
            <div className="h-[180px] flex items-center justify-center text-slate-500">
              <Activity className="w-5 h-5 animate-spin mr-2 text-cyan-400" />
              <span>Fetching live FIX & REST market series for {cleanTicker}...</span>
            </div>
          )}

          {/* Hover Tooltip */}
          {hoverPoint && (
            <div className="absolute top-2 right-2 bg-slate-900/90 border border-cyan-500/40 text-cyan-300 p-2 rounded text-[11px] shadow-lg">
              <div className="font-bold">${hoverPoint.price.toFixed(2)}</div>
              <div className="text-[9px] text-slate-400">{new Date(hoverPoint.time).toLocaleTimeString()}</div>
            </div>
          )}
        </div>
      </div>
    </Card>
  );
}
