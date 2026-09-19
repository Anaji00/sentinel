'use client';

import React, { useState, useMemo } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { TrendingUp, TrendingDown, Activity, RefreshCw } from 'lucide-react';
import { formatPercent } from '../../lib/format';
import { PALETTE } from '../../lib/palette';
import { ClockTime } from '../ui/ClockTime';
import { POLL } from '../ui/DataProvider';

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
    { refreshInterval: POLL.live },
  );

  const rawSeries =
    data?.series?.[cleanTicker] ||
    data?.series?.[cleanTicker.replace('USD', '')] ||
    data?.series?.[`${cleanTicker}USD`] ||
    data?.series?.[
      cleanTicker === '2YR' ? 'US02Y' : cleanTicker === '30YR' ? 'US30Y' : cleanTicker
    ] ||
    [];
  const hasData = rawSeries.length > 0;

  const currentPrice = rawSeries[rawSeries.length - 1]?.price ?? null;
  const startPrice = rawSeries[0]?.price ?? currentPrice;
  const priceChange = currentPrice !== null && startPrice !== null ? currentPrice - startPrice : 0;
  const priceChangePct = startPrice && startPrice > 0 ? (priceChange / startPrice) * 100 : 0;
  const isPositive = priceChange >= 0;

  // Compute SVG dimensions
  const svgWidth = 600;
  const svgHeight = height;
  const padding = 20;

  const minPrice = hasData ? Math.min(...rawSeries.map((p) => p.price)) * 0.998 : 0;
  const maxPrice = hasData ? Math.max(...rawSeries.map((p) => p.price)) * 1.002 : 100;
  const priceRange = Math.max(0.0001, maxPrice - minPrice);

  const { points, pathD } = useMemo(() => {
    if (!hasData) return { points: [], pathD: '' };
    const pts = rawSeries.map((pt, i) => {
      const x = padding + (i / Math.max(1, rawSeries.length - 1)) * (svgWidth - padding * 2);
      const y =
        svgHeight - padding - ((pt.price - minPrice) / priceRange) * (svgHeight - padding * 2);
      return { x, y, pt };
    });
    const d = pts.reduce(
      (acc, p, i) => `${acc} ${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`,
      '',
    );
    return { points: pts, pathD: d };
  }, [rawSeries, hasData, minPrice, priceRange, svgWidth, svgHeight, padding]);

  return (
    <Card
      title={`${cleanTicker} REAL-TIME EQUITIES CHART`}
      badge={
        hasData ? (
          <Badge variant={isPositive ? 'live' : 'anomaly'} pulse>
            {formatPercent(priceChangePct, { decimals: 2, signed: true })}
          </Badge>
        ) : (
          <Badge variant="warning" pulse>
            Waiting for data…
          </Badge>
        )
      }
    >
      <div className="p-4 space-y-3 text-xs">
        {/* Metric Header */}
        <div className="flex items-center justify-between bg-inset p-3 rounded-lg border border-line">
          <div>
            <span className="text-ink-dim block text-micro uppercase font-bold">
              {cleanTicker} LAST PRICE
            </span>
            <div className="flex items-center gap-2 mt-0.5">
              <span
                className={`font-extrabold text-lg ${isPositive ? 'text-emerald-400' : 'text-rose-400'}`}
              >
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
            <span className="text-ink-dim block text-micro uppercase font-bold">24H RANGE</span>
            <span className="text-ink-dim font-bold text-xs">
              ${minPrice.toFixed(2)} - ${maxPrice.toFixed(2)}
            </span>
          </div>

          <button
            onClick={() => mutate()}
            className="p-1.5 rounded bg-overlay hover:bg-slate-700 text-ink-dim transition"
            title="Refresh series"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isValidating ? 'animate-spin' : ''}`} />
          </button>
        </div>

        {/* SVG Sparkline */}
        <div className="relative bg-[#050810] p-2 rounded-lg border border-line/80 overflow-hidden">
          {hasData ? (
            <svg
              viewBox={`0 0 ${svgWidth} ${svgHeight}`}
              className="w-full h-[180px] overflow-visible"
              preserveAspectRatio="none"
            >
              <defs>
                <linearGradient id={`grad-${cleanTicker}`} x1="0" y1="0" x2="0" y2="1">
                  <stop
                    offset="0%"
                    stopColor={isPositive ? PALETTE.positive : PALETTE.negative}
                    stopOpacity="0.3"
                  />
                  <stop
                    offset="100%"
                    stopColor={isPositive ? PALETTE.positive : PALETTE.negative}
                    stopOpacity="0.0"
                  />
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
                stroke={isPositive ? PALETTE.positive : PALETTE.negative}
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
                  fill={isPositive ? PALETTE.positive : PALETTE.negative}
                  className="cursor-pointer transition-all hover:r-6"
                  onMouseEnter={() => setHoverPoint(p.pt)}
                  onMouseLeave={() => setHoverPoint(null)}
                />
              ))}
            </svg>
          ) : (
            <div className="h-[180px] flex items-center justify-center text-ink-mute">
              <Activity className="w-5 h-5 animate-spin mr-2 text-cyan-400" />
              <span>Fetching live FIX & REST market series for {cleanTicker}...</span>
            </div>
          )}

          {/* Hover Tooltip */}
          {hoverPoint && (
            <div className="absolute top-2 right-2 bg-raised/90 border border-cyan-500/40 text-cyan-300 p-2 rounded text-micro shadow-lg">
              <div className="font-bold">${hoverPoint.price.toFixed(2)}</div>
              <div className="text-micro text-ink-dim">
                <ClockTime value={hoverPoint.time} />
              </div>
            </div>
          )}
        </div>
      </div>
    </Card>
  );
}
