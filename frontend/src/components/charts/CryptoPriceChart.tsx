'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { useLiveEvents } from '../../lib/useLiveEvents';
import { Bitcoin, TrendingUp, Zap } from 'lucide-react';

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

export default function CryptoPriceChart() {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  // Real-time WebSocket stream ticks
  const liveCryptoEvents = useLiveEvents('crypto');

  const { data } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=BTCUSD&limit=60',
    fetcher,
    { refreshInterval: 4000 }
  );

  const basePoints = data?.series?.['BTCUSD'] || [];
  
  // Merge live ticks from WebSocket stream if available
  const mergedSeries = useMemo(() => {
    const list = [...basePoints];
    liveCryptoEvents.forEach(e => {
      const price = e.crypto_data?.price || e.financial_data?.current_price;
      if (price) {
        list.push({
          timestamp: e.occurred_at,
          price: price,
          volume: e.crypto_data?.volume || 1000,
          anomaly_score: e.anomaly_score
        });
      }
    });
    return list.slice(-60);
  }, [basePoints, liveCryptoEvents]);

  const latestPrice = mergedSeries[mergedSeries.length - 1]?.price || 67450.0;
  const startPrice = mergedSeries[0]?.price || 67000.0;
  const priceChange = latestPrice - startPrice;
  const priceChangePct = (priceChange / (startPrice || 1)) * 100;
  const isPositive = priceChange >= 0;

  const minPrice = useMemo(() => Math.min(...mergedSeries.map(p => p.price), 65000), [mergedSeries]);
  const maxPrice = useMemo(() => Math.max(...mergedSeries.map(p => p.price), 70000), [mergedSeries]);

  const { pathStr, areaStr } = useMemo(() => {
    const pts = mergedSeries.map((p, idx) => {
      const x = (idx / Math.max(1, mergedSeries.length - 1)) * 600;
      const y = 160 - ((p.price - minPrice) / Math.max(1, maxPrice - minPrice)) * 140;
      return { x, y };
    });

    const dPath = pts.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
    const dArea = `${dPath} L 600 180 L 0 180 Z`;
    return { pathStr: dPath, areaStr: dArea };
  }, [mergedSeries, minPrice, maxPrice]);

  const activeHoverPoint = hoverIndex !== null && mergedSeries[hoverIndex] ? mergedSeries[hoverIndex] : null;

  return (
    <Card
      title="BTC / USD REAL-TIME MARKET TELEMETRY & VOLATILITY"
      badge={<Badge variant="live" pulse>LIVE WS SYNC</Badge>}
      noPadding
    >
      <div className="p-4 space-y-3 font-mono">
        {/* Metric Summary Ribbon */}
        <div className="grid grid-cols-3 gap-3 bg-[#080b12] p-3 rounded-lg border border-amber-500/20 text-xs">
          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold flex items-center gap-1">
              <Bitcoin className="w-3.5 h-3.5 text-amber-400" /> BTC / USD PRICE
            </span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-amber-400 font-extrabold text-base">${latestPrice.toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>
              <Zap className="w-3.5 h-3.5 text-amber-400 animate-pulse" />
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">24H CHANGE</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className={`font-extrabold text-base ${isPositive ? 'text-emerald-400' : 'text-rose-400'}`}>
                {isPositive ? `+${priceChangePct.toFixed(2)}%` : `${priceChangePct.toFixed(2)}%`}
              </span>
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">INTRADAY RANGE</span>
            <div className="text-slate-200 font-bold text-xs mt-1">
              ${minPrice.toLocaleString()} - ${maxPrice.toLocaleString()}
            </div>
          </div>
        </div>

        {/* Live Interactive SVG Area Chart */}
        <div className="relative bg-[#07090e] p-3 rounded-lg border border-slate-800 overflow-hidden">
          <div className="flex items-center justify-between text-[10px] text-slate-400 mb-2 font-bold uppercase">
            <span className="text-amber-300 font-bold">BTC / USD LIVE CANDLE & STREAM TRAJECTORY</span>
            <span>
              {activeHoverPoint
                ? `HOVER: $${activeHoverPoint.price.toLocaleString()} | VOL: ${activeHoverPoint.volume.toLocaleString()}`
                : 'REAL-TIME 1-SEC TELEMETRY BARS'}
            </span>
          </div>

          <svg viewBox="0 0 600 180" className="w-full h-44 overflow-visible">
            <defs>
              <linearGradient id="btcGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#f59e0b" stopOpacity="0.4" />
                <stop offset="100%" stopColor="#f59e0b" stopOpacity="0.0" />
              </linearGradient>
            </defs>

            {/* Gridlines */}
            <line x1="0" y1="45" x2="600" y2="45" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />
            <line x1="0" y1="90" x2="600" y2="90" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />
            <line x1="0" y1="135" x2="600" y2="135" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />

            {/* Gradient Area & Stroke Line */}
            <path d={areaStr} fill="url(#btcGradient)" />
            <path d={pathStr} fill="none" stroke="#f59e0b" strokeWidth="2.2" strokeLinecap="round" />

            {/* Hover Interaction Cursor Line */}
            {hoverIndex !== null && (
              <line
                x1={(hoverIndex / Math.max(1, mergedSeries.length - 1)) * 600}
                y1="0"
                x2={(hoverIndex / Math.max(1, mergedSeries.length - 1)) * 600}
                y2="180"
                stroke="#00f2fe"
                strokeWidth="1.2"
                strokeDasharray="2 2"
              />
            )}

            {/* Invisible Hitboxes */}
            {mergedSeries.map((_, idx) => (
              <rect
                key={idx}
                x={(idx / Math.max(1, mergedSeries.length - 1)) * 600 - 5}
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
