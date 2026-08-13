'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { useLiveEvents } from '../../lib/useLiveEvents';
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

  // Real-time WebSocket stream ticks
  const liveTradfiEvents = useLiveEvents('tradfi');

  const { data } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=US30Y,US10Y,US02Y,TLT,IEF&limit=50',
    fetcher,
    { refreshInterval: 3000 }
  );

  const base30Y = data?.series?.['US30Y'] || data?.series?.['US30'] || [];
  const base10Y = data?.series?.['US10Y'] || [];
  const base2Y = data?.series?.['US02Y'] || data?.series?.['US2Y'] || data?.series?.['2Y'] || [];

  // Merge live ticks from WebSocket stream
  const us30ySeries = useMemo(() => {
    const list = [...base30Y];
    liveTradfiEvents.forEach(e => {
      const sym = (e.financial_data?.ticker || e.primary_entity?.id || e.primary_entity?.name || '').toUpperCase();
      if (sym.includes('30') || sym.includes('TYX')) {
        const p = e.financial_data?.current_price || e.financial_data?.underlying_price;
        if (p) list.push({ timestamp: e.occurred_at, price: p, volume: 1000, anomaly_score: e.anomaly_score });
      }
    });
    return list.slice(-50);
  }, [base30Y, liveTradfiEvents]);

  const us10ySeries = useMemo(() => {
    const list = [...base10Y];
    liveTradfiEvents.forEach(e => {
      const sym = (e.financial_data?.ticker || e.primary_entity?.id || e.primary_entity?.name || '').toUpperCase();
      if (sym.includes('10') || sym.includes('TNX')) {
        const p = e.financial_data?.current_price || e.financial_data?.underlying_price;
        if (p) list.push({ timestamp: e.occurred_at, price: p, volume: 1000, anomaly_score: e.anomaly_score });
      }
    });
    return list.slice(-50);
  }, [base10Y, liveTradfiEvents]);

  const us02ySeries = useMemo(() => {
    const list = [...base2Y];
    liveTradfiEvents.forEach(e => {
      const sym = (e.financial_data?.ticker || e.primary_entity?.id || e.primary_entity?.name || '').toUpperCase();
      if (sym.includes('2') || sym.includes('2YY') || sym.includes('IRX')) {
        const p = e.financial_data?.current_price || e.financial_data?.underlying_price;
        if (p) list.push({ timestamp: e.occurred_at, price: p, volume: 1000, anomaly_score: e.anomaly_score });
      }
    });
    return list.slice(-50);
  }, [base2Y, liveTradfiEvents]);

  const hasData = us30ySeries.length > 0 || us10ySeries.length > 0 || us02ySeries.length > 0;

  const latest30Y = us30ySeries[us30ySeries.length - 1]?.price || null;
  const latest10Y = us10ySeries[us10ySeries.length - 1]?.price || null;
  const latest2Y = us02ySeries[us02ySeries.length - 1]?.price || null;
  
  const spread2Y10Y = latest10Y !== null && latest2Y !== null ? latest10Y - latest2Y : null;
  const spread10Y30Y = latest30Y !== null && latest10Y !== null ? latest30Y - latest10Y : null;
  const isInverted = spread2Y10Y !== null ? spread2Y10Y < 0 : false;

  const pointsCount = Math.max(us30ySeries.length, us10ySeries.length, us02ySeries.length);

  // Normalize SVG Path Coordinates for Real Ticks Only
  const { path30Y, path10Y, path2Y, pointsData } = useMemo(() => {
    if (!hasData) {
      return { path30Y: '', path10Y: '', path2Y: '', pointsData: [] };
    }
    const p30: { x: number; y: number }[] = [];
    const p10: { x: number; y: number }[] = [];
    const p2: { x: number; y: number }[] = [];
    const combined: { time: string; y30: number | null; y10: number | null; y2: number | null; spread: number | null }[] = [];

    const allPrices = [...us30ySeries.map(s => s.price), ...us10ySeries.map(s => s.price), ...us02ySeries.map(s => s.price)];
    const minVal = Math.min(...allPrices) * 0.98 || 3.80;
    const maxVal = Math.max(...allPrices) * 1.02 || 4.80;
    const height = 180;
    const width = 600;

    for (let i = 0; i < pointsCount; i++) {
      const x = (i / Math.max(1, pointsCount - 1)) * width;
      const v30 = us30ySeries[i]?.price ?? null;
      const v10 = us10ySeries[i]?.price ?? null;
      const v2 = us02ySeries[i]?.price ?? null;

      if (v30 !== null) {
        const y30 = height - ((v30 - minVal) / (maxVal - minVal || 1)) * height;
        p30.push({ x, y: y30 });
      }

      if (v10 !== null) {
        const y10 = height - ((v10 - minVal) / (maxVal - minVal || 1)) * height;
        p10.push({ x, y: y10 });
      }

      if (v2 !== null) {
        const y2 = height - ((v2 - minVal) / (maxVal - minVal || 1)) * height;
        p2.push({ x, y: y2 });
      }

      combined.push({
        time: us30ySeries[i]?.timestamp || us10ySeries[i]?.timestamp || us02ySeries[i]?.timestamp || new Date().toISOString(),
        y30: v30,
        y10: v10,
        y2: v2,
        spread: v10 !== null && v2 !== null ? v10 - v2 : null,
      });
    }

    const path30YStr = p30.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
    const path10YStr = p10.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
    const path2YStr = p2.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');

    return { path30Y: path30YStr, path10Y: path10YStr, path2Y: path2YStr, pointsData: combined };
  }, [us30ySeries, us10ySeries, us02ySeries, pointsCount, hasData]);

  return (
    <Card
      title="U.S. TREASURY BOND YIELD CURVE & SPREAD TELEMETRY (30Y, 10Y, 2Y)"
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
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 bg-[#080b12] p-3 rounded-lg border border-cyan-500/20 text-xs">
          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">30Y BOND YIELD (US30Y)</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-emerald-400 font-extrabold text-base">
                {latest30Y !== null ? `${latest30Y.toFixed(2)}%` : 'AWAITING FEED...'}
              </span>
              <Activity className="w-3.5 h-3.5 text-emerald-400 animate-pulse" />
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">10Y TREASURY YIELD (US10Y)</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-cyan-400 font-extrabold text-base">
                {latest10Y !== null ? `${latest10Y.toFixed(2)}%` : 'AWAITING FEED...'}
              </span>
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
              <span className={`font-extrabold text-base ${spread2Y10Y !== null && spread2Y10Y < 0 ? 'text-rose-400' : 'text-emerald-400'}`}>
                {spread2Y10Y !== null ? `${spread2Y10Y > 0 ? '+' : ''}${(spread2Y10Y * 100).toFixed(1)} bps` : 'AWAITING FEED...'}
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

              {/* 30Y Yield Curve (Emerald) */}
              {path30Y && (
                <path d={path30Y} fill="none" stroke="#10b981" strokeWidth="2.5" strokeLinecap="round" />
              )}
              {/* 10Y Yield Curve (Cyan) */}
              {path10Y && (
                <path d={path10Y} fill="none" stroke="#00f2fe" strokeWidth="2.5" strokeLinecap="round" />
              )}
              {/* 2Y Yield Curve (Purple) */}
              {path2Y && (
                <path d={path2Y} fill="none" stroke="#a855f7" strokeWidth="2.5" strokeLinecap="round" />
              )}
            </svg>
          </div>
        ) : (
          <div className="h-48 w-full bg-[#05070c] rounded-lg border border-dashed border-amber-500/30 flex flex-col items-center justify-center text-center p-4 space-y-2">
            <Radio className="w-6 h-6 text-amber-400 animate-pulse" />
            <span className="text-xs font-bold text-amber-400 uppercase tracking-widest">
              AWAITING LIVE DATA STREAM FOR TREASURY YIELDS (30Y, 10Y, 2Y)
            </span>
            <p className="text-[10px] text-slate-500 max-w-sm">
              Backend live WebSocket and REST pollers are active. Ticks will render automatically upon database ingestion.
            </p>
          </div>
        )}
      </div>
    </Card>
  );
}
