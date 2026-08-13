'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { TrendingUp, BarChart2, Radio } from 'lucide-react';

import { useLiveEvents } from '../../lib/useLiveEvents';

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

export default function EquitiesFlowChart() {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const [activeSeries, setActiveSeries] = useState<'both' | 'spy' | 'qqq'>('both');

  // Real-time WebSocket stream ticks
  const liveTradfiEvents = useLiveEvents('tradfi');

  const { data } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=SPY,QQQ&limit=60',
    fetcher,
    { refreshInterval: 3000 }
  );

  const baseSPY = data?.series?.['SPY'] || [];
  const baseQQQ = data?.series?.['QQQ'] || [];

  const spySeries = useMemo(() => {
    const list = [...baseSPY];
    liveTradfiEvents.forEach(e => {
      const sym = (e.financial_data?.ticker || e.primary_entity?.id || e.primary_entity?.name || '').toUpperCase();
      if (sym === 'SPY') {
        const p = e.financial_data?.current_price || e.financial_data?.underlying_price;
        if (p) list.push({ timestamp: e.occurred_at, price: p, volume: 1000, anomaly_score: e.anomaly_score });
      }
    });
    return list.slice(-60);
  }, [baseSPY, liveTradfiEvents]);

  const qqqSeries = useMemo(() => {
    const list = [...baseQQQ];
    liveTradfiEvents.forEach(e => {
      const sym = (e.financial_data?.ticker || e.primary_entity?.id || e.primary_entity?.name || '').toUpperCase();
      if (sym === 'QQQ') {
        const p = e.financial_data?.current_price || e.financial_data?.underlying_price;
        if (p) list.push({ timestamp: e.occurred_at, price: p, volume: 1000, anomaly_score: e.anomaly_score });
      }
    });
    return list.slice(-60);
  }, [baseQQQ, liveTradfiEvents]);

  const hasData = spySeries.length > 0 || qqqSeries.length > 0;

  const latestSPY = spySeries[spySeries.length - 1]?.price ?? null;
  const startSPY = spySeries[0]?.price ?? null;
  const spyPct = latestSPY !== null && startSPY !== null && startSPY > 0 ? ((latestSPY - startSPY) / startSPY) * 100 : null;

  const latestQQQ = qqqSeries[qqqSeries.length - 1]?.price ?? null;
  const startQQQ = qqqSeries[0]?.price ?? null;
  const qqqPct = latestQQQ !== null && startQQQ !== null && startQQQ > 0 ? ((latestQQQ - startQQQ) / startQQQ) * 100 : null;

  const pointsCount = Math.max(spySeries.length, qqqSeries.length);

  // Normalize Relative Performance % Coordinates
  const { pathSPY, pathQQQ, pointsData } = useMemo(() => {
    if (!hasData) {
      return { pathSPY: '', pathQQQ: '', pointsData: [] };
    }
    const pSPY: { x: number; y: number }[] = [];
    const pQQQ: { x: number; y: number }[] = [];
    const combined: { time: string; spyPct: number | null; qqqPct: number | null }[] = [];

    const minPct = -2.0;
    const maxPct = 2.5;
    const height = 180;
    const width = 600;

    for (let i = 0; i < pointsCount; i++) {
      const x = (i / Math.max(1, pointsCount - 1)) * width;

      const pS = spySeries[i]?.price ?? null;
      const pctS = pS !== null && startSPY !== null ? ((pS - startSPY) / (startSPY || 1)) * 100 : null;

      const pQ = qqqSeries[i]?.price ?? null;
      const pctQ = pQ !== null && startQQQ !== null ? ((pQ - startQQQ) / (startQQQ || 1)) * 100 : null;

      if (pctS !== null) {
        const yS = height - ((pctS - minPct) / (maxPct - minPct)) * height;
        pSPY.push({ x, y: yS });
      }

      if (pctQ !== null) {
        const yQ = height - ((pctQ - minPct) / (maxPct - minPct)) * height;
        pQQQ.push({ x, y: yQ });
      }

      combined.push({
        time: spySeries[i]?.timestamp || qqqSeries[i]?.timestamp || new Date().toISOString(),
        spyPct: pctS,
        qqqPct: pctQ,
      });
    }

    const pathSStr = pSPY.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
    const pathQStr = pQQQ.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');

    return { pathSPY: pathSStr, pathQQQ: pathQStr, pointsData: combined };
  }, [spySeries, qqqSeries, startSPY, startQQQ, pointsCount, hasData]);

  const activeHoverPoint = hoverIndex !== null && pointsData[hoverIndex] ? pointsData[hoverIndex] : null;

  return (
    <Card
      title="SPY / QQQ EQUITIES RELATIVE PERFORMANCE & VOLUME FLOW"
      badge={
        hasData ? (
          <Badge variant="live" pulse>ALPACA / SIP TELEMETRY</Badge>
        ) : (
          <Badge variant="warning" pulse>AWAITING LIVE DATA STREAM...</Badge>
        )
      }
      noPadding
    >
      <div className="p-4 space-y-3 font-mono">
        {/* Metric Summary Ribbon */}
        <div className="grid grid-cols-3 gap-3 bg-[#080b12] p-3 rounded-lg border border-cyan-500/20 text-xs">
          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">SPY (S&P 500 ETF)</span>
            <div className="flex items-center gap-2 mt-0.5">
              <span className="text-cyan-400 font-extrabold text-base">
                {latestSPY !== null ? `$${latestSPY.toFixed(2)}` : 'AWAITING FEED...'}
              </span>
              {spyPct !== null && (
                <span className={`font-bold text-xs ${spyPct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                  {spyPct >= 0 ? `+${spyPct.toFixed(2)}%` : `${spyPct.toFixed(2)}%`}
                </span>
              )}
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">QQQ (NASDAQ 100 ETF)</span>
            <div className="flex items-center gap-2 mt-0.5">
              <span className="text-emerald-400 font-extrabold text-base">
                {latestQQQ !== null ? `$${latestQQQ.toFixed(2)}` : 'AWAITING FEED...'}
              </span>
              {qqqPct !== null && (
                <span className={`font-bold text-xs ${qqqPct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                  {qqqPct >= 0 ? `+${qqqPct.toFixed(2)}%` : `${qqqPct.toFixed(2)}%`}
                </span>
              )}
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">SERIES SELECTOR</span>
            <div className="flex items-center gap-1 mt-1">
              {(['both', 'spy', 'qqq'] as const).map(mode => (
                <button
                  key={mode}
                  onClick={() => setActiveSeries(mode)}
                  className={`px-2 py-0.5 rounded text-[10px] uppercase font-bold transition-all cursor-pointer ${activeSeries === mode
                      ? 'bg-cyan-500/25 text-[#00f2fe] border border-cyan-500/50 glow-cyan'
                      : 'bg-slate-900 text-slate-400 hover:text-white border border-slate-800'
                    }`}
                >
                  {mode}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Live SVG Dual Line Area Chart or Awaiting Live Stream State */}
        {hasData ? (
          <div className="relative bg-[#07090e] p-3 rounded-lg border border-slate-800 overflow-hidden">
            <div className="flex items-center justify-between text-[10px] text-slate-400 mb-2 font-bold uppercase">
              <span className="flex items-center gap-3">
                <span className="flex items-center gap-1 text-cyan-400">
                  <span className="h-2 w-2 rounded-full bg-cyan-400" /> SPY RELATIVE %
                </span>
                <span className="flex items-center gap-1 text-emerald-400">
                  <span className="h-2 w-2 rounded-full bg-emerald-400" /> QQQ RELATIVE %
                </span>
              </span>
              <span>
                {activeHoverPoint
                  ? `HOVER: SPY=${activeHoverPoint.spyPct !== null ? activeHoverPoint.spyPct.toFixed(2) + '%' : 'N/A'} | QQQ=${activeHoverPoint.qqqPct !== null ? activeHoverPoint.qqqPct.toFixed(2) + '%' : 'N/A'}`
                  : 'REAL-TIME INTRADAY VWAP TRAJECTORY'}
              </span>
            </div>

            <svg viewBox="0 0 600 180" className="w-full h-44 overflow-visible">
              <line x1="0" y1="90" x2="600" y2="90" stroke="#334155" strokeDasharray="4 4" strokeWidth="1" />
              <line x1="0" y1="45" x2="600" y2="45" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />
              <line x1="0" y1="135" x2="600" y2="135" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />

              {(activeSeries === 'both' || activeSeries === 'spy') && pathSPY && (
                <path d={pathSPY} fill="none" stroke="#00f2fe" strokeWidth="2.2" strokeLinecap="round" />
              )}

              {(activeSeries === 'both' || activeSeries === 'qqq') && pathQQQ && (
                <path d={pathQQQ} fill="none" stroke="#10b981" strokeWidth="2.2" strokeLinecap="round" />
              )}

              {hoverIndex !== null && (
                <line
                  x1={(hoverIndex / Math.max(1, pointsCount - 1)) * 600}
                  y1="0"
                  x2={(hoverIndex / Math.max(1, pointsCount - 1)) * 600}
                  y2="180"
                  stroke="#a855f7"
                  strokeWidth="1.2"
                  strokeDasharray="2 2"
                />
              )}

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
        ) : (
          <div className="h-44 w-full bg-[#07090e] rounded-lg border border-dashed border-amber-500/30 flex flex-col items-center justify-center text-center p-4 space-y-2 font-mono">
            <Radio className="w-6 h-6 text-amber-400 animate-pulse" />
            <span className="text-xs font-bold text-amber-400 uppercase tracking-widest">
              AWAITING LIVE DATA STREAM FOR SPY / QQQ EQUITIES
            </span>
            <p className="text-[10px] text-slate-500 max-w-sm">
              Finnhub & Alpaca WebSocket stream connections active. Ticks will render automatically upon database ingestion.
            </p>
          </div>
        )}
      </div>
    </Card>
  );
}
