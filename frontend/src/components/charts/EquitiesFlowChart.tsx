'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { TrendingUp, BarChart2 } from 'lucide-react';

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

  const { data } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=SPY,QQQ&limit=60',
    fetcher,
    { refreshInterval: 5000 }
  );

  const spySeries = data?.series?.['SPY'] || [];
  const qqqSeries = data?.series?.['QQQ'] || [];

  const latestSPY = spySeries[spySeries.length - 1]?.price || 545.20;
  const startSPY = spySeries[0]?.price || 542.00;
  const spyPct = ((latestSPY - startSPY) / (startSPY || 1)) * 100;

  const latestQQQ = qqqSeries[qqqSeries.length - 1]?.price || 478.60;
  const startQQQ = qqqSeries[0]?.price || 474.00;
  const qqqPct = ((latestQQQ - startQQQ) / (startQQQ || 1)) * 100;

  const pointsCount = Math.max(spySeries.length, qqqSeries.length, 30);

  // Normalize Relative Performance % Coordinates
  const { pathSPY, pathQQQ, pointsData } = useMemo(() => {
    const pSPY: { x: number; y: number }[] = [];
    const pQQQ: { x: number; y: number }[] = [];
    const combined: { time: string; spyPct: number; qqqPct: number }[] = [];

    const minPct = -1.5;
    const maxPct = 2.5;
    const height = 180;
    const width = 600;

    for (let i = 0; i < pointsCount; i++) {
      const x = (i / Math.max(1, pointsCount - 1)) * width;

      const pS = spySeries[i]?.price || 545;
      const pctS = ((pS - startSPY) / (startSPY || 1)) * 100;

      const pQ = qqqSeries[i]?.price || 478;
      const pctQ = ((pQ - startQQQ) / (startQQQ || 1)) * 100;

      const yS = height - ((pctS - minPct) / (maxPct - minPct)) * height;
      const yQ = height - ((pctQ - minPct) / (maxPct - minPct)) * height;

      pSPY.push({ x, y: yS });
      pQQQ.push({ x, y: yQ });
      combined.push({
        time: spySeries[i]?.timestamp || new Date().toISOString(),
        spyPct: pctS,
        qqqPct: pctQ,
      });
    }

    const pathSStr = pSPY.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
    const pathQStr = pQQQ.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');

    return { pathSPY: pathSStr, pathQQQ: pathQStr, pointsData: combined };
  }, [spySeries, qqqSeries, startSPY, startQQQ, pointsCount]);

  const activeHoverPoint = hoverIndex !== null && pointsData[hoverIndex] ? pointsData[hoverIndex] : null;

  return (
    <Card
      title="SPY / QQQ EQUITIES RELATIVE PERFORMANCE & VOLUME FLOW"
      badge={<Badge variant="live" pulse>ALPACA / SIP TELEMETRY</Badge>}
      noPadding
    >
      <div className="p-4 space-y-3 font-mono">
        {/* Metric Summary Ribbon */}
        <div className="grid grid-cols-3 gap-3 bg-[#080b12] p-3 rounded-lg border border-cyan-500/20 text-xs">
          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">SPY (S&P 500 ETF)</span>
            <div className="flex items-center gap-2 mt-0.5">
              <span className="text-cyan-400 font-extrabold text-base">${latestSPY.toFixed(2)}</span>
              <span className={`font-bold text-xs ${spyPct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {spyPct >= 0 ? `+${spyPct.toFixed(2)}%` : `${spyPct.toFixed(2)}%`}
              </span>
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">QQQ (NASDAQ 100 ETF)</span>
            <div className="flex items-center gap-2 mt-0.5">
              <span className="text-emerald-400 font-extrabold text-base">${latestQQQ.toFixed(2)}</span>
              <span className={`font-bold text-xs ${qqqPct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {qqqPct >= 0 ? `+${qqqPct.toFixed(2)}%` : `${qqqPct.toFixed(2)}%`}
              </span>
            </div>
          </div>

          <div>
            <span className="text-slate-400 block text-[10px] uppercase font-bold">SERIES SELECTOR</span>
            <div className="flex items-center gap-1 mt-1">
              {(['both', 'spy', 'qqq'] as const).map(mode => (
                <button
                  key={mode}
                  onClick={() => setActiveSeries(mode)}
                  className={`px-2 py-0.5 rounded text-[10px] uppercase font-bold transition-all cursor-pointer ${
                    activeSeries === mode
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

        {/* Live SVG Dual Line Area Chart */}
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
                ? `HOVER: SPY=${activeHoverPoint.spyPct.toFixed(2)}% | QQQ=${activeHoverPoint.qqqPct.toFixed(2)}%`
                : 'REAL-TIME INTRADAY VWAP TRAJECTORY'}
            </span>
          </div>

          <svg viewBox="0 0 600 180" className="w-full h-44 overflow-visible">
            {/* Zero Line & Gridlines */}
            <line x1="0" y1="90" x2="600" y2="90" stroke="#334155" strokeDasharray="4 4" strokeWidth="1" />
            <line x1="0" y1="45" x2="600" y2="45" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />
            <line x1="0" y1="135" x2="600" y2="135" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.8" />

            {/* SPY Series */}
            {(activeSeries === 'both' || activeSeries === 'spy') && (
              <path d={pathSPY} fill="none" stroke="#00f2fe" strokeWidth="2.2" strokeLinecap="round" />
            )}

            {/* QQQ Series */}
            {(activeSeries === 'both' || activeSeries === 'qqq') && (
              <path d={pathQQQ} fill="none" stroke="#10b981" strokeWidth="2.2" strokeLinecap="round" />
            )}

            {/* Hover Cursor Line */}
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

            {/* Hitboxes */}
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
