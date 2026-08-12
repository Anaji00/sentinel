'use client';

import React, { useState, useMemo } from 'react';
import useSWR from 'swr';
import { fetcher } from '@/lib/api';
import { Card } from '@/components/ui/Card';
import { Badge } from '@/components/ui/Badge';
import { Tabs } from '@/components/ui/Tabs';
import { Maximize2, X, TrendingUp, TrendingDown, Activity, AlertTriangle, Zap, Radio } from 'lucide-react';
import BondYieldsChart from '@/components/charts/BondYieldsChart';

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

interface AssetConfig {
  symbol: string;
  title: string;
  subtitle: string;
  category: 'futures' | 'commodities' | 'crypto' | 'bonds';
  color: string;
  badgeText: string;
}

const ASSET_REGISTRY: AssetConfig[] = [
  // Stock Futures
  { symbol: 'SPY', title: 'S&P 500 ETF TRUST', subtitle: 'US Broad Market Benchmark Futures', category: 'futures', color: '#00f2fe', badgeText: 'STOCK FUTURES' },
  { symbol: 'QQQ', title: 'NASDAQ 100 ETF TRUST', subtitle: 'US Mega-Cap Tech Benchmark Futures', category: 'futures', color: '#a855f7', badgeText: 'TECH FUTURES' },
  { symbol: 'DJI', title: 'DOW JONES INDUSTRIAL', subtitle: 'US Blue-Chip Industrial Benchmark', category: 'futures', color: '#3b82f6', badgeText: 'DOW FUTURES' },
  { symbol: 'VIX', title: 'CBOE VOLATILITY INDEX', subtitle: 'Market Fear & Volatility Implied Index', category: 'futures', color: '#ef4444', badgeText: 'VOLATILITY' },

  // Energy & Commodities
  { symbol: 'WTI', title: 'WTI CRUDE OIL FUTURES', subtitle: 'US Light Sweet Crude ($/bbl)', category: 'commodities', color: '#f59e0b', badgeText: 'ENERGY FUTURES' },
  { symbol: 'BRENT', title: 'BRENT CRUDE OIL FUTURES', subtitle: 'Global Crude Oil Benchmark ($/bbl)', category: 'commodities', color: '#eab308', badgeText: 'GLOBAL ENERGY' },
  { symbol: 'GLD', title: 'SPDR GOLD SHARES', subtitle: 'Physical Gold Spot ETF ($/oz)', category: 'commodities', color: '#fbbf24', badgeText: 'PRECIOUS METALS' },

  // Crypto Leaders
  { symbol: 'BTCUSD', title: 'BITCOIN / USD PERPETUAL', subtitle: 'Crypto Market Leader Mark & Spot Price', category: 'crypto', color: '#f59e0b', badgeText: 'CRYPTO MARK' },
  { symbol: 'ETHUSD', title: 'ETHEREUM / USD PERPETUAL', subtitle: 'Smart Contract Platform Benchmark', category: 'crypto', color: '#6366f1', badgeText: 'CRYPTO MARK' },

  // Treasury Bonds
  { symbol: 'US30Y', title: '30Y TREASURY YIELD RATE', subtitle: 'US Long-Term Sovereign Debt Yield %', category: 'bonds', color: '#10b981', badgeText: 'LONG BOND RATE' },
  { symbol: 'US10Y', title: '10Y TREASURY YIELD RATE', subtitle: 'US Benchmark Sovereign Debt Yield %', category: 'bonds', color: '#00f2fe', badgeText: 'SOVEREIGN RATE' },
  { symbol: 'US02Y', title: '2Y TREASURY YIELD RATE', subtitle: 'US Short-Term Policy Yield %', category: 'bonds', color: '#a855f7', badgeText: 'POLICY RATE' },
  { symbol: 'TLT', title: 'iSHARES 20+ Y TREASURY BOND', subtitle: 'Long-Term Debt Duration ETF', category: 'bonds', color: '#14b8a6', badgeText: 'BOND ETF' },
];

// Helper SVG Sparkline Component for Multi-Asset Grid
function AssetSparklineCard({
  config,
  data,
  onExpand,
}: {
  config: AssetConfig;
  data: SeriesPoint[];
  onExpand: (config: AssetConfig) => void;
}) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  const hasData = data && data.length > 0;
  const latestPoint = hasData ? data[data.length - 1] : null;
  const firstPoint = hasData ? data[0] : null;

  const priceChange = latestPoint && firstPoint ? latestPoint.price - firstPoint.price : 0;
  const pctChange = firstPoint && firstPoint.price > 0 ? (priceChange / firstPoint.price) * 100 : 0;

  const { pathStr, points } = useMemo(() => {
    if (!hasData) {
      return { pathStr: '', points: [] };
    }
    const prices = data.map((d) => d.price);
    const minP = Math.min(...prices) * 0.998;
    const maxP = Math.max(...prices) * 1.002;
    const height = 120;
    const width = 400;

    const pts = data.map((d, i) => {
      const x = (i / Math.max(1, data.length - 1)) * width;
      const y = height - ((d.price - minP) / (maxP - minP || 1)) * height;
      return { x, y, price: d.price, time: d.timestamp, vol: d.volume, score: d.anomaly_score };
    });

    const pStr = pts.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
    return { pathStr: pStr, points: pts };
  }, [data, hasData]);

  const activeHover = hoverIndex !== null && points[hoverIndex] ? points[hoverIndex] : null;

  return (
    <div
      onClick={() => onExpand(config)}
      className="group relative bg-[#090d16] border border-slate-800 hover:border-cyan-500/50 rounded-2xl p-4 transition-all duration-200 cursor-pointer shadow-[0_4px_25px_rgba(0,0,0,0.5)] hover:shadow-[0_0_30px_rgba(0,242,254,0.15)] overflow-hidden font-mono flex flex-col justify-between"
    >
      {/* Top Header & Expand Icon */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-black text-white uppercase tracking-wider">{config.symbol}</span>
            <span className="px-2 py-0.5 text-[9px] font-extrabold rounded bg-slate-950 border border-slate-800 text-slate-300">
              {config.badgeText}
            </span>
          </div>
          <p className="text-[10px] text-slate-400 font-sans font-medium mt-0.5 line-clamp-1">{config.title}</p>
        </div>
        <button
          onClick={(e) => {
            e.stopPropagation();
            onExpand(config);
          }}
          className="p-1.5 rounded-lg bg-slate-950/80 border border-slate-800 text-slate-400 group-hover:text-cyan-400 group-hover:border-cyan-500/40 transition-colors"
          title="Expand Full Screen"
        >
          <Maximize2 className="w-3.5 h-3.5" />
        </button>
      </div>

      {/* Price Banner */}
      <div className="flex items-baseline justify-between my-2">
        {hasData ? (
          <>
            <div className="text-xl font-extrabold text-white">
              ${latestPoint?.price ? latestPoint.price.toLocaleString(undefined, { minimumFractionDigits: 2 }) : '0.00'}
            </div>
            <div className={`text-xs font-black flex items-center gap-0.5 ${pctChange >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
              {pctChange >= 0 ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
              {pctChange >= 0 ? '+' : ''}{pctChange.toFixed(2)}%
            </div>
          </>
        ) : (
          <div className="flex items-center gap-2 py-1">
            <span className="h-2 w-2 rounded-full bg-amber-400 animate-ping" />
            <span className="text-xs font-bold text-amber-400 uppercase tracking-wider animate-pulse">
              AWAITING LIVE STREAM...
            </span>
          </div>
        )}
      </div>

      {/* SVG Sparkline or Live Waiting State */}
      <div className="relative h-28 w-full bg-[#05070c] rounded-xl border border-slate-800/80 p-2 overflow-hidden flex items-center justify-center">
        {hasData ? (
          <svg
            viewBox="0 0 400 120"
            className="w-full h-full overflow-visible"
            onMouseLeave={() => setHoverIndex(null)}
          >
            <line x1="0" y1="30" x2="400" y2="30" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.5" />
            <line x1="0" y1="60" x2="400" y2="60" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.5" />
            <line x1="0" y1="90" x2="400" y2="90" stroke="#1e293b" strokeDasharray="3 3" strokeWidth="0.5" />

            {pathStr && (
              <path
                d={pathStr}
                fill="none"
                stroke={config.color}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            )}

            {points.map((pt, idx) => (
              <circle
                key={idx}
                cx={pt.x}
                cy={pt.y}
                r={hoverIndex === idx ? 5 : 2}
                fill={hoverIndex === idx ? '#ffffff' : config.color}
                className="cursor-pointer transition-all"
                onMouseEnter={() => setHoverIndex(idx)}
              />
            ))}
          </svg>
        ) : (
          <div className="flex flex-col items-center justify-center gap-1.5 text-center p-2">
            <Radio className="w-5 h-5 text-amber-400/80 animate-pulse" />
            <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">AWAITING BACKEND TELEMETRY</span>
            <span className="text-[9px] text-slate-600">POLLING WEBSOCKET / REST FEED (3S)</span>
          </div>
        )}
      </div>

      {/* Footer Details */}
      <div className="flex items-center justify-between text-[10px] text-slate-400 pt-2 border-t border-slate-800/60 mt-2">
        {hasData ? (
          <>
            <span>Vol: <span className="text-slate-200 font-bold">{(latestPoint?.volume || 0) / 1e3}k</span></span>
            {activeHover ? (
              <span className="text-cyan-300 font-bold">${activeHover.price.toFixed(2)}</span>
            ) : (
              <span className="text-slate-500">Click to expand</span>
            )}
          </>
        ) : (
          <span className="text-amber-400/90 font-bold uppercase tracking-widest text-[9px] flex items-center gap-1">
            <span className="h-1.5 w-1.5 rounded-full bg-amber-400 animate-ping" />
            NO RECENT EVENT ROW IN DB
          </span>
        )}
      </div>
    </div>
  );
}

export default function ChartsPage() {
  const [activeTab, setActiveTab] = useState<string>('all');
  const [expandedAsset, setExpandedAsset] = useState<AssetConfig | null>(null);
  const [timeframe, setTimeframe] = useState<string>('1D');

  // Live polling every 3 seconds from backend market-series route
  const { data: marketData } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=SPY,QQQ,DJI,VIX,WTI,BRENT,BTCUSD,ETHUSD,TLT,US30Y,US10Y,US02Y,GLD&limit=60',
    fetcher,
    { refreshInterval: 3000 }
  );

  const seriesMap = marketData?.series || {};

  const categoryTabs = [
    { id: 'all', label: 'ALL MARKET FEEDS' },
    { id: 'futures', label: 'STOCK FUTURES (SPY, QQQ, DJI, VIX)' },
    { id: 'commodities', label: 'ENERGY & COMMODITIES (WTI, BRENT, GLD)' },
    { id: 'crypto', label: 'CRYPTO MARK (BTC, ETH)' },
    { id: 'bonds', label: 'TREASURY BONDS (30Y, 10Y, 2Y, TLT)' },
  ];

  const filteredRegistry = useMemo(() => {
    if (activeTab === 'all') return ASSET_REGISTRY;
    return ASSET_REGISTRY.filter((a) => a.category === activeTab);
  }, [activeTab]);

  const expandedSeries = expandedAsset ? seriesMap[expandedAsset.symbol] || [] : [];
  const hasExpandedData = expandedSeries.length > 0;
  const expandedLatest = hasExpandedData ? expandedSeries[expandedSeries.length - 1] : null;
  const expandedFirst = hasExpandedData ? expandedSeries[0] : null;
  const expandedPriceChange = expandedLatest && expandedFirst ? expandedLatest.price - expandedFirst.price : 0;
  const expandedPctChange = expandedFirst && expandedFirst.price > 0 ? (expandedPriceChange / expandedFirst.price) * 100 : 0;

  // Max/Min prices & SMA overlays for expanded chart
  const { maxPrice, minPrice, expandedSvgPath, volumeBars, sma20Path, sma50Path, sma200Path, smaMetrics } = useMemo(() => {
    if (!hasExpandedData) {
      return {
        maxPrice: 0, minPrice: 0, expandedSvgPath: '', volumeBars: [],
        sma20Path: '', sma50Path: '', sma200Path: '',
        smaMetrics: { sma20: 0, sma50: 0, sma200: 0, dist20: 0, dist50: 0, dist200: 0, alignment: 'NEUTRAL' }
      };
    }
    const prices = expandedSeries.map((s) => s.price);
    const n = prices.length;
    const maxP = Math.max(...prices) * 1.005;
    const minP = Math.min(...prices) * 0.995;
    const height = 300;
    const width = 1000;

    const pts = expandedSeries.map((d, i) => {
      const x = (i / Math.max(1, n - 1)) * width;
      const y = height - ((d.price - minP) / (maxP - minP || 1)) * height;
      return { x, y, vol: d.volume, price: d.price };
    });

    const path = pts.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');

    // Compute rolling SMAs (using period 20, 50, 200 or proportionally scaled for shorter series)
    const p20 = Math.max(3, Math.min(20, Math.floor(n * 0.2)));
    const p50 = Math.max(5, Math.min(50, Math.floor(n * 0.4)));
    const p200 = Math.max(10, Math.min(200, Math.floor(n * 0.8)));

    const getSmaPts = (period: number) => {
      const smaPts: { x: number; y: number; val: number }[] = [];
      for (let i = 0; i < n; i++) {
        if (i < period - 1) continue;
        const windowSlice = prices.slice(i - period + 1, i + 1);
        const avg = windowSlice.reduce((a, b) => a + b, 0) / period;
        const x = (i / Math.max(1, n - 1)) * width;
        const y = height - ((avg - minP) / (maxP - minP || 1)) * height;
        smaPts.push({ x, y, val: avg });
      }
      return smaPts;
    };

    const sma20Pts = getSmaPts(p20);
    const sma50Pts = getSmaPts(p50);
    const sma200Pts = getSmaPts(p200);

    const buildPath = (ptsArr: { x: number; y: number }[]) =>
      ptsArr.reduce((acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');

    const sma20PathStr = buildPath(sma20Pts);
    const sma50PathStr = buildPath(sma50Pts);
    const sma200PathStr = buildPath(sma200Pts);

    const latestPrice = prices[n - 1] || 1;
    const latest20 = sma20Pts.length > 0 ? sma20Pts[sma20Pts.length - 1].val : latestPrice;
    const latest50 = sma50Pts.length > 0 ? sma50Pts[sma50Pts.length - 1].val : latestPrice;
    const latest200 = sma200Pts.length > 0 ? sma200Pts[sma200Pts.length - 1].val : latestPrice;

    const dist20 = Number((((latestPrice - latest20) / latest20) * 100).toFixed(2));
    const dist50 = Number((((latestPrice - latest50) / latest50) * 100).toFixed(2));
    const dist200 = Number((((latestPrice - latest200) / latest200) * 100).toFixed(2));

    let alignment = 'NEUTRAL';
    if (latestPrice > latest20 && latest20 > latest50 && latest50 > latest200) alignment = 'BULLISH_STACK';
    else if (latestPrice < latest20 && latest20 < latest50 && latest50 < latest200) alignment = 'BEARISH_STACK';
    else if (latestPrice > latest20 && latestPrice > latest50) alignment = 'BULLISH_CROSS';
    else if (latestPrice < latest20 && latestPrice < latest50) alignment = 'BEARISH_CROSS';

    const maxVol = Math.max(...expandedSeries.map((s) => s.volume)) || 1;
    const bars = pts.map((pt) => ({
      x: pt.x,
      h: (pt.vol / maxVol) * 60,
    }));

    return {
      maxPrice: maxP,
      minPrice: minP,
      expandedSvgPath: path,
      volumeBars: bars,
      sma20Path: sma20PathStr,
      sma50Path: sma50PathStr,
      sma200Path: sma200PathStr,
      smaMetrics: {
        sma20: Number(latest20.toFixed(2)),
        sma50: Number(latest50.toFixed(2)),
        sma200: Number(latest200.toFixed(2)),
        dist20,
        dist50,
        dist200,
        alignment,
      },
    };
  }, [expandedSeries, hasExpandedData]);

  return (
    <div className="flex h-full w-full flex-col p-4 sm:p-6 gap-5 overflow-y-auto font-mono bg-[#06080d] text-slate-100">

      {/* Header & Live Stream Bar */}
      <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4 bg-[#080d1a] p-4 rounded-2xl border border-cyan-500/20 shadow-xl">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-xl font-black text-white uppercase tracking-widest flex items-center gap-2">
              REAL-TIME FINANCIAL MARKET CHARTS & FUTURES FEED
            </h1>
            <span className="px-2.5 py-0.5 rounded text-[10px] font-mono font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/40 animate-pulse">
              LIVE 3S SYNC
            </span>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            STOCK FUTURES (SPY, QQQ, DJI, VIX), CRUDE OIL (WTI, BRENT), CRYPTO & SOVEREIGN YIELDS
          </p>
        </div>

        <Tabs
          tabs={categoryTabs}
          activeTab={activeTab}
          onChange={setActiveTab}
        />
      </div>

      {/* Main Treasury Yield Curve Section */}
      {(activeTab === 'all' || activeTab === 'bonds') && (
        <div className="w-full">
          <BondYieldsChart />
        </div>
      )}

      {/* Multi-Asset Sparklines Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
        {filteredRegistry.map((asset) => (
          <AssetSparklineCard
            key={asset.symbol}
            config={asset}
            data={seriesMap[asset.symbol] || []}
            onExpand={(config) => setExpandedAsset(config)}
          />
        ))}
      </div>

      {/* CLICK-TO-EXPAND LARGEST COMPONENT MODAL VIEW */}
      {expandedAsset && (
        <div className="fixed inset-0 z-50 bg-black/85 backdrop-blur-xl flex items-center justify-center p-4 sm:p-8 font-mono">
          <div className="bg-[#070a12] border border-cyan-500/50 rounded-3xl max-w-6xl w-full h-[90vh] flex flex-col overflow-hidden shadow-[0_0_60px_rgba(0,242,254,0.3)] text-xs text-slate-200">

            {/* Modal Header Bar */}
            <div className="flex items-center justify-between px-6 py-4 bg-[#0b0f1d] border-b border-cyan-500/30">
              <div className="flex items-center gap-3">
                <div className="h-10 w-10 rounded-xl bg-cyan-950 border border-cyan-500/50 flex items-center justify-center text-cyan-400 font-extrabold text-sm shadow-[0_0_15px_rgba(0,242,254,0.4)]">
                  {expandedAsset.symbol.substring(0, 3)}
                </div>
                <div>
                  <div className="flex items-center gap-2">
                    <h2 className="text-lg font-black text-white tracking-widest">{expandedAsset.symbol} — {expandedAsset.title}</h2>
                    <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-cyan-500/20 text-cyan-300 border border-cyan-500/40">
                      {expandedAsset.badgeText}
                    </span>
                  </div>
                  <p className="text-xs text-slate-400">{expandedAsset.subtitle}</p>
                </div>
              </div>

              <div className="flex items-center gap-3">
                {/* Timeframe Selector */}
                <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-xl border border-slate-800">
                  {['1H', '4H', '1D', '1W'].map((tf) => (
                    <button
                      key={tf}
                      onClick={() => setTimeframe(tf)}
                      className={`px-3 py-1 rounded-lg font-bold text-xs transition-colors cursor-pointer ${timeframe === tf ? 'bg-cyan-500 text-slate-950' : 'text-slate-400 hover:text-white'
                        }`}
                    >
                      {tf}
                    </button>
                  ))}
                </div>

                <button
                  onClick={() => setExpandedAsset(null)}
                  className="p-2 rounded-xl bg-slate-900 border border-slate-800 text-slate-400 hover:text-white hover:bg-slate-800 cursor-pointer transition-colors"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>
            </div>

            {/* Modal Content Body */}
            <div className="flex-1 p-6 space-y-4 overflow-y-auto">
              {hasExpandedData ? (
                <>
                  {/* Telemetry Summary Cards */}
                  <div className="grid grid-cols-4 gap-4">
                    <div className="p-4 bg-[#0b0f1d] border border-cyan-500/20 rounded-2xl">
                      <span className="text-slate-400 text-[10px] font-bold uppercase">Current Price</span>
                      <div className="text-2xl font-black text-white mt-1">
                        ${expandedLatest?.price ? expandedLatest.price.toLocaleString(undefined, { minimumFractionDigits: 2 }) : '0.00'}
                      </div>
                    </div>

                    <div className="p-4 bg-[#0b0f1d] border border-slate-800 rounded-2xl">
                      <span className="text-slate-400 text-[10px] font-bold uppercase">Price Trajectory</span>
                      <div className={`text-xl font-extrabold mt-1 flex items-center gap-1 ${expandedPctChange >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                        {expandedPctChange >= 0 ? '+' : ''}{expandedPctChange.toFixed(2)}%
                      </div>
                    </div>

                    <div className="p-4 bg-[#0b0f1d] border border-slate-800 rounded-2xl">
                      <span className="text-slate-400 text-[10px] font-bold uppercase">24H High / Low Range</span>
                      <div className="text-sm font-bold text-cyan-300 mt-1">
                        H: ${maxPrice.toFixed(2)} | L: ${minPrice.toFixed(2)}
                      </div>
                    </div>

                    <div className="p-4 bg-[#0b0f1d] border border-slate-800 rounded-2xl">
                      <span className="text-slate-400 text-[10px] font-bold uppercase">Anomaly Risk Score</span>
                      <div className="text-base font-extrabold text-purple-400 mt-1">
                        {expandedLatest?.anomaly_score ? expandedLatest.anomaly_score.toFixed(2) : '0.00'}
                      </div>
                    </div>
                  </div>

                  {/* Moving Average Telemetry HUD */}
                  <div className="p-3 bg-[#0b0f1d] border border-cyan-500/30 rounded-2xl flex items-center justify-between text-[11px]">
                    <div className="flex items-center gap-2">
                      <span className="font-extrabold text-white uppercase text-[10px]">MA ALIGNMENT:</span>
                      <span className="px-2 py-0.5 rounded text-[10px] font-extrabold bg-cyan-500/20 text-cyan-300 border border-cyan-500/40">
                        {smaMetrics.alignment}
                      </span>
                    </div>
                    <div className="flex items-center gap-4 text-[10px]">
                      <div className="flex items-center gap-1.5">
                        <span className="h-2 w-2 rounded-full bg-[#00f2fe]" />
                        <span className="text-slate-400">SMA 20:</span>
                        <span className="text-white font-bold">${smaMetrics.sma20}</span>
                        <span className={`font-bold ${smaMetrics.dist20 >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                          ({smaMetrics.dist20 >= 0 ? '+' : ''}{smaMetrics.dist20}%)
                        </span>
                      </div>
                      <div className="flex items-center gap-1.5">
                        <span className="h-2 w-2 rounded-full bg-[#a855f7]" />
                        <span className="text-slate-400">SMA 50:</span>
                        <span className="text-white font-bold">${smaMetrics.sma50}</span>
                        <span className={`font-bold ${smaMetrics.dist50 >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                          ({smaMetrics.dist50 >= 0 ? '+' : ''}{smaMetrics.dist50}%)
                        </span>
                      </div>
                      <div className="flex items-center gap-1.5">
                        <span className="h-2 w-2 rounded-full bg-[#10b981]" />
                        <span className="text-slate-400">SMA 200:</span>
                        <span className="text-white font-bold">${smaMetrics.sma200}</span>
                        <span className={`font-bold ${smaMetrics.dist200 >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                          ({smaMetrics.dist200 >= 0 ? '+' : ''}{smaMetrics.dist200}%)
                        </span>
                      </div>
                    </div>
                  </div>

                  {/* Large Interactive SVG Chart */}
                  <div className="bg-[#05070d] border border-slate-800 rounded-2xl p-4 relative h-[360px] flex flex-col justify-between overflow-hidden">
                    <div className="flex justify-between items-center text-[10px] text-slate-500 mb-2">
                      <span>MAX: ${maxPrice.toFixed(2)}</span>
                      <span>TIME HORIZON: REAL-TIME {timeframe} INTRADAY WITH SMA 20/50/200 OVERLAYS</span>
                      <span>MIN: ${minPrice.toFixed(2)}</span>
                    </div>

                    <div className="relative flex-1 w-full">
                      <svg viewBox="0 0 1000 300" className="w-full h-full overflow-visible">
                        {/* Horizontal Grid lines */}
                        <line x1="0" y1="75" x2="1000" y2="75" stroke="#1e293b" strokeDasharray="4 4" strokeWidth="0.5" />
                        <line x1="0" y1="150" x2="1000" y2="150" stroke="#1e293b" strokeDasharray="4 4" strokeWidth="0.5" />
                        <line x1="0" y1="225" x2="1000" y2="225" stroke="#1e293b" strokeDasharray="4 4" strokeWidth="0.5" />

                        {/* SMA 200 Overlay (Emerald) */}
                        {sma200Path && (
                          <path
                            d={sma200Path}
                            fill="none"
                            stroke="#10b981"
                            strokeWidth="1.5"
                            strokeDasharray="4 2"
                            opacity="0.8"
                          />
                        )}

                        {/* SMA 50 Overlay (Purple) */}
                        {sma50Path && (
                          <path
                            d={sma50Path}
                            fill="none"
                            stroke="#a855f7"
                            strokeWidth="2.0"
                            opacity="0.85"
                          />
                        )}

                        {/* SMA 20 Overlay (Cyan) */}
                        {sma20Path && (
                          <path
                            d={sma20Path}
                            fill="none"
                            stroke="#00f2fe"
                            strokeWidth="2.0"
                            opacity="0.9"
                          />
                        )}

                        {/* Main Price Curve */}
                        {expandedSvgPath && (
                          <path
                            d={expandedSvgPath}
                            fill="none"
                            stroke={expandedAsset.color}
                            strokeWidth="3.5"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                        )}

                        {/* Volume Bars */}
                        {volumeBars.map((bar, i) => (
                          <rect
                            key={i}
                            x={bar.x - 2}
                            y={300 - bar.h}
                            width="4"
                            height={bar.h}
                            fill={expandedAsset.color}
                            opacity="0.25"
                          />
                        ))}
                      </svg>
                    </div>
                  </div>
                </>
              ) : (
                <div className="bg-[#05070d] border border-dashed border-amber-500/40 rounded-3xl p-12 flex flex-col items-center justify-center text-center space-y-3 font-mono">
                  <div className="h-12 w-12 rounded-2xl bg-amber-950/60 border border-amber-500/50 flex items-center justify-center">
                    <Radio className="w-6 h-6 text-amber-400 animate-pulse" />
                  </div>
                  <h3 className="text-base font-black text-amber-400 uppercase tracking-widest">
                    AWAITING LIVE DATA STREAM FOR {expandedAsset.symbol}
                  </h3>
                  <p className="text-xs text-slate-400 max-w-lg leading-relaxed font-sans">
                    No recent events currently recorded in TimescaleDB for ticker <span className="text-amber-300 font-bold">{expandedAsset.symbol}</span>. Backend live WebSocket and REST pollers are active. Live ticks will render automatically upon intake.
                  </p>
                </div>
              )}
            </div>

            {/* Modal Footer */}
            <div className="px-6 py-3 bg-[#0b0f1d] border-t border-slate-800 flex justify-between items-center text-slate-400 text-xs">
              <div className="flex items-center gap-2">
                <Zap className="w-4 h-4 text-amber-400 animate-pulse" />
                <span>Backend Communications & WebSockets Polling Active</span>
              </div>
              <button
                onClick={() => setExpandedAsset(null)}
                className="px-4 py-1.5 bg-slate-900 text-white rounded-xl font-bold border border-slate-700 hover:bg-slate-800 cursor-pointer"
              >
                Close View
              </button>
            </div>

          </div>
        </div>
      )}

    </div>
  );
}
