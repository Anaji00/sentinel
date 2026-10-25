'use client';

import React, { useState, useMemo } from 'react';
import useSWR from 'swr';
import { describeApiError } from '@/lib/api';
import { fetcher } from '@/lib/api';
import { Tabs } from '@/components/ui/Tabs';
import {
  Maximize2,
  X,
  TrendingUp,
  TrendingDown,
  Zap,
  Radio,
} from 'lucide-react';
import BondYieldsChart from '@/components/charts/BondYieldsChart';

interface SeriesPoint {
  timestamp: string;
  price: number;
  // Nullable, as the gateway has always been able to send it.
  //
  // /market/series answers a 2Y par yield and a cached quote with no volume at
  // all -- BondYieldsChart's own `SeriesPoint` has said `number | null` since
  // that was found -- and these two copies of the interface still declared it
  // required. Which is how a live tick with no volume ended up carrying the
  // literal 1000 instead: with the type insisting on a number, the `|| 1000`
  // that produced one looked correct.
  volume: number | null;
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
  badgeText: string;
}

const ASSET_REGISTRY: AssetConfig[] = [
  // Stock Futures
  {
    symbol: 'SPY',
    title: 'S&P 500',
    subtitle: 'SPY fund · the 500 largest US companies',
    category: 'futures',
    badgeText: 'Stocks',
  },
  {
    symbol: 'QQQ',
    title: 'Nasdaq 100',
    subtitle: 'QQQ fund · the 100 largest Nasdaq companies',
    category: 'futures',
    badgeText: 'Tech',
  },
  {
    symbol: 'DJI',
    title: 'Dow Jones',
    subtitle: '30 large, established US companies',
    category: 'futures',
    badgeText: 'Blue chips',
  },
  {
    symbol: 'VIX',
    title: 'Volatility (VIX)',
    subtitle: 'How big a swing options traders expect: the fear gauge',
    category: 'futures',
    badgeText: 'Volatility',
  },

  // Energy & Commodities
  {
    symbol: 'WTI',
    title: 'US crude oil (WTI)',
    subtitle: 'Dollars per barrel',
    category: 'commodities',
    badgeText: 'Energy',
  },
  {
    symbol: 'BRENT',
    title: 'Brent crude oil',
    subtitle: 'The world benchmark, dollars per barrel',
    category: 'commodities',
    badgeText: 'Energy',
  },
  {
    symbol: 'GLD',
    title: 'Gold',
    subtitle: 'GLD fund, backed by physical gold (price per share)',
    category: 'commodities',
    badgeText: 'Metals',
  },

  // Crypto Leaders
  {
    symbol: 'BTCUSD',
    title: 'Bitcoin',
    subtitle: 'In US dollars',
    category: 'crypto',
    badgeText: 'Crypto',
  },
  {
    symbol: 'ETHUSD',
    title: 'Ethereum',
    subtitle: 'In US dollars',
    category: 'crypto',
    badgeText: 'Crypto',
  },

  // Treasury Bonds
  {
    symbol: 'US30Y',
    title: '30-year Treasury yield',
    subtitle: 'What the US pays to borrow for 30 years',
    category: 'bonds',
    badgeText: 'Rates',
  },
  {
    symbol: 'US10Y',
    title: '10-year Treasury yield',
    subtitle: 'Sets mortgage and loan rates',
    category: 'bonds',
    badgeText: 'Rates',
  },
  {
    symbol: 'US02Y',
    title: '2-year Treasury yield',
    subtitle: 'Moves with Fed rate expectations',
    category: 'bonds',
    badgeText: 'Rates',
  },
  {
    symbol: 'TLT',
    title: 'Long-term Treasury bonds',
    subtitle: 'TLT fund · falls when long-term rates rise',
    category: 'bonds',
    badgeText: 'Bonds',
  },
];

// Helper SVG Sparkline Component for Multi-Asset Grid
function AssetSparklineCard({
  config,
  data,
  feedError,
  onExpand,
}: {
  config: AssetConfig;
  data: SeriesPoint[];
  /** Why there is no series, when the reason is that the request failed.
   *  An empty grid during an outage used to read "AWAITING BACKEND
   *  TELEMETRY / POLLING WEBSOCKET / REST FEED (3S)" -- a description of a
   *  healthy system, printed thirteen times over. */
  feedError?: unknown;
  onExpand: (config: AssetConfig) => void;
}) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  const hasData = data && data.length > 0;
  const latestPoint = hasData ? data[data.length - 1] : null;
  const firstPoint = hasData ? data[0] : null;

  const priceChange = latestPoint && firstPoint ? latestPoint.price - firstPoint.price : 0;
  const pctChange = firstPoint && firstPoint.price > 0 ? (priceChange / firstPoint.price) * 100 : 0;
  // The line takes the direction's colour, as every other price line here does:
  // a falling S&P drawn in the accent read as "selected", not "down".
  const trendColor = pctChange >= 0 ? PALETTE.positive : PALETTE.negative;
  // The newest point is often a live tick that carries no volume; the last
  // point that does is the figure worth showing, and none is "no figure", not 0.
  // Index and rate series report a volume of 0, which is "not reported", not "nothing traded".
  const lastVolume = hasData ? ([...data].reverse().find((d) => (d.volume ?? 0) > 0)?.volume ?? null) : null;

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

    const pStr = pts.reduce(
      (acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`,
      '',
    );
    return { pathStr: pStr, points: pts };
  }, [data, hasData]);

  const activeHover = hoverIndex !== null && points[hoverIndex] ? points[hoverIndex] : null;

  return (
    <div
      onClick={() => onExpand(config)}
      className="group relative bg-raised border border-line hover:border-line-accent rounded-2xl p-4 transition-all duration-200 cursor-pointer hover: overflow-hidden font-mono flex flex-col justify-between"
    >
      {/* Top Header & Expand Icon */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-ink">{config.symbol}</span>
            <span className="px-2 py-0.5 text-micro font-extrabold rounded bg-page border border-line text-ink-dim">
              {config.badgeText}
            </span>
          </div>
          <p className="text-micro text-ink-dim font-sans font-medium mt-0.5 line-clamp-1">
            {config.title}
          </p>
        </div>
        <button
          onClick={(e) => {
            e.stopPropagation();
            onExpand(config);
          }}
          className="p-1.5 rounded-lg bg-page/80 border border-line text-ink-dim group-hover:text-accent group-hover:border-line-accent transition-colors"
          title="Expand Full Screen"
        >
          <Maximize2 className="w-3.5 h-3.5" />
        </button>
      </div>

      {/* Price Banner */}
      <div className="flex items-baseline justify-between my-2">
        {hasData ? (
          <>
            <div className="text-xl font-extrabold text-ink">
              ${latestPoint?.price ? formatNumber(latestPoint.price, { decimals: 2 }) : '0.00'}
            </div>
            <div
              className={`text-xs font-black flex items-center gap-0.5 ${pctChange >= 0 ? 'text-positive' : 'text-negative'}`}
            >
              {pctChange >= 0 ? (
                <TrendingUp className="w-3 h-3" />
              ) : (
                <TrendingDown className="w-3 h-3" />
              )}
              {pctChange >= 0 ? '+' : ''}
              {pctChange.toFixed(2)}%
            </div>
          </>
        ) : (
          <div className="flex items-center gap-2 py-1">
            <span className="h-2 w-2 rounded-full bg-amber-400 animate-ping" />
            <span className="text-xs font-bold text-caution uppercase tracking-wider animate-pulse">
              Waiting for the stream…
            </span>
          </div>
        )}
      </div>

      {/* SVG Sparkline or Live Waiting State */}
      <div className="relative h-28 w-full bg-page rounded-xl border border-line/80 p-2 overflow-hidden flex items-center justify-center">
        {hasData ? (
          <svg
            viewBox="0 0 400 120"
            className="w-full h-full overflow-visible"
            onMouseLeave={() => setHoverIndex(null)}
          >
            <line
              x1="0"
              y1="30"
              x2="400"
              y2="30"
              stroke="#1e293b"
              strokeDasharray="3 3"
              strokeWidth="0.5"
            />
            <line
              x1="0"
              y1="60"
              x2="400"
              y2="60"
              stroke="#1e293b"
              strokeDasharray="3 3"
              strokeWidth="0.5"
            />
            <line
              x1="0"
              y1="90"
              x2="400"
              y2="90"
              stroke="#1e293b"
              strokeDasharray="3 3"
              strokeWidth="0.5"
            />

            {pathStr && (
              <path
                d={pathStr}
                fill="none"
                stroke={trendColor}
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
                fill={hoverIndex === idx ? '#ffffff' : trendColor}
                className="cursor-pointer transition-all"
                onMouseEnter={() => setHoverIndex(idx)}
              />
            ))}
          </svg>
        ) : (
          <div className="flex flex-col items-center justify-center gap-1.5 text-center p-2">
            <Radio className="w-5 h-5 text-caution/80 animate-pulse" />
            <span className="text-micro text-ink-dim font-bold uppercase tracking-wider">
              {describeApiError(feedError) ?? 'Waiting for the backend'}
            </span>
            <span className="text-micro text-ink-mute">
              {feedError
                ? 'THE SERIES REQUEST DID NOT SUCCEED'
                : 'POLLING WEBSOCKET / REST FEED (3S)'}
            </span>
          </div>
        )}
      </div>

      {/* Footer Details */}
      <div className="flex items-center justify-between text-micro text-ink-dim pt-2 border-t border-line/60 mt-2">
        {hasData ? (
          <>
            <span>
              Vol: <span className="text-ink font-bold">{formatCompact(lastVolume)}</span>
            </span>
            {activeHover ? (
              <span className="text-accent font-bold">${activeHover.price.toFixed(2)}</span>
            ) : (
              <span className="text-ink-mute">Click to expand</span>
            )}
          </>
        ) : (
          <span className="text-caution/90 font-bold uppercase tracking-widest text-micro flex items-center gap-1">
            <span className="h-1.5 w-1.5 rounded-full bg-amber-400 animate-ping" />
            No recent events stored
          </span>
        )}
      </div>
    </div>
  );
}

import { useLiveEvents } from '@/lib/useLiveEvents';
import { PALETTE } from '@/lib/palette';
import { formatCompact, formatNumber } from '@/lib/format';
import { useDialog } from '@/components/ui/useDialog';
import { POLL } from '@/components/ui/DataProvider';

/** Every market's price chart, by category: the Prices view of /charts. */
export default function MarketCharts() {
  const [activeTab, setActiveTab] = useState<string>('all');
  const [expandedAsset, setExpandedAsset] = useState<AssetConfig | null>(null);

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(Boolean(expandedAsset), () => setExpandedAsset(null), 'Asset detail');
  const [timeframe, setTimeframe] = useState<string>('1D');

  // Real-time WebSocket live events (works with backend WS or Coinbase Direct Exchange WS fallback when backend is offline)
  const liveEvents = useLiveEvents('all');

  // Live polling every 3 seconds from backend market-series route
  const { data: marketData, error: marketError } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=SPY,QQQ,DJI,VIX,WTI,BRENT,BTCUSD,ETHUSD,TLT,US30Y,US10Y,US02Y,GLD&limit=60',
    fetcher,
    { refreshInterval: POLL.live },
  );

  const baseSeriesMap = useMemo(() => marketData?.series || {}, [marketData]);

  // Merge real-time WebSocket ticks into series map dynamically
  const seriesMap = useMemo(() => {
    const map: Record<string, SeriesPoint[]> = {};

    // 1. Base series from fetcher / public APIs
    Object.keys(baseSeriesMap).forEach((key) => {
      map[key] = [...(baseSeriesMap[key] || [])];
    });

    // 2. Real-time WebSocket ticks
    liveEvents.forEach((e) => {
      const rawSym = (
        e.crypto_data?.pair ||
        e.financial_data?.ticker ||
        e.primary_entity?.id ||
        e.primary_entity?.name ||
        ''
      )
        .toUpperCase()
        .replace('-', '');
      const price =
        e.crypto_data?.price || e.financial_data?.underlying_price || e.financial_data?.close_price;

      if (rawSym && price && price > 0) {
        const canonicalKeys = [rawSym];
        if (rawSym.includes('BTC')) canonicalKeys.push('BTCUSD', 'BTCUSDT', 'BTC');
        if (rawSym.includes('ETH')) canonicalKeys.push('ETHUSD', 'ETHUSDT', 'ETH');
        if (rawSym.includes('30') || rawSym.includes('TYX'))
          canonicalKeys.push('US30Y', 'US30', '30YR');
        if (rawSym.includes('2') || rawSym.includes('2YY') || rawSym.includes('SHY'))
          canonicalKeys.push('US02Y', 'US2Y', '2YR');

        canonicalKeys.forEach((k) => {
          if (!map[k]) map[k] = [];
          const lastPt = map[k][map[k].length - 1];
          if (!lastPt || lastPt.timestamp !== e.occurred_at) {
            map[k] = [
              ...map[k],
              {
                timestamp: e.occurred_at,
                price: price,
                // `crypto_data.volume` has never existed; the payload carries
                // `size_tokens`. With the first term permanently undefined and
                // `financial_data` absent on a crypto tick, every live point on
                // this chart reported a volume of exactly 1000.
                volume: e.crypto_data?.size_tokens ?? e.financial_data?.volume ?? null,
                anomaly_score: e.anomaly_score ?? 0,
              },
            ].slice(-60);
          }
        });
      }
    });

    return map;
  }, [baseSeriesMap, liveEvents]);

  const categoryTabs = [
    { id: 'all', label: 'Everything' },
    { id: 'futures', label: 'Stocks' },
    { id: 'commodities', label: 'Oil & gold' },
    { id: 'crypto', label: 'Crypto' },
    { id: 'bonds', label: 'Bonds' },
  ];

  const filteredRegistry = useMemo(() => {
    if (activeTab === 'all') return ASSET_REGISTRY;
    return ASSET_REGISTRY.filter((a) => a.category === activeTab);
  }, [activeTab]);

  const expandedSeries = useMemo(
    () => (expandedAsset ? seriesMap[expandedAsset.symbol] || [] : []),
    [expandedAsset, seriesMap],
  );
  const hasExpandedData = expandedSeries.length > 0;
  const expandedLatest = hasExpandedData ? expandedSeries[expandedSeries.length - 1] : null;
  const expandedFirst = hasExpandedData ? expandedSeries[0] : null;
  const expandedPriceChange =
    expandedLatest && expandedFirst ? expandedLatest.price - expandedFirst.price : 0;
  const expandedPctChange =
    expandedFirst && expandedFirst.price > 0
      ? (expandedPriceChange / expandedFirst.price) * 100
      : 0;

  // Max/Min prices & SMA overlays for expanded chart
  const {
    maxPrice,
    minPrice,
    expandedSvgPath,
    volumeBars,
    sma20Path,
    sma50Path,
    sma200Path,
    smaMetrics,
  } = useMemo(() => {
    if (!hasExpandedData) {
      return {
        maxPrice: 0,
        minPrice: 0,
        expandedSvgPath: '',
        volumeBars: [],
        sma20Path: '',
        sma50Path: '',
        sma200Path: '',
        smaMetrics: {
          sma20: 0,
          sma50: 0,
          sma200: 0,
          dist20: 0,
          dist50: 0,
          dist200: 0,
          alignment: 'NEUTRAL',
        },
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

    const path = pts.reduce(
      (acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`,
      '',
    );

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
      ptsArr.reduce(
        (acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`,
        '',
      );

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
    if (latestPrice > latest20 && latest20 > latest50 && latest50 > latest200)
      alignment = 'BULLISH_STACK';
    else if (latestPrice < latest20 && latest20 < latest50 && latest50 < latest200)
      alignment = 'BEARISH_STACK';
    else if (latestPrice > latest20 && latestPrice > latest50) alignment = 'BULLISH_CROSS';
    else if (latestPrice < latest20 && latestPrice < latest50) alignment = 'BEARISH_CROSS';

    const maxVol = Math.max(...expandedSeries.map((s) => s.volume ?? 0)) || 1;
    const bars = pts.map((pt) => ({
      x: pt.x,
      // No volume draws no bar, rather than borrowing the scale's own zero.
      h: ((pt.vol ?? 0) / maxVol) * 60,
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
    <div className="flex w-full flex-col gap-5 text-ink">
      <Tabs className="self-start" tabs={categoryTabs} activeTab={activeTab} onChange={setActiveTab} />

      {/* The yield curve leads the Economy view; here it shows under Bonds
          only, so the two views do not open on the same panel. */}
      {activeTab === 'bonds' && (
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
            feedError={marketError}
            onExpand={(config) => setExpandedAsset(config)}
          />
        ))}
      </div>

      {/* CLICK-TO-EXPAND LARGEST COMPONENT MODAL VIEW */}
      {expandedAsset && (
        <div
          className="fixed inset-0 z-50 bg-black/85 flex items-center justify-center p-4 sm:p-8"
          {...dialog.overlayProps}
        >
          <div
            className="bg-inset border border-line-accent rounded-3xl max-w-6xl w-full h-[90vh] flex flex-col overflow-hidden text-xs text-ink"
            {...dialog.panelProps}
          >
            {/* Modal Header Bar */}
            <div className="flex items-center justify-between px-6 py-4 bg-raised border-b border-line">
              <div className="flex items-center gap-3">
                <div className="h-10 w-10 rounded-xl bg-accent-dim border border-line-accent flex items-center justify-center text-accent font-extrabold text-sm">
                  {expandedAsset.symbol.substring(0, 3)}
                </div>
                <div>
                  <div className="flex items-center gap-2">
                    <h2 className="text-lg font-black text-ink tracking-widest">
                      {expandedAsset.symbol} — {expandedAsset.title}
                    </h2>
                    <span className="px-2 py-0.5 rounded text-micro font-bold bg-accent-dim text-accent border border-line-accent">
                      {expandedAsset.badgeText}
                    </span>
                  </div>
                  <p className="text-xs text-ink-dim">{expandedAsset.subtitle}</p>
                </div>
              </div>

              <div className="flex items-center gap-3">
                {/* Timeframe Selector */}
                <div className="flex items-center gap-1 bg-page p-1 rounded-xl border border-line">
                  {['1H', '4H', '1D', '1W'].map((tf) => (
                    <button
                      key={tf}
                      onClick={() => setTimeframe(tf)}
                      className={`px-3 py-1 rounded-lg font-bold text-xs transition-colors cursor-pointer ${timeframe === tf ? 'bg-cyan-500 text-slate-950' : 'text-ink-dim hover:text-ink'}`}
                    >
                      {tf}
                    </button>
                  ))}
                </div>

                <button
                  onClick={() => setExpandedAsset(null)}
                  className="p-2 rounded-xl bg-raised border border-line text-ink-dim hover:text-ink hover:bg-overlay cursor-pointer transition-colors"
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
                    <div className="p-4 bg-raised border border-line rounded-2xl">
                      <span className="text-ink-dim text-micro font-bold uppercase">
                        Current Price
                      </span>
                      <div className="text-2xl font-black text-ink mt-1">
                        $
                        {expandedLatest?.price
                          ? formatNumber(expandedLatest.price, { decimals: 2 })
                          : '0.00'}
                      </div>
                    </div>

                    <div className="p-4 bg-raised border border-line rounded-2xl">
                      <span className="text-ink-dim text-micro font-bold uppercase">
                        Price Trajectory
                      </span>
                      <div
                        className={`text-xl font-extrabold mt-1 flex items-center gap-1 ${expandedPctChange >= 0 ? 'text-positive' : 'text-negative'}`}
                      >
                        {expandedPctChange >= 0 ? '+' : ''}
                        {expandedPctChange.toFixed(2)}%
                      </div>
                    </div>

                    <div className="p-4 bg-raised border border-line rounded-2xl">
                      <span className="text-ink-dim text-micro font-bold uppercase">
                        24H High / Low Range
                      </span>
                      <div className="text-sm font-bold text-accent mt-1">
                        H: ${maxPrice.toFixed(2)} | L: ${minPrice.toFixed(2)}
                      </div>
                    </div>

                    <div className="p-4 bg-raised border border-line rounded-2xl">
                      <span className="text-ink-dim text-micro font-bold uppercase">
                        Anomaly Risk Score
                      </span>
                      <div className="text-base font-extrabold text-accent mt-1">
                        {expandedLatest?.anomaly_score
                          ? expandedLatest.anomaly_score.toFixed(2)
                          : '0.00'}
                      </div>
                    </div>
                  </div>

                  {/* Moving Average Telemetry HUD */}
                  <div className="p-3 bg-raised border border-line rounded-2xl flex items-center justify-between text-micro">
                    <div className="flex items-center gap-2">
                      <span className="font-extrabold text-ink uppercase text-micro">
                        Ma alignment:
                      </span>
                      <span className="px-2 py-0.5 rounded text-micro font-extrabold bg-accent-dim text-accent border border-line-accent">
                        {smaMetrics.alignment}
                      </span>
                    </div>
                    <div className="flex items-center gap-4 text-micro">
                      <div className="flex items-center gap-1.5">
                        <span className="h-2 w-2 rounded-full bg-accent" />
                        <span className="text-ink-dim">SMA 20:</span>
                        <span className="text-ink font-bold">${smaMetrics.sma20}</span>
                        <span
                          className={`font-bold ${smaMetrics.dist20 >= 0 ? 'text-positive' : 'text-negative'}`}
                        >
                          ({smaMetrics.dist20 >= 0 ? '+' : ''}
                          {smaMetrics.dist20}%)
                        </span>
                      </div>
                      <div className="flex items-center gap-1.5">
                        <span className="h-2 w-2 rounded-full bg-info" />
                        <span className="text-ink-dim">SMA 50:</span>
                        <span className="text-ink font-bold">${smaMetrics.sma50}</span>
                        <span
                          className={`font-bold ${smaMetrics.dist50 >= 0 ? 'text-positive' : 'text-negative'}`}
                        >
                          ({smaMetrics.dist50 >= 0 ? '+' : ''}
                          {smaMetrics.dist50}%)
                        </span>
                      </div>
                      <div className="flex items-center gap-1.5">
                        <span className="h-2 w-2 rounded-full bg-positive" />
                        <span className="text-ink-dim">SMA 200:</span>
                        <span className="text-ink font-bold">${smaMetrics.sma200}</span>
                        <span
                          className={`font-bold ${smaMetrics.dist200 >= 0 ? 'text-positive' : 'text-negative'}`}
                        >
                          ({smaMetrics.dist200 >= 0 ? '+' : ''}
                          {smaMetrics.dist200}%)
                        </span>
                      </div>
                    </div>
                  </div>

                  {/* Large Interactive SVG Chart */}
                  <div className="bg-inset border border-line rounded-2xl p-4 relative h-[360px] flex flex-col justify-between overflow-hidden">
                    <div className="flex justify-between items-center text-micro text-ink-mute mb-2">
                      <span>MAX: ${maxPrice.toFixed(2)}</span>
                      <span>
                        TIME HORIZON: REAL-TIME {timeframe} INTRADAY WITH SMA 20/50/200 OVERLAYS
                      </span>
                      <span>MIN: ${minPrice.toFixed(2)}</span>
                    </div>

                    <div className="relative flex-1 w-full">
                      <svg viewBox="0 0 1000 300" className="w-full h-full overflow-visible">
                        {/* Horizontal Grid lines */}
                        <line
                          x1="0"
                          y1="75"
                          x2="1000"
                          y2="75"
                          stroke="#1e293b"
                          strokeDasharray="4 4"
                          strokeWidth="0.5"
                        />
                        <line
                          x1="0"
                          y1="150"
                          x2="1000"
                          y2="150"
                          stroke="#1e293b"
                          strokeDasharray="4 4"
                          strokeWidth="0.5"
                        />
                        <line
                          x1="0"
                          y1="225"
                          x2="1000"
                          y2="225"
                          stroke="#1e293b"
                          strokeDasharray="4 4"
                          strokeWidth="0.5"
                        />

                        {/* SMA 200 Overlay (Emerald) */}
                        {sma200Path && (
                          <path
                            d={sma200Path}
                            fill="none"
                            stroke={PALETTE.positive}
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
                            stroke={PALETTE.info}
                            strokeWidth="2.0"
                            opacity="0.85"
                          />
                        )}

                        {/* SMA 20 Overlay (Cyan) */}
                        {sma20Path && (
                          <path
                            d={sma20Path}
                            fill="none"
                            stroke={PALETTE.accent}
                            strokeWidth="2.0"
                            opacity="0.9"
                          />
                        )}

                        {/* Main Price Curve */}
                        {expandedSvgPath && (
                          <path
                            d={expandedSvgPath}
                            fill="none"
                            stroke={PALETTE.textPrimary}
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
                            fill={PALETTE.textMuted}
                            opacity="0.35"
                          />
                        ))}
                      </svg>
                    </div>
                  </div>
                </>
              ) : (
                <div className="bg-inset border border-dashed border-caution/30 rounded-3xl p-12 flex flex-col items-center justify-center text-center space-y-3">
                  <div className="h-12 w-12 rounded-2xl bg-caution/10 border border-caution/30 flex items-center justify-center">
                    <Radio className="w-6 h-6 text-caution animate-pulse" />
                  </div>
                  <h3 className="text-base font-semibold text-caution">
                    AWAITING LIVE DATA STREAM FOR {expandedAsset.symbol}
                  </h3>
                  <p className="text-xs text-ink-dim max-w-lg leading-relaxed font-sans">
                    No recent events currently recorded in TimescaleDB for ticker{' '}
                    <span className="text-caution font-bold">{expandedAsset.symbol}</span>.
                    Backend live WebSocket and REST pollers are active. Live ticks will render
                    automatically upon intake.
                  </p>
                </div>
              )}
            </div>

            {/* Modal Footer */}
            <div className="px-6 py-3 bg-raised border-t border-line flex justify-between items-center text-ink-dim text-xs">
              <div className="flex items-center gap-2">
                <Zap className="w-4 h-4 text-caution animate-pulse" />
                <span>Backend Communications & WebSockets Polling Active</span>
              </div>
              <button
                onClick={() => setExpandedAsset(null)}
                className="px-4 py-1.5 bg-raised text-ink rounded-xl font-bold border border-line-strong hover:bg-overlay cursor-pointer"
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
