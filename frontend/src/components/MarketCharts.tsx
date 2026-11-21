'use client';

import React, { useState, useMemo } from 'react';
import useSWR from 'swr';
import { describeApiError } from '@/lib/api';
import { fetcher } from '@/lib/api';
import { Button } from '@/components/ui/Button';
import { flashClass, useFlash } from '@/components/ui/useFlash';
import {
  Maximize2,
  X,
  TrendingUp,
  TrendingDown,
} from 'lucide-react';
import BondYieldsChart from '@/components/charts/BondYieldsChart';
import CandleChart from '@/components/CandleChart';
import { Sparkline } from '@/components/ui/Sparkline';

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
  const flash = useFlash(latestPoint?.price);

  const priceChange = latestPoint && firstPoint ? latestPoint.price - firstPoint.price : 0;
  // A yield is a percentage, so it is shown as one and its move in points; an index is not money.
  const isYield = /^US\d\dY$/.test(config.symbol);
  const isIndex = config.symbol === 'VIX' || config.symbol === 'DJI';
  const windowHours = latestPoint && firstPoint ? (Date.parse(latestPoint.timestamp) - Date.parse(firstPoint.timestamp)) / 3_600_000 : 0;
  const windowLabel = windowHours >= 1 ? `change over the last ${Math.round(windowHours)} hours` : windowHours > 0 ? `change over the last ${Math.max(1, Math.round(windowHours * 60))} minutes` : '';
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
      className={`lift wash group relative bg-raised border border-line hover:border-line-accent rounded-md p-4 cursor-pointer overflow-hidden font-mono flex flex-col justify-between ${
        hasData ? (pctChange >= 0 ? 'text-positive' : 'text-negative') : ''
      }`}
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
            <div className={`text-lead font-bold leading-none text-ink px-1 -mx-1 ${flashClass(flash)}`}>
              {latestPoint?.price
                ? `${isYield || isIndex ? '' : '$'}${formatNumber(latestPoint.price, { decimals: 2 })}${isYield ? '%' : ''}`
                : '—'}
            </div>
            <div
              className={`flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-bold ${pctChange >= 0 ? 'bg-positive/15 text-positive' : 'bg-negative/15 text-negative'}`}
            >
              {pctChange >= 0 ? (
                <TrendingUp className="w-3 h-3" />
              ) : (
                <TrendingDown className="w-3 h-3" />
              )}
              {isYield
                ? `${priceChange >= 0 ? '+' : '−'}${Math.abs(priceChange).toFixed(2)} points`
                : `${pctChange >= 0 ? '+' : '−'}${Math.abs(pctChange).toFixed(2)}%`}
            </div>
          </>
        ) : (
          <div className="flex items-center gap-2 py-1">
            <span className="text-xs text-ink-mute">No prices yet</span>
          </div>
        )}
      </div>

      {/* SVG Sparkline or Live Waiting State */}
      <div className="relative h-28 w-full bg-page rounded-md border border-line/80 p-2 overflow-hidden flex items-center justify-center">
        {hasData ? (
          <svg
            viewBox="0 0 400 120"
            className="w-full h-full overflow-visible"
            onMouseLeave={() => setHoverIndex(null)}
          >
            <line
              x1="0"
              y1="60"
              x2="400"
              y2="60"
              stroke="var(--border-strong)"
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
                fill={hoverIndex === idx ? 'var(--text-primary)' : trendColor}
                className="cursor-pointer transition-all"
                onMouseEnter={() => setHoverIndex(idx)}
              />
            ))}
          </svg>
        ) : (
          <div className="flex flex-col items-center justify-center gap-1.5 text-center p-2">
            <span className="font-sans text-xs text-ink-dim">
              {describeApiError(feedError) ?? 'No prices are stored for this market yet.'}
            </span>
            <span className="font-sans text-micro text-ink-mute">
              {feedError ? 'The request for prices did not succeed.' : 'It fills in when the next price arrives.'}
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
              <span className="font-sans text-ink-mute">{windowLabel}</span>
            )}
          </>
        ) : (
          <span className="font-sans text-micro text-ink-mute">
            No recent prices
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
/** Markets with no stored bars of their own, and the fund that tracks each. The chart says it is showing the fund. */
const CANDLE_PROXY: Record<string, { ticker: string; note: string }> = {
  DJI: { ticker: 'DIA', note: 'The Dow itself has no stored bars. This is DIA, the fund that tracks it.' },
  BRENT: { ticker: 'BNO', note: 'Brent crude itself has no stored bars. This is BNO, a fund that tracks its price.' },
};

/** The other names the same live price is kept under, looked up by the exact symbol the tick carries. */
const LIVE_ALIASES: Record<string, string[]> = Object.fromEntries(
  [
    ['BTC', 'BTCUSD', 'BTCUSDT'],
    ['ETH', 'ETHUSD', 'ETHUSDT'],
    ['US30Y', 'US30', '30YR', 'TYX'],
    ['US02Y', 'US2Y', '2YR', 'SHY'],
  ].flatMap((group) => group.map((sym) => [sym, group.filter((g) => g !== sym)] as const)),
);

export default function MarketCharts() {
  const [activeTab, setActiveTab] = useState<string>('all');
  const [expandedAsset, setExpandedAsset] = useState<AssetConfig | null>(null);

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(Boolean(expandedAsset), () => setExpandedAsset(null), 'Asset detail');

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
        // Exact names only. These were substring tests, so any symbol containing a 2 or a 30 was drawn into the
        // two-year and thirty-year yield lines, and ETHE or BTCS into the Ethereum and Bitcoin lines.
        const canonicalKeys = [rawSym, ...(LIVE_ALIASES[rawSym] ?? [])];

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
  const expandedValues = expandedSeries.map((p) => p.price).filter((v) => Number.isFinite(v));

  return (
    <div className="flex w-full flex-col gap-5 text-ink">
      {/* A filter on the charts below, not a second navigation: quiet chips under the view tabs. */}
      <div role="group" aria-label="Show markets" className="flex flex-wrap items-center gap-1.5 font-sans">
        <span className="mr-1 text-micro uppercase tracking-[0.08em] text-ink-mute">Show</span>
        {categoryTabs.map((t) => (
          <Button
            key={t.id}
            variant={activeTab === t.id ? 'secondary' : 'outline'}
            size="sm"
            pressed={activeTab === t.id}
            onClick={() => setActiveTab(t.id)}
            className="rounded-full"
          >
            {t.label}
          </Button>
        ))}
      </div>

      {/* The yield curve leads the Economy view; here it shows under Bonds
          only, so the two views do not open on the same panel. */}
      {activeTab === 'bonds' && (
        <div className="w-full">
          <BondYieldsChart />
        </div>
      )}

      {/* Multi-Asset Sparklines Grid */}
      {/* Everything: thirteen look-alike cards read as one wall, so under "Everything" they sit in labelled groups. */}
      {(activeTab === 'all'
        ? categoryTabs.filter((t) => t.id !== 'all').map((t) => ({ title: t.label, assets: filteredRegistry.filter((a) => a.category === t.id) }))
        : [{ title: null as string | null, assets: filteredRegistry }]
      )
        .filter((g) => g.assets.length > 0)
        .map((g) => (
          <section key={g.title ?? 'one'} aria-label={g.title ?? 'Markets'} className="space-y-3">
            {g.title && <h2 className="font-sans text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">{g.title}</h2>}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {g.assets.map((asset) => (
                <AssetSparklineCard
                  key={asset.symbol}
                  config={asset}
                  data={seriesMap[asset.symbol] || []}
                  feedError={marketError}
                  onExpand={(config) => setExpandedAsset(config)}
                />
              ))}
            </div>
          </section>
        ))}

      {expandedAsset && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4 sm:p-8" {...dialog.overlayProps}>
          <div className="flex max-h-[90vh] w-full max-w-5xl flex-col overflow-hidden rounded-md border border-line-strong bg-inset text-xs text-ink" {...dialog.panelProps}>
            <div className="flex items-start justify-between gap-3 border-b border-line bg-raised px-5 py-3.5">
              <div className="min-w-0">
                <h2 className="truncate text-base font-semibold text-ink">
                  {expandedAsset.title} <span className="font-mono text-sm font-normal text-ink-mute">{expandedAsset.symbol}</span>
                </h2>
                <p className="text-xs text-ink-dim">{expandedAsset.subtitle}</p>
              </div>
              <Button variant="ghost" size="sm" onClick={() => setExpandedAsset(null)} aria-label="Close">
                <X className="h-4 w-4" />
              </Button>
            </div>
            <div className="space-y-3 overflow-y-auto p-4">
              {CANDLE_PROXY[expandedAsset.symbol] && <p className="text-micro text-ink-dim">{CANDLE_PROXY[expandedAsset.symbol].note}</p>}
              <CandleChart ticker={CANDLE_PROXY[expandedAsset.symbol]?.ticker ?? expandedAsset.symbol} />
              {expandedValues.length > 1 && (
                <div className="space-y-1.5 rounded-md border border-line bg-raised p-3">
                  <p className="text-micro text-ink-dim">
                    Latest prices, one every 5 minutes, last {expandedValues.length}. Stored candles are shown above when this market has them.
                  </p>
                  <Sparkline values={expandedValues} width={720} height={64} label={`${expandedAsset.symbol}, recent 5-minute prices`} className="w-full" />
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
