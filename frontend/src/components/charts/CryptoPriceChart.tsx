'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { describeApiError } from '../../lib/api';
import { fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { useLiveEvents } from '../../lib/useLiveEvents';
import { Bitcoin, TrendingUp, Zap, Radio } from 'lucide-react';
import { formatCompact, formatCurrency, formatPercent } from '../../lib/format';
import { PALETTE } from '../../lib/palette';
import { POLL } from '../ui/DataProvider';

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

export default function CryptoPriceChart() {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  // Real-time WebSocket stream ticks
  const liveCryptoEvents = useLiveEvents('crypto');

  const { data, error } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=BTCUSD&limit=60',
    fetcher,
    { refreshInterval: POLL.live },
  );

  const basePoints =
    data?.series?.['BTCUSD'] || data?.series?.['BTC'] || data?.series?.['BTCUSDT'] || [];

  // Merge live ticks from WebSocket stream if available
  const mergedSeries = useMemo(() => {
    const list = [...basePoints];
    liveCryptoEvents.forEach((e) => {
      const sym = (
        e.crypto_data?.pair ||
        e.financial_data?.ticker ||
        e.primary_entity?.id ||
        e.primary_entity?.name ||
        ''
      ).toUpperCase();
      if (sym.includes('BTC')) {
        const price =
          e.crypto_data?.price || e.crypto_data?.mark_price || e.crypto_data?.close_price;
        if (price && price > 10000) {
          list.push({
            timestamp: e.occurred_at,
            price: price,
            // Absent, not 1000.
            //
            // `crypto_data.volume` is not a field the server has ever sent --
            // the payload carries `size_tokens` and `notional_usd` -- so this
            // fell through to the literal on every point, and the chart
            // reported a volume of exactly 1000 for every live BTC tick it
            // drew. An invented measurement is worse than a gap: a gap shows.
            volume: e.crypto_data?.size_tokens ?? null,
            anomaly_score: e.anomaly_score,
          });
        }
      }
    });
    return list.slice(-60);
  }, [basePoints, liveCryptoEvents]);

  const hasData = mergedSeries.length > 0;

  const latestPrice = hasData ? mergedSeries[mergedSeries.length - 1].price : null;
  const startPrice = hasData ? mergedSeries[0].price : null;
  const priceChange = latestPrice !== null && startPrice !== null ? latestPrice - startPrice : null;
  const priceChangePct =
    startPrice && startPrice > 0 && priceChange !== null ? (priceChange / startPrice) * 100 : null;
  const isPositive = priceChange !== null ? priceChange >= 0 : true;

  const prices = mergedSeries.map((p) => p.price);
  const minPrice = hasData ? Math.min(...prices) : 0;
  const maxPrice = hasData ? Math.max(...prices) : 0;

  const { pathStr, areaStr } = useMemo(() => {
    if (!hasData) return { pathStr: '', areaStr: '' };
    const pts = mergedSeries.map((p, idx) => {
      const x = (idx / Math.max(1, mergedSeries.length - 1)) * 600;
      const y = 160 - ((p.price - minPrice) / Math.max(1, maxPrice - minPrice || 1)) * 140;
      return { x, y };
    });

    const dPath = pts.reduce(
      (acc, pt, idx) => `${acc} ${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`,
      '',
    );
    const dArea = `${dPath} L 600 180 L 0 180 Z`;
    return { pathStr: dPath, areaStr: dArea };
  }, [mergedSeries, minPrice, maxPrice, hasData]);

  const activeHoverPoint =
    hoverIndex !== null && mergedSeries[hoverIndex] ? mergedSeries[hoverIndex] : null;

  return (
    <Card
      title="BTC / USD REAL-TIME MARKET TELEMETRY & VOLATILITY"
      badge={
        hasData ? (
          <Badge variant="live" pulse>
            LIVE WS SYNC
          </Badge>
        ) : (
          <Badge variant="warning" pulse>
            {describeApiError(error) ?? 'Waiting for the stream…'}
          </Badge>
        )
      }
      noPadding
    >
      <div className="p-4 space-y-3">
        {/* Metric Summary Ribbon */}
        <div className="grid grid-cols-3 gap-3 bg-inset p-3 rounded-lg border border-amber-500/20 text-xs">
          <div>
            <span className="text-ink-dim block text-micro uppercase font-bold flex items-center gap-1">
              <Bitcoin className="w-3.5 h-3.5 text-amber-400" /> BTC / USD PRICE
            </span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-amber-400 font-extrabold text-base">
                {latestPrice !== null
                  ? formatCurrency(latestPrice, { decimals: 2 })
                  : 'AWAITING FEED...'}
              </span>
              {hasData && <Zap className="w-3.5 h-3.5 text-amber-400 animate-pulse" />}
            </div>
          </div>

          <div>
            <span className="text-ink-dim block text-micro uppercase font-bold">24H CHANGE</span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span
                className={`font-extrabold text-base ${priceChangePct !== null ? (isPositive ? 'text-emerald-400' : 'text-rose-400') : 'text-ink-dim'}`}
              >
                {priceChangePct !== null
                  ? formatPercent(priceChangePct, { decimals: 2, signed: true })
                  : 'AWAITING FEED...'}
              </span>
            </div>
          </div>

          <div>
            <span className="text-ink-dim block text-micro uppercase font-bold">
              INTRADAY RANGE
            </span>
            <div className="text-ink font-bold text-xs mt-1">
              {hasData
                ? `${formatCurrency(minPrice)} - ${formatCurrency(maxPrice)}`
                : 'AWAITING FEED...'}
            </div>
          </div>
        </div>

        {/* Live Interactive SVG Area Chart or Awaiting Live Stream State */}
        {hasData ? (
          <div className="relative bg-page p-3 rounded-lg border border-line overflow-hidden">
            <div className="flex items-center justify-between text-micro text-ink-dim mb-2 font-bold uppercase">
              <span className="text-amber-300 font-bold">BTC / USD</span>
              <span>
                {activeHoverPoint
                  ? `${formatCurrency(activeHoverPoint.price)} · vol ${formatCompact(activeHoverPoint.volume)}`
                  : 'REAL-TIME 1-SEC TELEMETRY BARS'}
              </span>
            </div>

            <svg viewBox="0 0 600 180" className="w-full h-44 overflow-visible">
              <defs>
                <linearGradient id="btcGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={PALETTE.caution} stopOpacity="0.4" />
                  <stop offset="100%" stopColor={PALETTE.caution} stopOpacity="0.0" />
                </linearGradient>
              </defs>

              <line
                x1="0"
                y1="45"
                x2="600"
                y2="45"
                stroke="#1e293b"
                strokeDasharray="3 3"
                strokeWidth="0.8"
              />
              <line
                x1="0"
                y1="90"
                x2="600"
                y2="90"
                stroke="#1e293b"
                strokeDasharray="3 3"
                strokeWidth="0.8"
              />
              <line
                x1="0"
                y1="135"
                x2="600"
                y2="135"
                stroke="#1e293b"
                strokeDasharray="3 3"
                strokeWidth="0.8"
              />

              <path d={areaStr} fill="url(#btcGradient)" />
              <path
                d={pathStr}
                fill="none"
                stroke={PALETTE.caution}
                strokeWidth="2.2"
                strokeLinecap="round"
              />

              {hoverIndex !== null && (
                <line
                  x1={(hoverIndex / Math.max(1, mergedSeries.length - 1)) * 600}
                  y1="0"
                  x2={(hoverIndex / Math.max(1, mergedSeries.length - 1)) * 600}
                  y2="180"
                  stroke={PALETTE.accent}
                  strokeWidth="1.2"
                  strokeDasharray="2 2"
                />
              )}

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
        ) : (
          <div className="h-44 w-full bg-page rounded-lg border border-dashed border-amber-500/30 flex flex-col items-center justify-center text-center p-4 space-y-2">
            <Radio className="w-6 h-6 text-amber-400 animate-pulse" />
            <span className="text-xs font-bold text-amber-400 uppercase tracking-widest">
              Waiting for the BTC / USD stream
            </span>
            <p className="text-micro text-ink-mute max-w-sm">
              Binance & Coinbase WebSocket stream connections active. Ticks will render
              automatically upon database ingestion.
            </p>
          </div>
        )}
      </div>
    </Card>
  );
}
