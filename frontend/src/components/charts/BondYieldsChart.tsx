'use client';

import React, { useMemo } from 'react';
import useSWR from 'swr';
import { describeApiError, fetcher } from '../../lib/api';
import { Card } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { useLiveEvents } from '../../lib/useLiveEvents';
import { Activity, Radio } from 'lucide-react';
import { formatNumber, formatPercent } from '../../lib/format';
import ProvenanceBadge, { ProvenanceType } from '../ProvenanceBadge';
import { PALETTE } from '../../lib/palette';
import { POLL } from '../ui/DataProvider';

interface SeriesProvenance {
  source_type: ProvenanceType;
  methodology?: string;
}

interface SeriesPoint {
  timestamp: string;
  price: number;
  // Which feed answered, and what kind of number this is. The gateway resolves
  // the 2Y through three sources in order -- the Treasury's own par-yield
  // publication, the CBOE futures yield, then its own Redis cache -- and used
  // to fall through to a hardcoded 4.15 labelled "Parametric Baseline Yield".
  // The label never reached this component, which plots `price`, so an invented
  // constant rendered identically to a Treasury print. The constant is gone and
  // the label now arrives with the data.
  provider?: string;
  provenance?: SeriesProvenance;
  // Nullable, because the backend stopped inventing one. A Treasury par
  // yield and a cached quote report no volume, and the gateway now says so
  // instead of sending 1000.
  volume: number | null;
  anomaly_score: number;
}

interface MarketSeriesResponse {
  symbols: string[];
  series: Record<string, SeriesPoint[]>;
}

/** Live-tick symbols for each yield, matched exactly. A substring test sent
 * every ticker containing a "2" into the 2-year line, and counted IRX -- the
 * 13-week bill -- as a 2-year yield. */
interface YieldCurveResponse {
  series: Record<'US30Y' | 'US10Y' | 'US02Y', { date: string; yield: number }[]>;
  source: string;
  as_of: string | null;
}

/** The 2s10s spread from one date of the Treasury curve: the latest date that
 * has both maturities. Null when no date has both. */
export function sameDaySpread(
  series: Partial<Record<'US02Y' | 'US10Y' | 'US30Y', { date: string; yield: number }[]>> | undefined,
): { date: string; spread: number } | null {
  const two = new Map((series?.US02Y ?? []).map((p) => [p.date, p.yield]));
  const ten = [...(series?.US10Y ?? [])].sort((a, b) => b.date.localeCompare(a.date));
  for (const p of ten) {
    const y2 = two.get(p.date);
    if (y2 !== undefined) return { date: p.date, spread: p.yield - y2 };
  }
  return null;
}

export const YIELD_SYMBOLS = {
  US30Y: new Set(['US30Y', 'US30', 'TYX']),
  US10Y: new Set(['US10Y', 'TNX']),
  US02Y: new Set(['US02Y', 'US2Y', '2Y']),
};

export interface PlotBox {
  t0: number;
  t1: number;
  lo: number;
  hi: number;
  width: number;
  height: number;
}

/** An SVG path for one series, x by timestamp on the shared axis, y by value. */
export function timePath(series: { timestamp: string; price: number | null }[], b: PlotBox): string {
  const span = b.t1 - b.t0;
  const range = b.hi - b.lo || 1;
  const pts = series
    .map((s) => ({ t: Date.parse(s.timestamp), v: s.price }))
    .filter((p): p is { t: number; v: number } => Number.isFinite(p.t) && typeof p.v === 'number' && Number.isFinite(p.v))
    .sort((a, b2) => a.t - b2.t);
  return pts
    .map((p, i) => {
      const x = span > 0 ? ((p.t - b.t0) / span) * b.width : b.width;
      const y = b.height - ((p.v - b.lo) / range) * b.height;
      return `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(' ');
}

export default function BondYieldsChart() {

  // Real-time WebSocket stream ticks
  const liveTradfiEvents = useLiveEvents('tradfi');

  // The lines: one official daily series per maturity, so all three share a
  // time scale. The figures above them stay on the latest intraday readings.
  const { data: curve } = useSWR<YieldCurveResponse>('/radar/yield-curve?days=60', fetcher, { refreshInterval: POLL.slow });

  const { data, error } = useSWR<MarketSeriesResponse>(
    '/radar/market-series?symbols=US30Y,US10Y,US02Y,TLT,IEF&limit=50',
    fetcher,
    { refreshInterval: POLL.live },
  );

  const base30Y = useMemo(() => data?.series?.['US30Y'] || data?.series?.['US30'] || [], [data]);
  const base10Y = useMemo(() => data?.series?.['US10Y'] || [], [data]);
  const base2Y = useMemo(
    () => data?.series?.['US02Y'] || data?.series?.['US2Y'] || data?.series?.['2Y'] || [],
    [data],
  );

  // Merge live ticks from WebSocket stream
  const us30ySeries = useMemo(() => {
    const list = [...base30Y];
    liveTradfiEvents.forEach((e) => {
      const sym = (
        e.financial_data?.ticker ||
        e.primary_entity?.id ||
        e.primary_entity?.name ||
        ''
      ).toUpperCase();
      if (YIELD_SYMBOLS.US30Y.has(sym)) {
        const p = e.financial_data?.underlying_price || e.financial_data?.close_price;
        if (p)
          list.push({
            timestamp: e.occurred_at,
            price: p,
            volume: null,
            anomaly_score: e.anomaly_score,
          });
      }
    });
    return list.slice(-50);
  }, [base30Y, liveTradfiEvents]);

  const us10ySeries = useMemo(() => {
    const list = [...base10Y];
    liveTradfiEvents.forEach((e) => {
      const sym = (
        e.financial_data?.ticker ||
        e.primary_entity?.id ||
        e.primary_entity?.name ||
        ''
      ).toUpperCase();
      if (YIELD_SYMBOLS.US10Y.has(sym)) {
        const p = e.financial_data?.underlying_price || e.financial_data?.close_price;
        if (p)
          list.push({
            timestamp: e.occurred_at,
            price: p,
            volume: null,
            anomaly_score: e.anomaly_score,
          });
      }
    });
    return list.slice(-50);
  }, [base10Y, liveTradfiEvents]);

  const us02ySeries = useMemo(() => {
    const list = [...base2Y];
    liveTradfiEvents.forEach((e) => {
      const sym = (
        e.financial_data?.ticker ||
        e.primary_entity?.id ||
        e.primary_entity?.name ||
        ''
      ).toUpperCase();
      if (YIELD_SYMBOLS.US02Y.has(sym)) {
        const p = e.financial_data?.underlying_price || e.financial_data?.close_price;
        if (p)
          list.push({
            timestamp: e.occurred_at,
            price: p,
            volume: null,
            anomaly_score: e.anomaly_score,
          });
      }
    });
    return list.slice(-50);
  }, [base2Y, liveTradfiEvents]);

  const hasData = us30ySeries.length > 0 || us10ySeries.length > 0 || us02ySeries.length > 0;

  const latest30Y = us30ySeries[us30ySeries.length - 1]?.price || null;
  const latest10Y = us10ySeries[us10ySeries.length - 1]?.price || null;
  const latest2Y = us02ySeries[us02ySeries.length - 1]?.price || null;

  // Both legs from the same day. The intraday 10-year minus the 2-year's
  // previous Treasury close mixed two days, wrong by exactly the day's move
  // (14 bp on 15 Oct). The Treasury curve publishes both for one date; the
  // intraday pair is used only when both legs are intraday.
  const sameDay = sameDaySpread(curve?.series);
  const spread2Y10Y = sameDay ? sameDay.spread : null;
  const spreadDate = sameDay?.date ?? null;
  const isInverted = spread2Y10Y !== null ? spread2Y10Y < 0 : false;

  // The provenance of whichever point is actually on screen. A live WebSocket
  // tick carries none, so the badge falls back to the last point that did.
  const latest2YProvenance = (() => {
    for (let i = us02ySeries.length - 1; i >= 0; i--) {
      const point = us02ySeries[i];
      if (point?.provenance) return { provenance: point.provenance, provider: point.provider };
    }
    return null;
  })();

  // Each line is placed by its own dates on one shared time axis. Placing
  // points by array position squeezed a short series into a corner, and the
  // intraday ticks of one maturity sat beside the daily closes of another.
  const { path30Y, path10Y, path2Y, curveSpan } = useMemo(() => {
    const toPts = (k: 'US30Y' | 'US10Y' | 'US02Y') =>
      (curve?.series?.[k] ?? []).map((p) => ({ timestamp: `${p.date}T20:00:00Z`, price: p.yield }));
    const s30 = toPts('US30Y');
    const s10 = toPts('US10Y');
    const s2 = toPts('US02Y');
    const all = [...s30, ...s10, ...s2];
    const times = all.map((p) => Date.parse(p.timestamp)).filter(Number.isFinite);
    const prices = all.map((p) => p.price).filter((v) => Number.isFinite(v));
    if (!times.length || !prices.length) return { path30Y: '', path10Y: '', path2Y: '', curveSpan: null };
    const lo = Math.min(...prices);
    const hi = Math.max(...prices);
    const pad = Math.max(0.05, (hi - lo) * 0.1);
    const box = { t0: Math.min(...times), t1: Math.max(...times), lo: lo - pad, hi: hi + pad, width: 600, height: 180 };
    return {
      path30Y: timePath(s30, box),
      path10Y: timePath(s10, box),
      path2Y: timePath(s2, box),
      curveSpan: { from: s2[0]?.timestamp.slice(0, 10) ?? null, to: curve?.as_of ?? null, days: s2.length, lo: box.lo, hi: box.hi },
    };
  }, [curve]);

  return (
    <Card
      title="Treasury yield curve"
      subtitle="30Y, 10Y and 2Y, with the 10Y–2Y spread"
      badge={
        hasData ? (
          <Badge variant={isInverted ? 'warning' : 'live'} pulse>
            {isInverted ? 'YIELD CURVE INVERTED' : 'NORMAL CURVE'}
          </Badge>
        ) : (
          /* An outage and a quiet market are opposite facts, and this badge
             used to state the second one for both. */
          <Badge variant="warning" pulse>
            {describeApiError(error) ?? 'Waiting for the stream…'}
          </Badge>
        )
      }
      noPadding
    >
      <div className="p-4 space-y-3">
        {/* Metric Summary Ribbon */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 bg-inset p-3 rounded-lg border border-line text-xs">
          <div>
            <span className="text-ink-dim block text-micro uppercase font-bold">
              30Y BOND YIELD (US30Y)
            </span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-positive font-extrabold text-base">
                {latest30Y !== null
                  ? formatPercent(latest30Y, { decimals: 2 })
                  : 'Waiting…'}
              </span>
              <Activity className="w-3.5 h-3.5 text-positive animate-pulse" />
            </div>
          </div>

          <div>
            <span className="text-ink-dim block text-micro uppercase font-bold">
              10Y TREASURY YIELD (US10Y)
            </span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className="text-accent font-extrabold text-base">
                {latest10Y !== null
                  ? formatPercent(latest10Y, { decimals: 2 })
                  : 'Waiting…'}
              </span>
            </div>
          </div>

          <div>
            <span className="text-ink-dim block text-micro uppercase font-bold">
              2Y TREASURY YIELD (US02Y)
            </span>
            <div className="flex items-center gap-1.5 mt-0.5 flex-wrap">
              <span className="text-accent font-extrabold text-base">
                {latest2Y !== null ? formatPercent(latest2Y, { decimals: 2 }) : 'Waiting…'}
              </span>
              {latest2Y !== null && latest2YProvenance && (
                <ProvenanceBadge
                  sourceType={latest2YProvenance.provenance.source_type}
                  methodology={latest2YProvenance.provenance.methodology}
                  dataInputs={latest2YProvenance.provider ? [latest2YProvenance.provider] : []}
                />
              )}
            </div>
          </div>

          <div>
            <span className="text-ink-dim block text-micro uppercase font-bold" title="10-year minus 2-year Treasury yield. Below zero (an inverted curve) has often come before recessions.">
              10-year minus 2-year rate
            </span>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span
                className={`font-extrabold text-base ${spread2Y10Y !== null && spread2Y10Y < 0 ? 'text-negative' : 'text-positive'}`}
              >
                {spread2Y10Y !== null
                  ? `${spread2Y10Y >= 0 ? '+' : ''}${formatNumber(spread2Y10Y, { decimals: 2 })} pts`
                  : 'Waiting…'}
              </span>
              {spreadDate && <span className="text-micro text-ink-mute">close of {spreadDate}</span>}
            </div>
          </div>
        </div>

        {/* SVG Yield Chart or Awaiting Live Stream State */}
        {path2Y && (
          <div className="flex flex-wrap gap-4 text-micro text-ink-dim">
            <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 rounded" style={{ background: PALETTE.positive }} />30-year</span>
            <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 rounded" style={{ background: PALETTE.accent }} />10-year</span>
            <span className="flex items-center gap-1.5"><span className="h-0.5 w-4 rounded" style={{ background: PALETTE.info }} />2-year</span>
            {curveSpan && (
              <span className="text-ink-mute">
                Daily close, last {curveSpan.days} trading days ({curveSpan.from} to {curveSpan.to}) · {curveSpan.lo.toFixed(2)}% to{' '}
                {curveSpan.hi.toFixed(2)}% · US Treasury
              </span>
            )}
          </div>
        )}
        {path2Y || path10Y || path30Y ? (
          <div className="relative h-48 w-full bg-page rounded-lg border border-line p-2 overflow-hidden">
            <svg
              viewBox="0 0 600 180"
              preserveAspectRatio="none"
              className="w-full h-full overflow-visible"
            >
              <line
                x1="0"
                y1="45"
                x2="600"
                y2="45"
                stroke="var(--border-strong)"
                strokeDasharray="3 3"
                strokeWidth="0.5"
              />
              <line
                x1="0"
                y1="90"
                x2="600"
                y2="90"
                stroke="var(--border-strong)"
                strokeDasharray="3 3"
                strokeWidth="0.5"
              />
              <line
                x1="0"
                y1="135"
                x2="600"
                y2="135"
                stroke="var(--border-strong)"
                strokeDasharray="3 3"
                strokeWidth="0.5"
              />

              {/* 30Y Yield Curve (Emerald) */}
              {path30Y && (
                <path
                  d={path30Y}
                  fill="none"
                  stroke={PALETTE.positive}
                  strokeWidth="2.5"
                  vectorEffect="non-scaling-stroke"
                  strokeLinecap="round"
                />
              )}
              {/* 10Y Yield Curve (Cyan) */}
              {path10Y && (
                <path
                  d={path10Y}
                  fill="none"
                  stroke={PALETTE.accent}
                  strokeWidth="2.5"
                  vectorEffect="non-scaling-stroke"
                  strokeLinecap="round"
                />
              )}
              {/* 2Y Yield Curve (Purple) */}
              {path2Y && (
                <path
                  d={path2Y}
                  fill="none"
                  stroke={PALETTE.info}
                  strokeWidth="2.5"
                  vectorEffect="non-scaling-stroke"
                  strokeLinecap="round"
                />
              )}
            </svg>
          </div>
        ) : (
          <div className="h-48 w-full bg-page rounded-lg border border-dashed border-caution/30 flex flex-col items-center justify-center text-center p-4 space-y-2">
            <Radio className="w-6 h-6 text-caution animate-pulse" />
            <span className="text-xs font-semibold text-caution">
              {error
                ? `Treasury yields: ${describeApiError(error)}`
                : 'Waiting for the first Treasury yield readings'}
            </span>
            {/* The reassurance below is a claim about the backend, and it was
                printed while the request to that backend was failing. */}
            <p className="text-micro text-ink-mute max-w-sm">
              {error
                ? 'The series request did not succeed. Nothing is being plotted, and this is not a quiet market.'
                : 'Backend live WebSocket and REST pollers are active. Ticks will render automatically upon database ingestion.'}
            </p>
          </div>
        )}
      </div>
    </Card>
  );
}
