'use client';

/**
 * The stock's price chart: one candle per bar, volume underneath, and the 20 and 50 bar averages. Hover or touch a bar
 * to read its open, high, low, close and volume. Daily and weekly bars come from the consolidated daily table, so the
 * last candle matches the price in the header. Green means the bar closed above where it opened, red below; nothing
 * else on the chart uses those colours.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { formatCompact, formatNumber } from '../lib/format';
import {
  FRAMES,
  barAt,
  barLabel,
  barRange,
  cleanBars,
  isUp,
  movingAverage,
  summarize,
  type CandleResponse,
  type FrameKey,
} from '../lib/candles';
import { Button } from './ui/Button';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';
import { POLL } from './ui/DataProvider';

const W = 800;
const H = 300;
const VOL_H = 54;
const GAP = 8;
const PLOT_H = H - VOL_H - GAP;
const PAD_R = 54;

const money = (v: number) => `$${formatNumber(v, { decimals: v >= 100 ? 2 : 3 })}`;

function path(values: (number | null)[], x: (i: number) => number, y: (v: number) => number): string {
  let d = '';
  let pen = false;
  values.forEach((v, i) => {
    if (v == null) {
      pen = false;
      return;
    }
    d += `${pen ? 'L' : 'M'}${x(i).toFixed(1)} ${y(v).toFixed(1)} `;
    pen = true;
  });
  return d.trim();
}

export default function CandleChart({ ticker }: { ticker: string }) {
  const [frame, setFrame] = React.useState<FrameKey>('1d');
  const [hover, setHover] = React.useState<number | null>(null);
  const spec = FRAMES.find((f) => f.key === frame) ?? FRAMES[2];
  const { data, error } = useSWR<CandleResponse>(`/radar/candles/${encodeURIComponent(ticker)}?timeframe=${spec.key}&limit=${spec.limit}`, fetcher, {
    refreshInterval: frame === '1d' || frame === '1w' ? POLL.slow : POLL.standard,
    revalidateOnFocus: false,
  });

  const bars = React.useMemo(() => cleanBars(data?.candles), [data]);
  const ma20 = React.useMemo(() => movingAverage(bars, 20), [bars]);
  const ma50 = React.useMemo(() => movingAverage(bars, 50), [bars]);
  const range = React.useMemo(() => barRange(bars, [ma20, ma50]), [bars, ma20, ma50]);
  const sum = React.useMemo(() => summarize(bars), [bars]);

  React.useEffect(() => setHover(null), [ticker, frame]);

  const plotW = W - PAD_R;
  const slot = bars.length ? plotW / bars.length : plotW;
  const body = Math.max(1, Math.min(14, slot * 0.68));
  const x = (i: number) => slot * (i + 0.5);
  const y = (v: number) => (range ? PLOT_H - ((v - range.lo) / (range.hi - range.lo)) * PLOT_H : 0);
  const vy = (v: number) => (range && range.maxVolume > 0 ? H - (v / range.maxVolume) * VOL_H : H);

  const shown = hover != null && bars[hover] ? bars[hover] : sum?.last ?? null;
  const prior = hover != null && hover > 0 ? bars[hover - 1] : null;
  const move = shown && prior ? (shown.close / prior.close - 1) * 100 : null;

  const onPointer = (e: React.PointerEvent<SVGSVGElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    const f = ((e.clientX - r.left) / r.width) * (W / plotW);
    setHover(f > 1 ? null : barAt(f, bars.length));
  };

  const ticks = range ? [0, 0.25, 0.5, 0.75, 1].map((t) => range.lo + (range.hi - range.lo) * t) : [];

  return (
    <section aria-label={`${ticker} price chart`} className="space-y-2 rounded-md border border-line bg-raised p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div role="group" aria-label="Bar size" className="flex gap-1.5">
          {FRAMES.map((f) => (
            <Button key={f.key} variant="outline" size="sm" pressed={frame === f.key} onClick={() => setFrame(f.key)}>
              {f.label}
            </Button>
          ))}
        </div>
        <p className="text-micro text-ink-mute" aria-live="polite">
          {shown ? (
            <>
              <span className="text-ink-dim">{barLabel(shown.ts, frame)}</span>
              {'  '}
              Open {money(shown.open)} · High {money(shown.high)} · Low {money(shown.low)} · Close{' '}
              <span className={isUp(shown) ? 'text-positive' : 'text-negative'}>{money(shown.close)}</span>
              {move != null && (
                <span className={move >= 0 ? 'text-positive' : 'text-negative'}>
                  {' '}
                  ({move >= 0 ? '+' : '−'}
                  {formatNumber(Math.abs(move), { decimals: 2 })}%)
                </span>
              )}
              {shown.volume > 0 && <> · Volume {formatCompact(shown.volume)}</>}
            </>
          ) : (
            'Hover a bar to read it'
          )}
        </p>
      </div>

      {failedWithout(error, data) ? (
        <PanelError what="the price bars" inline />
      ) : !data ? (
        <LoadingLines lines={5} label={`Loading ${ticker} price bars`} />
      ) : !range || !sum ? (
        <p role="status" className="py-8 text-center text-xs text-ink-mute">
          No stored {spec.label.toLowerCase()} bars for {ticker}. {frame === '1d' || frame === '1w' ? 'Daily history is kept for US stocks and ETFs.' : 'Intraday bars are kept only for the stocks being watched.'}
        </p>
      ) : (
        <>
          <svg
            viewBox={`0 0 ${W} ${H}`}
            className="h-72 w-full touch-pan-y select-none"
            role="img"
            aria-label={`${ticker} ${spec.label.toLowerCase()} candles, ${bars.length} bars, last close ${money(sum.last.close)}`}
            onPointerMove={onPointer}
            onPointerDown={onPointer}
            onPointerLeave={() => setHover(null)}
          >
            {ticks.map((t) => (
              <g key={t}>
                <line x1={0} x2={plotW} y1={y(t)} y2={y(t)} className="stroke-line" strokeWidth={0.5} />
                <text x={plotW + 6} y={y(t) + 3} className="fill-ink-mute" fontSize={10}>
                  {formatNumber(t, { decimals: t >= 100 ? 0 : 2 })}
                </text>
              </g>
            ))}
            {bars.map((b, i) => (
              <g key={b.ts} className={isUp(b) ? 'text-positive' : 'text-negative'}>
                <rect x={x(i) - body / 2} y={vy(b.volume)} width={body} height={Math.max(0, H - vy(b.volume))} fill="currentColor" opacity={0.28} />
                <line x1={x(i)} x2={x(i)} y1={y(b.high)} y2={y(b.low)} stroke="currentColor" strokeWidth={1} />
                <rect x={x(i) - body / 2} y={Math.min(y(b.open), y(b.close))} width={body} height={Math.max(1, Math.abs(y(b.open) - y(b.close)))} fill="currentColor" />
              </g>
            ))}
            <path d={path(ma50, x, y)} fill="none" className="stroke-caution" strokeWidth={1.25} />
            <path d={path(ma20, x, y)} fill="none" className="stroke-accent" strokeWidth={1.25} />
            <line x1={0} x2={plotW} y1={y(sum.last.close)} y2={y(sum.last.close)} className="stroke-ink-mute" strokeWidth={0.75} strokeDasharray="2 3" />
            <rect x={plotW} y={y(sum.last.close) - 8} width={PAD_R} height={16} className={isUp(sum.last) ? 'fill-positive' : 'fill-negative'} />
            <text x={plotW + 4} y={y(sum.last.close) + 3.5} fontSize={10} className="fill-page" fontWeight={600}>
              {formatNumber(sum.last.close, { decimals: 2 })}
            </text>
            {hover != null && bars[hover] && (
              <line x1={x(hover)} x2={x(hover)} y1={0} y2={H} className="stroke-ink-dim" strokeWidth={0.75} />
            )}
          </svg>
          <p className="flex flex-wrap items-center gap-x-4 gap-y-1 text-micro text-ink-mute">
            <span>
              <span className="mr-1 inline-block h-0.5 w-3 bg-accent align-middle" aria-hidden />
              Average of the last 20 bars
            </span>
            <span>
              <span className="mr-1 inline-block h-0.5 w-3 bg-caution align-middle" aria-hidden />
              Average of the last 50 bars
            </span>
            <span>
              {bars.length} bars, {money(sum.low)} to {money(sum.high)}, {sum.changePct >= 0 ? 'up' : 'down'} {formatNumber(Math.abs(sum.changePct), { decimals: 1 })}% over the window
              {data.source ? ` · ${data.source.replace('timescale:daily_bars', 'consolidated daily prices')}` : ''}
            </span>
          </p>
        </>
      )}
    </section>
  );
}
