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
  RANGES,
  barAt,
  barLabel,
  barRange,
  barTick,
  cleanBars,
  isUp,
  movingAverage,
  priceAtY,
  rangeRequest,
  responseFrame,
  spanDays,
  splitWarmup,
  summarize,
  type CandleResponse,
  type RangeKey,
} from '../lib/candles';
import { KIND_WORD, placeEvents, type StockEvent } from '../lib/events';
import { Button } from './ui/Button';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';
import { POLL } from './ui/DataProvider';

const NONE: (number | null)[] = [];
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
  const [rangeKey, setRangeKey] = React.useState<RangeKey>('1Y');
  const [hover, setHover] = React.useState<number | null>(null);
  const [hoverY, setHoverY] = React.useState<number | null>(null);
  const [asTable, setAsTable] = React.useState(false);
  // Drawn at the width it is shown at, so candles fill the box rather than sitting in a letterboxed corner.
  // A state ref: the box only exists once the bars have loaded, so an effect keyed on it runs then.
  const [box, setBox] = React.useState<HTMLDivElement | null>(null);
  const [W, setW] = React.useState(800);
  React.useEffect(() => {
    if (!box || typeof ResizeObserver === 'undefined') return;
    const ro = new ResizeObserver(() => setW(Math.max(320, Math.round(box.clientWidth))));
    ro.observe(box);
    setW(Math.max(320, Math.round(box.clientWidth)));
    return () => ro.disconnect();
  }, [box]);
  const spec = RANGES.find((r) => r.key === rangeKey) ?? RANGES[3];
  // Extra earlier bars come back with the window so the averages are right from the first bar drawn.
  const { data, error } = useSWR<CandleResponse>(`/radar/candles/${encodeURIComponent(ticker)}?range=${rangeRequest(spec.key).ask}&warmup=200`, fetcher, {
    refreshInterval: spec.key === '1Y' || spec.key === '5Y' ? POLL.slow : POLL.standard,
    revalidateOnFocus: false,
  });
  const frame = responseFrame(data?.timeframe, spec.key);
  // Dated events from stored rows (earnings, filings, alerts), marked on the candles they fall on.
  const { data: eventData } = useSWR<StockEvent[]>(`/stocks/${encodeURIComponent(ticker)}/events?days=365`, fetcher, { refreshInterval: POLL.rare, revalidateOnFocus: false, shouldRetryOnError: false });

  const keep = rangeRequest(spec.key).keep;
  const drawn = React.useMemo(() => {
    const all = cleanBars(data?.candles);
    const split = splitWarmup(all, [movingAverage(all, 20), movingAverage(all, 50)]);
    if (keep == null || split.bars.length <= keep) return split;
    const cut = split.bars.length - keep;
    return { bars: split.bars.slice(cut), averages: split.averages.map((a) => a.slice(cut)) };
  }, [data, keep]);
  const bars = drawn.bars;
  const ma20 = drawn.averages[0] ?? NONE;
  const ma50 = drawn.averages[1] ?? NONE;
  const placed = React.useMemo(() => placeEvents(Array.isArray(eventData) ? eventData : [], bars, frame), [eventData, bars, frame]);
  const range = React.useMemo(() => barRange(bars, [ma20, ma50]), [bars, ma20, ma50]);
  const sum = React.useMemo(() => summarize(bars), [bars]);

  React.useEffect(() => setHover(null), [ticker, rangeKey]);

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
    setHoverY(((e.clientY - r.top) / r.height) * H);
  };

  const ticks = range ? [0, 0.25, 0.5, 0.75, 1].map((t) => range.lo + (range.hi - range.lo) * t) : [];

  return (
    <section aria-label={`${ticker} price chart`} className="space-y-2 rounded-md border border-line bg-raised p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div role="group" aria-label="How far back" className="flex gap-1.5">
          {RANGES.map((f) => (
            <Button key={f.key} variant="outline" size="sm" pressed={rangeKey === f.key} onClick={() => setRangeKey(f.key)}>
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
          No stored bars for {ticker} over {spec.label}. {spec.key === '1Y' || spec.key === '5Y' ? 'Daily history is kept for US stocks and ETFs.' : 'Intraday bars are kept only for the stocks being watched.'}
        </p>
      ) : (
        <>
          <div ref={setBox} className="w-full">
          <svg
            viewBox={`0 0 ${W} ${H}`}
            width={W}
            height={H}
            className="block touch-pan-y select-none"
            role="img"
            aria-label={`${ticker} candles over ${spec.label}, ${bars.length} bars, last close ${money(sum.last.close)}`}
            onPointerMove={onPointer}
            onPointerDown={onPointer}
            onPointerLeave={() => {
              setHover(null);
              setHoverY(null);
            }}
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
                <rect x={x(i) - body / 2} y={Math.min(y(b.open), y(b.close))} width={body} height={Math.max(1, Math.abs(y(b.open) - y(b.close)))} fill={isUp(b) ? 'var(--bg-raised)' : 'currentColor'} stroke="currentColor" strokeWidth={1} />
              </g>
            ))}
            {placed.map((e) => (
              <g key={`${e.i}-${e.kind}`} transform={`translate(${x(e.i)} 8)`} tabIndex={0} role="img" aria-label={`${KIND_WORD[e.kind]} on ${e.date}: ${e.label}`}>
                <title>{`${KIND_WORD[e.kind]}, ${e.date}: ${e.label}`}</title>
                <line y1="6" y2={PLOT_H - 8} stroke="var(--border-subtle)" strokeDasharray="1 3" />
                {e.kind === 'earnings' ? (
                  <path d="M0 -5 l5 5 -5 5 -5 -5z" fill="var(--text-secondary)" />
                ) : e.kind === 'filing' ? (
                  <path d="M0 -5 l5 9 h-10z" fill="var(--text-secondary)" />
                ) : (
                  <circle r="3" fill="var(--text-secondary)" />
                )}
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
            {hover != null && hoverY != null && range && priceAtY(hoverY, PLOT_H, range.lo, range.hi) != null && (
              <g>
                <line x1={0} x2={plotW} y1={hoverY} y2={hoverY} className="stroke-ink-dim" strokeWidth={0.75} />
                <rect x={plotW} y={hoverY - 8} width={PAD_R} height={16} className="fill-overlay" />
                <text x={plotW + 4} y={hoverY + 3.5} fontSize={10} className="fill-ink">
                  {formatNumber(priceAtY(hoverY, PLOT_H, range.lo, range.hi) ?? 0, { decimals: 2 })}
                </text>
              </g>
            )}
          </svg>
          <div className="flex justify-between text-micro text-ink-mute" style={{ width: plotW }} aria-hidden>
            {[0, 0.25, 0.5, 0.75, 1].map((f) => (
              <span key={f}>{barTick(bars[Math.min(bars.length - 1, Math.round(f * (bars.length - 1)))].ts, frame)}</span>
            ))}
          </div>
          </div>
          <p className="flex flex-wrap items-center gap-x-4 gap-y-1 text-micro text-ink-mute">
            <span>
              <span className="mr-1 inline-block h-0.5 w-3 bg-accent align-middle" aria-hidden />
              20-bar average
            </span>
            <span>
              <span className="mr-1 inline-block h-0.5 w-3 bg-caution align-middle" aria-hidden />
              50-bar average
            </span>
            {placed.length > 0 && <span>◆ earnings · ▲ filing · ● alert</span>}
          </p>
          {data.history_limited && (
            <p className="text-micro text-caution">Stored history does not reach back this far, so the chart starts later. Intraday prices are stored only since mid-July.</p>
          )}
          <details className="text-micro text-ink-mute">
            <summary className="cursor-pointer select-none hover:text-ink">About this chart</summary>
            <p className="pt-1">Hollow bar: closed higher. Filled bar: closed lower.</p>
            <p>
              {bars.length} {frame === '1d' ? 'daily' : frame === '1w' ? 'weekly' : frame === '1h' ? 'hourly' : '5-minute'} bars, about {spanDays(bars)} days, {money(sum.low)} to {money(sum.high)}, {sum.changePct >= 0 ? 'up' : 'down'} {formatNumber(Math.abs(sum.changePct), { decimals: 1 })}% over the window
              {data.source ? ` · ${data.source.replace('timescale:daily_bars', 'consolidated daily prices')}` : ''}
            </p>
          </details>
          <p className="text-micro text-ink-mute">
            Latest bar: {barLabel(sum.last.ts, frame)}.{frame === '1d' || frame === '1w' ? '' : ' Free intraday prices can be up to 15 minutes late.'}{' '}
            <Button variant="link" size="sm" pressed={asTable} onClick={() => setAsTable((v) => !v)}>
              {asTable ? 'Hide the table' : 'Show as a table'}
            </Button>
          </p>
          {asTable && (
            <table className="w-full text-micro tabular-nums">
              <caption className="sr-only">Last 30 bars of {ticker}</caption>
              <thead className="text-ink-mute">
                <tr>
                  <th className="text-left font-normal">Bar</th>
                  <th className="text-right font-normal">Open</th>
                  <th className="text-right font-normal">High</th>
                  <th className="text-right font-normal">Low</th>
                  <th className="text-right font-normal">Close</th>
                  <th className="text-right font-normal">Volume</th>
                </tr>
              </thead>
              <tbody className="font-mono">
                {bars.slice(-30).reverse().map((b) => (
                  <tr key={b.ts} className="border-t border-line/60">
                    <td className="text-left font-sans text-ink-dim">{barTick(b.ts, frame)}</td>
                    <td className="text-right">{money(b.open)}</td>
                    <td className="text-right">{money(b.high)}</td>
                    <td className="text-right">{money(b.low)}</td>
                    <td className={`text-right ${isUp(b) ? 'text-positive' : 'text-negative'}`}>{money(b.close)}</td>
                    <td className="text-right">{formatCompact(b.volume)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}
    </section>
  );
}
