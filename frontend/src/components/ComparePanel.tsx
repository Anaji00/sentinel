'use client';

/**
 * Two stocks head to head: both lines start at zero so you can see which has done better, with each one's return over a
 * month, three months and the whole window, and how closely their daily moves track. Built only from daily closes, so
 * it says what the history supports and nothing else. The first stock is drawn in the accent colour, the second in amber.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { alignCloses, compare, correlationWords, leaderSentence, rebased, type Close } from '../lib/compare';
import { formatNumber } from '../lib/format';
import { Button } from './ui/Button';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';
import { Term } from './ui/Term';

const W = 600;
const H = 180;
const PAD = 6;

/** The windows offered, in calendar days of history asked for (the route allows up to 400). */
const RANGES = [
  { label: '1 month', days: 31 },
  { label: '3 months', days: 90 },
  { label: '6 months', days: 180 },
  { label: '1 year', days: 365 },
] as const;

function useCloses(ticker: string, days: number) {
  return useSWR<{ closes: Close[] }>(`/predictions/closes/${encodeURIComponent(ticker)}?days=${days}`, fetcher, { revalidateOnFocus: false });
}

const pct = (v: number | null) => (v == null ? '—' : `${v > 0 ? '+' : v < 0 ? '−' : ''}${formatNumber(Math.abs(v), { decimals: 1 })}%`);
const tone = (v: number | null) => (v == null ? 'text-ink-mute' : v > 0 ? 'text-positive' : v < 0 ? 'text-negative' : 'text-ink');

function Lines({ a, b, aTicker, bTicker }: { a: number[]; b: number[]; aTicker: string; bTicker: string }) {
  const all = [...a, ...b, 0];
  const lo = Math.min(...all);
  const hi = Math.max(...all);
  const span = hi - lo || 1;
  // Room on the right for each line's name and final change, so the colours are not the only way to tell them apart.
  const x = (i: number) => PAD + (i / Math.max(1, a.length - 1)) * (W - 2 * PAD - 84);
  const y = (v: number) => H - PAD - ((v - lo) / span) * (H - 2 * PAD);
  const path = (s: number[]) => s.map((v, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)} ${y(v).toFixed(1)}`).join(' ');
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="h-44 w-full rounded-sm border border-line bg-inset" role="img" aria-label={`${aTicker} and ${bTicker}, change since the start of the window`}>
      <line x1={PAD} x2={W - PAD} y1={y(0)} y2={y(0)} className="stroke-line-strong" strokeDasharray="3 3" />
      <path d={path(b)} fill="none" className="stroke-caution" strokeWidth={1.5} />
      <path d={path(a)} fill="none" className="stroke-accent" strokeWidth={1.5} />
      {[
        { t: aTicker, v: a[a.length - 1], cls: 'fill-accent' },
        { t: bTicker, v: b[b.length - 1], cls: 'fill-caution' },
      ]
        .map((l, i, arr) => {
          const ly = y(l.v);
          const other = y(arr[1 - i].v);
          // Two labels close together are pushed apart so both stay readable.
          const nudge = Math.abs(ly - other) < 12 ? (ly <= other ? -6 : 6) : 0;
          return (
            <text key={l.t} x={x(a.length - 1) + 6} y={ly + 3 + nudge} fontSize={10} className={l.cls}>
              {l.t} {pct(l.v)}
            </text>
          );
        })}
    </svg>
  );
}

export default function ComparePanel({ a, b, onSwap, onClear }: { a: string; b: string; onSwap: () => void; onClear: () => void }) {
  const [days, setDays] = React.useState<number>(90);
  const ra = useCloses(a, days);
  const rb = useCloses(b, days);
  const aligned = React.useMemo(() => alignCloses(ra.data?.closes, rb.data?.closes), [ra.data, rb.data]);
  const c = React.useMemo(() => compare(aligned), [aligned]);

  return (
    <section aria-label={`${a} compared with ${b}`} className="space-y-2.5 rounded-sm border border-line-accent bg-inset p-3">
      <div className="flex items-center justify-between gap-2">
        <h3 className="text-xs font-semibold text-ink">
          <span className="text-accent">{a}</span> compared with <span className="text-caution">{b}</span>
        </h3>
        <span className="flex gap-1.5">
          <Button variant="outline" size="sm" onClick={onSwap}>
            Swap
          </Button>
          <Button variant="outline" size="sm" onClick={onClear}>
            Stop comparing
          </Button>
        </span>
      </div>
      <div role="group" aria-label="How far back" className="flex gap-1.5">
        {RANGES.map((r) => (
          <Button key={r.days} variant="outline" size="sm" pressed={days === r.days} onClick={() => setDays(r.days)}>
            {r.label}
          </Button>
        ))}
      </div>
      {failedWithout(ra.error, ra.data) || failedWithout(rb.error, rb.data) ? (
        <PanelError what="the price history" inline />
      ) : !ra.data || !rb.data ? (
        <LoadingLines lines={4} label={`Loading ${a} and ${b}`} />
      ) : !c ? (
        <p role="status" className="text-xs text-ink-mute">
          {a} and {b} do not share enough trading days in the stored history to compare.
        </p>
      ) : (
        <>
          <p className="text-xs text-ink-dim">{leaderSentence(a, b, c)}</p>
          <Lines a={rebased(aligned.a)} b={rebased(aligned.b)} aTicker={a} bTicker={b} />
          <table className="w-full text-xs">
            <thead className="text-ink-mute">
              <tr className="border-b border-line">
                <th className="py-1 text-left font-normal" />
                <th className="py-1 text-right font-semibold text-accent">{a}</th>
                <th className="py-1 text-right font-semibold text-caution">{b}</th>
              </tr>
            </thead>
            <tbody className="font-mono tabular-nums">
              {[
                ['Last close', `$${formatNumber(c.a.last, { decimals: 2 })}`, `$${formatNumber(c.b.last, { decimals: 2 })}`, null, null],
                ['Last month', pct(c.a.month), pct(c.b.month), c.a.month, c.b.month],
                ['Last 3 months', pct(c.a.quarter), pct(c.b.quarter), c.a.quarter, c.b.quarter],
                [`Last ${c.sessions} days`, pct(c.a.window), pct(c.b.window), c.a.window, c.b.window],
              ].map(([label, av, bv, an, bn]) => (
                <tr key={label as string} className="border-b border-line/60">
                  <td className="py-1 font-sans text-ink-dim">{label as string}</td>
                  <td className={`py-1 text-right ${tone(an as number | null)}`}>{av as string}</td>
                  <td className={`py-1 text-right ${tone(bn as number | null)}`}>{bv as string}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="text-micro text-ink-mute">
            <Term word="correlation">Correlation</Term> of their daily moves: {c.correlation == null ? '—' : formatNumber(c.correlation, { decimals: 2 })}, so {correlationWords(c.correlation)}. From daily closes only.
          </p>
        </>
      )}
    </section>
  );
}
