'use client';

/**
 * Scan: where the market is moving and which stocks to look at. Three blocks in reading order: how broad today's move
 * is (a sentence and three figures), the sectors as equal tiles coloured by size of move with the share of stocks that
 * agree, and a sortable list of stocks. Tiles are the same size on purpose: market values of foreign listings are not
 * reliable enough to size a map by. The first load of breadth and sectors can take about half a minute while the server
 * builds them, so those requests wait up to 60 seconds and say so.
 */

import React from 'react';
import useSWR from 'swr';
import { apiClient, fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { barLabel } from '../lib/candles';
import { formatCurrency, formatNumber } from '../lib/format';
import { rankWords } from '../lib/ideas';
import { bigMoney } from '../lib/snapshot';
import {
  SECTORS,
  SIZES,
  SORTS,
  eventWords,
  groupSentence,
  moveStep,
  screenerUrl,
  isBuilding,
  buildingWait,
  sessionNote,
  signedPct,
  volumeWords,
  weatherFigures,
  weatherSentence,
  type Breadth,
  type HeatGroup,
  type Heatmap,
  type Screener,
  type SortKey,
} from '../lib/scan';
import { StockLink } from './StockDrawer';
import { closesKey, type Closes } from '../lib/spark';
import { InlineSpark } from './ui/InlineSpark';
import EarningsWeek from './EarningsWeek';
import { Button } from './ui/Button';
import { POLL } from './ui/DataProvider';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

const day = (iso: string) => barLabel(`${iso.slice(0, 10)}T00:00:00Z`, '1d');

/** These two are built on first request and cached, so the first answer can take about half a minute. */
const slow = <T,>(url: string): Promise<T> => apiClient.get(url, { timeout: 60_000 }).then((r) => r.data as T);

/** Retry a route the server is still building for the first time after the wait it asks for; other errors use the usual back-off. */
const retryWhenWarm = (err: unknown, _key: string, _config: unknown, revalidate: (o: { retryCount: number }) => void, o: { retryCount: number }) => {
  const wait = buildingWait(err);
  // A 503 is the server still building it: keep asking, up to about ten times. Anything else gets three tries.
  if (o.retryCount >= (wait == null ? 3 : 10)) return;
  window.setTimeout(() => revalidate({ retryCount: o.retryCount + 1 }), (wait ?? 10) * 1000);
};

const MIX = [0, 12, 24, 38];

function tileStyle(pct: number | null): React.CSSProperties {
  const step = moveStep(pct);
  if (step === 0) return {};
  const hue = step > 0 ? 'var(--positive)' : 'var(--negative)';
  return { backgroundColor: `color-mix(in srgb, ${hue} ${MIX[Math.abs(step)]}%, var(--bg-raised))` };
}

function Weather() {
  const { data, error } = useSWR<Breadth>('/market/breadth?days=60', slow, { refreshInterval: POLL.rare, revalidateOnFocus: false, onErrorRetry: retryWhenWarm });
  const last = data?.series?.length ? data.series[data.series.length - 1] : null;
  const sentence = weatherSentence(last);
  const figures = weatherFigures(last);
  return (
    <section aria-label="How broad the market move is" className="space-y-2 rounded-md border border-line bg-raised p-4">
      <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">How broad is the move</h2>
      {failedWithout(error, data) && !isBuilding(error) ? (
        <PanelError what="the market breadth" inline />
      ) : !data ? (
        <LoadingLines lines={2} label="Working out how broad the market move is. The first load can take about half a minute." />
      ) : !sentence ? (
        <p role="status" className="text-xs text-ink-mute">{data.note ?? 'No completed session to read yet.'}</p>
      ) : (
        <>
          <p className="text-lead font-semibold text-ink">{sentence}</p>
          <dl className="flex flex-wrap gap-x-8 gap-y-2">
            {figures.map((f) => (
              <div key={f.label} title={f.title}>
                <dt className="text-micro text-ink-mute">{f.label}</dt>
                <dd className="font-mono text-sm tabular-nums text-ink">{f.value}</dd>
              </div>
            ))}
          </dl>
          <BreadthLine series={data.series} />
          <p className="text-micro text-ink-mute">
            {last ? `Session of ${day(last.day)}` : ''}, among {data.universe != null ? formatNumber(data.universe, { decimals: 0 }) : 'the'} stocks worth $1 billion or more.
            {data.dropped_thin.length > 0 ? ` ${data.dropped_thin.length} session${data.dropped_thin.length === 1 ? '' : 's'} left out because too few stocks reported.` : ''}
          </p>
        </>
      )}
    </section>
  );
}

/** The share of stocks up on each of the last sessions, with the 50% line: a run of low days is easy to see. */
function BreadthLine({ series }: { series: Breadth['series'] }) {
  const pts = series.filter((d) => d.share_up != null);
  if (pts.length < 3) return null;
  const W = 280;
  const H = 44;
  const x = (i: number) => 3 + (i / (pts.length - 1)) * (W - 6);
  const y = (v: number) => H - 3 - v * (H - 6);
  const d = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)} ${y(p.share_up as number).toFixed(1)}`).join(' ');
  const last = pts[pts.length - 1];
  return (
    <figure className="space-y-1">
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`Share of stocks up each day over the last ${pts.length} sessions, latest ${Math.round((last.share_up as number) * 100)}%`} className="h-11 w-full max-w-sm" preserveAspectRatio="xMinYMid meet">
        <line x1="3" x2={W - 3} y1={y(0.5)} y2={y(0.5)} stroke="var(--border-strong)" strokeDasharray="3 3" strokeWidth="1" />
        <path d={d} fill="none" stroke="var(--text-secondary)" strokeWidth="1.5" />
        <circle cx={x(pts.length - 1)} cy={y(last.share_up as number)} r="3" fill="var(--accent)" />
      </svg>
      <figcaption className="text-micro text-ink-mute">Share of stocks up each day, last {pts.length} sessions. The dashed line is half.</figcaption>
    </figure>
  );
}

function Tile({ g }: { g: HeatGroup }) {
  const step = moveStep(g.change_pct);
  const up = g.breadth_up_share;
  return (
    <li className="min-w-0 space-y-1 rounded-sm border border-line p-3" style={tileStyle(g.change_pct)} title={groupSentence(g) ?? 'Too few priced stocks for statistics.'}>
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-xs font-semibold leading-tight text-ink">{g.label}</span>
        <span className="shrink-0 font-mono text-xs tabular-nums text-ink">
          {g.change_pct == null ? '—' : `${step > 0 ? '▲' : step < 0 ? '▼' : '■'} ${signedPct(g.change_pct)}`}
        </span>
      </div>
      {up != null && (
        <div className="space-y-0.5">
          <div className="h-1 rounded-sm bg-line" role="presentation">
            <div className="h-1 rounded-sm bg-ink-dim" style={{ width: `${Math.round(up * 100)}%` }} />
          </div>
          <p className="text-micro text-ink-dim">{Math.round(up * 100)}% of {g.n} stocks up</p>
        </div>
      )}
      {g.top.length > 0 && (
        <p className="truncate text-micro text-ink-mute">
          Best {g.top[0].ticker} {signedPct(g.top[0].change_pct, 1)}
          {g.bottom.length > 0 ? ` · worst ${g.bottom[0].ticker} ${signedPct(g.bottom[0].change_pct, 1)}` : ''}
        </p>
      )}
    </li>
  );
}

function SectorMap() {
  const [by, setBy] = React.useState<'sector' | 'industry'>('sector');
  const { data, error } = useSWR<Heatmap>(`/market/heatmap?by=${by}`, slow, { refreshInterval: POLL.standard, revalidateOnFocus: false, onErrorRetry: retryWhenWarm });
  return (
    <section aria-label="Sectors" className="space-y-2 rounded-md border border-line bg-raised p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">{by === 'sector' ? 'Sectors' : 'Industries'}</h2>
        <div role="group" aria-label="Group by" className="flex gap-1.5">
          <Button variant="outline" size="sm" pressed={by === 'sector'} onClick={() => setBy('sector')}>
            Sectors
          </Button>
          <Button variant="outline" size="sm" pressed={by === 'industry'} onClick={() => setBy('industry')} title="The finer groups from the SEC's own industry text, with at least five stocks each">
            Industries
          </Button>
        </div>
      </div>
      {failedWithout(error, data) && !isBuilding(error) ? (
        <PanelError what="the sector moves" inline />
      ) : !data ? (
        <LoadingLines lines={4} label="Building the sector view. The first load can take about half a minute." />
      ) : data.groups.length === 0 ? (
        <p role="status" className="text-xs text-ink-mute">{data.note ?? 'No sector data yet.'}</p>
      ) : (
        <>
          <ul className="grid grid-cols-[repeat(auto-fill,minmax(13rem,1fr))] gap-2">
            {data.groups.map((g) => (
              <Tile key={g.key} g={g} />
            ))}
          </ul>
          <p className="text-micro text-ink-mute">
            {sessionNote(data.session)} Sectors are grouped from SEC industry codes, an approximation of the usual market sectors, so a company can sit somewhere you would not expect. Each tile is the same size; colour steps are under 0.25%, 1%, 2% and above, with the arrow and sign carrying the direction.
          </p>
        </>
      )}
    </section>
  );
}

function StockList() {
  const [sort, setSort] = React.useState<SortKey>('change_pct');
  const [dir, setDir] = React.useState<'asc' | 'desc'>('desc');
  const [sizeIdx, setSizeIdx] = React.useState(1);
  const [sector, setSector] = React.useState<string | null>(null);
  const size = SIZES[sizeIdx];
  const { data, error } = useSWR<Screener>(screenerUrl(sort, dir, size.min, 100, sector), fetcher, { refreshInterval: POLL.standard, revalidateOnFocus: false });
  // One request for every row's 60-day trend line; a failure leaves dashes, never blocks the table.
  const spark = useSWR<Closes>(closesKey((data?.rows ?? []).map((r) => r.ticker)), fetcher, { revalidateOnFocus: false, shouldRetryOnError: false });
  return (
    <section aria-label="Stocks" className="space-y-3 border-t border-line pt-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Stocks</h2>
        <div className="flex flex-wrap items-center gap-2">
          <div role="group" aria-label="Sort by" className="flex flex-wrap gap-1.5">
            {SORTS.map((s) => (
              <Button key={s.key} variant="outline" size="sm" pressed={sort === s.key} onClick={() => { setSort(s.key); setDir(s.key === 'next_event' ? 'asc' : 'desc'); }}>
                {s.label}
              </Button>
            ))}
          </div>
          <Button variant="outline" size="sm" onClick={() => setDir(dir === 'desc' ? 'asc' : 'desc')} title="Reverse the order">
            {sort === 'next_event' ? (dir === 'asc' ? 'Soonest first' : 'Latest first') : dir === 'desc' ? 'Highest first' : 'Lowest first'}
          </Button>
          <div role="group" aria-label="Company size" className="flex flex-wrap gap-1.5">
            {SIZES.map((s, i) => (
              <Button key={s.label} variant="outline" size="sm" pressed={sizeIdx === i} onClick={() => setSizeIdx(i)}>
                {s.label}
              </Button>
            ))}
          </div>
        </div>
      </div>
      <div role="group" aria-label="Sector" className="flex flex-wrap gap-1.5">
        <Button variant="outline" size="sm" pressed={sector === null} onClick={() => setSector(null)}>
          All sectors
        </Button>
        {SECTORS.map((s) => (
          <Button key={s} variant="outline" size="sm" pressed={sector === s} onClick={() => setSector(s)}>
            {s}
          </Button>
        ))}
      </div>
      {failedWithout(error, data) ? (
        <PanelError what="the stock list" inline />
      ) : !data ? (
        <LoadingLines lines={6} label="Loading the stock list" />
      ) : data.rows.length === 0 ? (
        <p role="status" className="text-xs text-ink-mute">No stocks match.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[640px] text-xs">
            <thead className="text-ink-mute">
              <tr className="border-b border-line text-left">
                <th className="py-1.5 font-normal">Stock</th>
                <th className="py-1.5 text-right font-normal">Price</th>
                <th className="py-1.5 text-right font-normal">Day change</th>
                <th className="py-1.5 pl-4 text-right font-normal" title="Daily closes over the last 60 sessions">60 days</th>
                <th className="py-1.5 text-right font-normal" title="Volume of the last full session compared with its average over the 20 sessions before">Volume vs usual</th>
                <th className="py-1.5 text-right font-normal" title={data.model_as_of ? `The model's ranking, as of ${day(data.model_as_of)}` : undefined}>Model view</th>
                <th className="py-1.5 text-right font-normal">Next event</th>
                <th className="py-1.5 text-right font-normal">Size</th>
              </tr>
            </thead>
            <tbody className="font-mono tabular-nums">
              {data.rows.map((r) => (
                <tr key={r.ticker} className="border-b border-line/60 hover:bg-overlay">
                  <td className="py-1.5 text-left font-sans">
                    <StockLink ticker={r.ticker} className="font-mono font-semibold text-ink hover:text-accent" />
                  </td>
                  <td className="py-1.5 text-right">{r.price != null ? formatCurrency(r.price, { decimals: 2 }) : '—'}</td>
                  <td className={`py-1.5 text-right ${r.change_pct == null ? 'text-ink-mute' : r.change_pct >= 0 ? 'text-positive' : 'text-negative'}`}>
                    {r.change_pct == null ? '—' : `${r.change_pct >= 0 ? '▲' : '▼'} ${signedPct(r.change_pct)}`}
                  </td>
                  <td className="py-1.5 pl-4 text-right">
                    <InlineSpark closes={spark.data?.[r.ticker]} />
                  </td>
                  <td className="py-1.5 text-right">{volumeWords(r.volume_vs_20d) || '—'}</td>
                  <td className="py-1.5 text-right font-sans text-ink-dim">{r.model_percentile != null ? rankWords(r.model_percentile) : '—'}</td>
                  <td className="py-1.5 text-right font-sans text-ink-dim">{eventWords(r.next_event, day) || '—'}</td>
                  <td className="py-1.5 text-right">{bigMoney(r.market_cap) ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="pt-2 text-micro text-ink-mute">
            {data.rows.length} of {data.universe != null ? formatNumber(data.universe, { decimals: 0 }) : 'the'} stocks the model ranks.
            {data.model_as_of ? ` Model view is as of ${day(data.model_as_of)}.` : ''} Volume is the last completed session, not today so far.
          </p>
        </div>
      )}
    </section>
  );
}

export default function ScanBoard() {
  return (
    <div className="space-y-4">
      <Weather />
      <SectorMap />
      <EarningsWeek />
      <StockList />
    </div>
  );
}
