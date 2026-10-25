'use client';

/**
 * Every anomalous ticker of the last three days with every signal the platform
 * can compute for it (services/agents/dossier_engine.py, shared/utils/signal_matrix.py).
 *
 * Votes are counted by evidence tier and never blended: proven (this platform's
 * reaction study), supported (the cross-sectional model where its horizon
 * clears every out-of-sample bar but the deflated Sharpe), published (a
 * documented anomaly not yet measured here). Measured-empty signals and
 * descriptive context are shown and never counted.
 */

import React, { useMemo, useState } from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { rankWords } from '../lib/ideas';
import { LoadingLines } from './ui/Skeleton';

interface Count {
  bull: number;
  bear: number;
}

interface BoardRow {
  ticker: string;
  name: string | null;
  industry: string | null;
  price: number | null;
  ret_1d: number | null;
  ret_5d: number | null;
  anomaly_score: number;
  events_72h: number;
  triggers: string[];
  lean: number;
  net: number;
  votes: number;
  proven: Count;
  supported: Count;
  published: Count;
  weekly_pct: number | null;
  verdict?: string;
  confidence?: 'higher' | 'moderate' | 'low' | 'none';
  why?: string;
  cost_bps_1m: number | null;
  adv_usd: number | null;
  iv_regime_ratio: number | null;
  dtc: number | null;
  insider_net: number | null;
  signals: number;
}

interface Board {
  generated_at?: string;
  tickers?: number;
  model_as_of?: string;
  primary_horizon?: string;
  board: BoardRow[];
  note?: string;
}

export interface MatrixSignal {
  group: string;
  name: string;
  value: number | null;
  display: string;
  direction: number;
  status: 'proven' | 'supported' | 'published' | 'no_edge' | 'context';
  source: string;
  note: string;
  as_of: string | null;
  plain?: string;
  tier?: string;
  /** Everyday name and value (shared/utils/signal_plain.everyday). */
  label?: string;
  shown?: string;
}

export interface MatrixDoc {
  ticker: string;
  name?: string | null;
  industry?: string | null;
  price?: number | null;
  price_as_of?: string;
  signals: MatrixSignal[];
  tally?: { lean: number; net: number; weighted_votes: number; by_status: Record<string, Count> };
  verdict?: { headline: string; confidence: string; why: string };
  generated_at?: string;
  note?: string;
}

const STATUS_STYLE: Record<string, string> = {
  proven: 'border-positive/30 text-positive',
  supported: 'border-sky-400/60 text-sky-200',
  published: 'border-caution/30 text-caution',
  no_edge: 'border-line text-ink-mute line-through',
  context: 'border-line text-ink-dim',
};
const GROUP_ORDER = ['events', 'model', 'fundamentals', 'price', 'options', 'short', 'insider', 'filings', 'news', 'execution'];
const GROUP_LABEL: Record<string, string> = {
  events: 'What happened',
  model: 'Our model',
  fundamentals: 'The business',
  price: 'Price and trend',
  options: 'Options traders',
  short: 'Short sellers',
  insider: 'Company insiders',
  filings: 'Filings and earnings',
  news: 'News',
  execution: 'What it costs to trade',
};

/** What set a stock off, in words: the board's triggers are event type codes. */
const TRIGGER_WORD: Record<string, string> = {
  market_anomaly: 'unusual price move',
  equity_block: 'large block trade',
  options_flow: 'big options bet',
  insider_trade: 'insider trade',
  filing: 'company filing',
  news: 'news',
};
export const triggerWords = (t: string[]): string =>
  t.map((x) => TRIGGER_WORD[x] ?? x.replace(/_/g, ' ')).join(', ');

/** How unusual the activity is, in words, from the anomaly score (0-1). */
export function unusualWord(score: number): string {
  return score >= 0.9 ? 'Very high' : score >= 0.75 ? 'High' : score >= 0.5 ? 'Some' : 'Low';
}
const pct = (v: number | null | undefined, d = 1) => (v == null ? ABSENT : `${v >= 0 ? '+' : ''}${formatNumber(v * 100, { decimals: d })}%`);

/** Which way a verdict leans, from its headline: 1 bullish, -1 bearish, 0 neither. */
export function verdictSide(verdict: string | undefined): 1 | -1 | 0 {
  if (!verdict) return 0;
  return /bull/i.test(verdict) ? 1 : /bear/i.test(verdict) ? -1 : 0;
}

/** A verdict pill coloured by direction, filled by confidence: green up, red
 * down, grey for no lean. The confidence colouring it replaces painted a
 * moderate bullish and a moderate bearish call the same blue. */
const VERDICT_TONE: Record<'up' | 'down', Record<string, string>> = {
  up: {
    higher: 'bg-positive/15 border-positive/50 text-positive',
    moderate: 'bg-positive/5 border-positive/30 text-positive',
    low: 'border-positive/20 text-positive/70',
  },
  down: {
    higher: 'bg-negative/15 border-negative/50 text-negative',
    moderate: 'bg-negative/5 border-negative/30 text-negative',
    low: 'border-negative/20 text-negative/70',
  },
};

export function verdictClass(verdict: string | undefined, confidence: string | undefined): string {
  const side = verdictSide(verdict);
  if (!side) return 'border-line text-ink-mute';
  return VERDICT_TONE[side > 0 ? 'up' : 'down'][confidence ?? 'low'] ?? VERDICT_TONE[side > 0 ? 'up' : 'down'].low;
}

/** The verdict without its parenthesis: "Leans bullish (higher confidence)" reads "Leans bullish". */
const shortVerdict = (v: string) => v.replace(/\s*\(.*\)\s*$/, '');

/** One stock's counted votes as a bar: green up, red down, each tier its own strength. */
function EvidenceBar({ r }: { r: BoardRow }) {
  const parts: { n: number; cls: string; label: string }[] = [
    { n: r.proven.bull, cls: 'bg-positive', label: 'proven up' },
    { n: r.supported.bull, cls: 'bg-positive/60', label: 'model up' },
    { n: r.published.bull, cls: 'bg-positive/30', label: 'research up' },
    { n: r.published.bear, cls: 'bg-negative/30', label: 'research down' },
    { n: r.supported.bear, cls: 'bg-negative/60', label: 'model down' },
    { n: r.proven.bear, cls: 'bg-negative', label: 'proven down' },
  ];
  const total = parts.reduce((s, p) => s + p.n, 0);
  if (!total) return <div className="h-1.5 rounded-full bg-inset" aria-hidden />;
  return (
    <div
      className="flex h-1.5 overflow-hidden rounded-full bg-inset gap-px"
      role="img"
      aria-label={parts.filter((p) => p.n).map((p) => `${p.n} ${p.label}`).join(', ')}
    >
      {parts.map((p) => (p.n ? <div key={p.label} className={p.cls} style={{ flex: p.n }} /> : null))}
    </div>
  );
}

/** The strongest leans one way, as cards: the board's answer before its table. */
function Spotlight({ rows, side }: { rows: BoardRow[]; side: 1 | -1 }) {
  const up = side > 0;
  return (
    <div className="space-y-2 min-w-0">
      <div>
        <h3 className={`text-sm font-semibold ${up ? 'text-positive' : 'text-negative'}`}>
          {up ? 'Strongest bullish' : 'Strongest bearish'}
        </h3>
        <p className="text-micro text-ink-mute">Higher confidence and the most votes, among stocks over $5 that are cheap to trade.</p>
      </div>
      {rows.length === 0 ? (
        <p className="rounded-xl border border-dashed border-line p-3 text-micro text-ink-mute">
          Nothing leans this way with higher confidence right now.
        </p>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
          {rows.map((r) => (
            <Link
              key={r.ticker}
              href={`/ticker?symbol=${encodeURIComponent(r.ticker)}`}
              className={`group rounded-xl border border-line bg-inset/60 p-3 space-y-2 border-t-2 hover:border-line-strong ${
                up ? 'border-t-positive/60' : 'border-t-negative/60'
              }`}
              title={r.why}
            >
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <div className="font-mono font-semibold text-ink group-hover:text-accent">{r.ticker}</div>
                  <div className="truncate text-micro text-ink-mute">{r.name ?? r.industry ?? ''}</div>
                </div>
                <div className="text-right shrink-0">
                  <div className="font-mono text-sm text-ink">
                    {r.price != null ? `$${formatNumber(r.price, { decimals: 2 })}` : ABSENT}
                  </div>
                  <div className={`font-mono text-micro ${(r.ret_1d ?? 0) >= 0 ? 'text-positive' : 'text-negative'}`}>
                    {pct(r.ret_1d)} today
                  </div>
                </div>
              </div>
              <EvidenceBar r={r} />
              <div className="flex items-center justify-between gap-2 text-micro">
                <span className="truncate text-ink-dim">{triggerWords(r.triggers)}</span>
                <span className={`font-mono shrink-0 ${up ? 'text-positive' : 'text-negative'}`}>
                  {formatNumber(Math.abs(r.net), { decimals: 0 })} of {formatNumber(r.votes, { decimals: 0 })}{' '}
                  {up ? 'say up' : 'say down'}
                </span>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}

type Side = 'all' | 'bull' | 'bear';

/** The spotlight's floor: a price above penny-stock territory and a round trip
 * on $1M under 0.5%, so every card it shows can be traded as shown. */
const SPOTLIGHT_MIN_PRICE = 5;
const SPOTLIGHT_MAX_COST_BPS = 50;

function Votes({ c, tone }: { c: Count; tone: string }) {
  if (!c.bull && !c.bear) return <span className="text-ink-mute">{ABSENT}</span>;
  return (
    <span className={`font-mono ${tone}`}>
      {c.bull ? `+${c.bull}` : ''}
      {c.bull && c.bear ? '/' : ''}
      {c.bear ? `-${c.bear}` : ''}
    </span>
  );
}

export function MatrixList({ doc }: { doc: MatrixDoc }) {
  const groups = useMemo(() => {
    const g: Record<string, MatrixSignal[]> = {};
    for (const s of doc.signals) (g[s.group] ??= []).push(s);
    return GROUP_ORDER.filter((k) => g[k]).map((k) => [k, g[k]] as const);
  }, [doc.signals]);
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
      {groups.map(([group, sigs]) => (
        <div key={group} className="rounded-lg border border-line p-2">
          <h5 className="stat-label mb-1">{GROUP_LABEL[group] ?? group}</h5>
          <ul className="space-y-0.5">
            {sigs.map((s) => (
              <li key={`${group}-${s.name}`} className="text-micro border-b border-line/40 pb-1" title={[s.note, s.source].filter(Boolean).join(' · ')}>
                <div className="flex items-start justify-between gap-2">
                  <span className="text-ink-dim">{s.label ?? s.name}</span>
                  <span className="flex items-center gap-1 shrink-0">
                    <span
                      className={`font-mono ${s.direction > 0 ? 'text-positive' : s.direction < 0 ? 'text-negative' : 'text-ink'}`}
                    >
                      {s.shown ?? s.display}
                    </span>
                    <span className={`px-1 rounded border text-[10px] ${STATUS_STYLE[s.status] ?? ''}`}>
                      {s.tier ?? s.status.replace('_', ' ')}
                    </span>
                  </span>
                </div>
                {s.plain && <p className="text-ink-mute leading-snug">{s.plain}</p>}
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}

function VerdictLine({ v }: { v?: MatrixDoc['verdict'] }) {
  if (!v) return null;
  return (
    <p className="text-xs">
      <span className={`px-1.5 py-0.5 rounded border mr-2 ${verdictClass(v.headline, v.confidence)}`}>{v.headline}</span>
      <span className="text-ink-dim">{v.why}</span>
    </p>
  );
}

function Expanded({ ticker }: { ticker: string }) {
  const { data } = useSWR<MatrixDoc>(`/signals/matrix/${encodeURIComponent(ticker)}`, fetcher);
  if (!data) return <LoadingLines />;
  if (!data.signals?.length) return <p className="text-micro text-ink-mute p-2">{data.note ?? 'No signals.'}</p>;
  return (
    <div className="p-2 space-y-2">
      <VerdictLine v={data.verdict} />
      <MatrixList doc={data} />
    </div>
  );
}

type SortKey = 'net' | 'anomaly_score' | 'ret_1d' | 'weekly_pct' | 'cost_bps_1m';

export function SignalBoard() {
  const { data, error } = useSWR<Board>('/signals/matrix', fetcher, { refreshInterval: POLL.standard });
  const [q, setQ] = useState('');
  const [onlyCounted, setOnlyCounted] = useState(false);
  const [side, setSide] = useState<Side>('all');
  const [sort, setSort] = useState<SortKey>('net');
  const [open, setOpen] = useState<string | null>(null);
  const [limit, setLimit] = useState(40);

  const board = useMemo(() => data?.board ?? [], [data]);
  const tally = useMemo(() => {
    let bull = 0;
    let bear = 0;
    let firm = 0;
    let proven = 0;
    for (const r of board) {
      const s = verdictSide(r.verdict);
      if (s > 0) bull += 1;
      if (s < 0) bear += 1;
      if (s && r.confidence === 'higher') firm += 1;
      if (r.proven.bull + r.proven.bear > 0) proven += 1;
    }
    return { bull, bear, firm, proven };
  }, [board]);

  const spotlight = useMemo(() => {
    // Tradeable names only: a $0.02 stock costing 26% to trade at $1M leans
    // hard and cannot be acted on. They stay in the table below.
    const firm = board.filter(
      (r) =>
        r.confidence === 'higher' &&
        (r.price ?? 0) >= SPOTLIGHT_MIN_PRICE &&
        r.cost_bps_1m != null &&
        r.cost_bps_1m <= SPOTLIGHT_MAX_COST_BPS,
    );
    const pick = (s: 1 | -1) =>
      firm
        .filter((r) => verdictSide(r.verdict) === s)
        .sort((a, b) => Math.abs(b.net) - Math.abs(a.net) || b.anomaly_score - a.anomaly_score)
        .slice(0, 4);
    return { bull: pick(1), bear: pick(-1) };
  }, [board]);

  const rows = useMemo(() => {
    const needle = q.trim().toUpperCase();
    const list = board.filter(
      (r) =>
        (!needle || r.ticker.includes(needle) || (r.name ?? '').toUpperCase().includes(needle) || (r.industry ?? '').toUpperCase().includes(needle)) &&
        (!onlyCounted || r.proven.bull + r.proven.bear + r.supported.bull + r.supported.bear > 0) &&
        (side === 'all' || verdictSide(r.verdict) === (side === 'bull' ? 1 : -1)),
    );
    const val = (r: BoardRow) => {
      if (sort === 'net') return Math.abs(r.net);
      const v = r[sort];
      return v == null ? -Infinity : sort === 'cost_bps_1m' ? -v : v;
    };
    return [...list].sort((a, b) => val(b) - val(a));
  }, [board, q, onlyCounted, side, sort]);

  const th = (key: SortKey, label: string) => (
    <th className="text-right font-normal">
      <button type="button" onClick={() => setSort(key)} className={sort === key ? 'text-accent' : 'hover:text-ink'}>
        {label}
        {sort === key ? ' ↓' : ''}
      </button>
    </th>
  );

  const sideBtn = (key: Side, label: string, n: number, tone: string) => (
    <button
      type="button"
      onClick={() => {
        setSide(key);
        setLimit(40);
      }}
      aria-pressed={side === key}
      className={`rounded-md px-3 py-1 text-xs ${side === key ? `bg-overlay ${tone} font-semibold` : 'text-ink-dim hover:text-ink'}`}
    >
      {label} <span className="font-mono text-micro opacity-80">{formatNumber(n, { decimals: 0 })}</span>
    </button>
  );

  return (
    <section className="bg-raised border border-line rounded-2xl p-4 sm:p-5 space-y-5 text-xs">
      <div className="flex items-start justify-between gap-3 flex-wrap">
        <div>
          <h2 className="text-head font-semibold text-ink">Stocks doing something unusual</h2>
          <p className="text-ink-mute">
            Every stock with unusual trading, filings or insider moves in the last three days, and which way the
            evidence leans.
          </p>
        </div>
        {data && (
          <div className="grid grid-cols-4 gap-2 text-center">
            {[
              { n: board.length, label: 'stocks', tone: 'text-ink' },
              { n: tally.bull, label: 'lean up', tone: 'text-positive' },
              { n: tally.bear, label: 'lean down', tone: 'text-negative' },
              { n: tally.proven, label: 'backed by tested patterns', tone: 'text-accent' },
            ].map((s) => (
              <div key={s.label} className="rounded-xl border border-line bg-inset px-3 py-2 min-w-[84px]">
                <div className={`font-mono text-lead leading-none ${s.tone}`}>{formatNumber(s.n, { decimals: 0 })}</div>
                <div className="mt-1 text-micro text-ink-mute">{s.label}</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {error ? (
        <p className="text-ink-mute">The board could not be read.</p>
      ) : !data ? (
        <div className="grid gap-3 lg:grid-cols-2">
          {[0, 1].map((n) => (
            <div key={n} className="h-48 rounded-xl border border-line bg-inset animate-pulse" />
          ))}
        </div>
      ) : !board.length ? (
        <p className="text-ink-mute">{data.note ?? 'No anomalous tickers.'}</p>
      ) : (
        <>
          <div className="grid gap-4 lg:grid-cols-2">
            <Spotlight rows={spotlight.bull} side={1} />
            <Spotlight rows={spotlight.bear} side={-1} />
          </div>

          <div className="flex items-center justify-between gap-2 flex-wrap border-t border-line pt-4">
            <div className="flex rounded-lg border border-line bg-inset p-0.5" role="group" aria-label="Filter by lean">
              {sideBtn('all', 'All', board.length, 'text-ink')}
              {sideBtn('bull', 'Bullish', tally.bull, 'text-positive')}
              {sideBtn('bear', 'Bearish', tally.bear, 'text-negative')}
            </div>
            <div className="flex items-center gap-3 flex-wrap">
              <label className="flex items-center gap-1.5 text-ink-dim">
                <input type="checkbox" checked={onlyCounted} onChange={(e) => setOnlyCounted(e.target.checked)} />
                Only well-tested evidence
              </label>
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Ticker, name or industry"
                aria-label="Filter tickers"
                className="bg-page border border-line rounded-md px-2.5 py-1.5 text-ink w-52"
              />
            </div>
          </div>

          <details className="rounded-lg border border-line bg-inset/50 px-3 py-2 text-ink-dim">
            <summary className="cursor-pointer text-ink">How to read the board</summary>
            <div className="space-y-1 pt-2">
              <p>Only three kinds of signal count toward a verdict, shown on each bar from strongest to weakest:</p>
              <ul className="list-disc pl-5 space-y-0.5">
                <li>
                  <span className="text-ink">Proven here</span> (solid): our own five-year testing shows the pattern has reliably moved prices.
                </li>
                <li>
                  <span className="text-ink">Model-backed</span> (medium): our ranking model, tested on years of data it never saw, found an edge.
                </li>
                <li>
                  <span className="text-ink">Research-backed</span> (faint): documented in academic research, not yet measured by us.
                </li>
              </ul>
              <p>
                Green votes say up, red say down. Signals marked &ldquo;Tested: doesn&apos;t work&rdquo; or &ldquo;Background&rdquo; are shown
                when you open a row and never counted. A verdict is a lean, not a forecast: even the best signals here are right
                only somewhat more often than not.
              </p>
            </div>
          </details>

          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead className="text-micro text-ink-mute">
                <tr className="border-b border-line">
                  <th className="py-2 font-normal">Stock</th>
                  <th className="font-normal">Verdict</th>
                  <th className="text-right font-normal">Price</th>
                  {th('ret_1d', 'Today')}
                  <th className="text-right font-normal">5 days</th>
                  <th className="pl-4 font-normal w-40">Up vs down</th>
                  {th('net', 'Evidence')}
                  {th('weekly_pct', 'Our model (week)')}
                  {th('cost_bps_1m', 'Cost to trade $1M')}
                  {th('anomaly_score', 'How unusual')}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, limit).map((r) => (
                  <React.Fragment key={r.ticker}>
                    <tr
                      className={`border-b border-line/60 cursor-pointer hover:bg-overlay/60 ${open === r.ticker ? 'bg-overlay/60' : ''}`}
                      onClick={() => setOpen(open === r.ticker ? null : r.ticker)}
                    >
                      <td className="py-2">
                        <Link
                          href={`/ticker?symbol=${encodeURIComponent(r.ticker)}`}
                          onClick={(e) => e.stopPropagation()}
                          className="font-mono font-semibold text-ink hover:text-accent hover:underline"
                        >
                          {r.ticker}
                        </Link>
                        <div className="text-micro text-ink-mute truncate max-w-[200px]" title={r.industry ?? ''}>
                          {r.name ?? r.industry ?? ''}
                        </div>
                      </td>
                      <td title={r.why}>
                        {r.verdict ? (
                          <span className={`px-2 py-0.5 rounded-full border text-micro whitespace-nowrap ${verdictClass(r.verdict, r.confidence)}`}>
                            {shortVerdict(r.verdict)}
                            {r.confidence && r.confidence !== 'none' && (
                              <span className="opacity-70"> · {r.confidence}</span>
                            )}
                          </span>
                        ) : (
                          ABSENT
                        )}
                      </td>
                      <td className="text-right font-mono text-ink">{r.price != null ? formatNumber(r.price, { decimals: 2 }) : ABSENT}</td>
                      <td className={`text-right font-mono ${(r.ret_1d ?? 0) >= 0 ? 'text-positive' : 'text-negative'}`}>{pct(r.ret_1d)}</td>
                      <td className={`text-right font-mono ${(r.ret_5d ?? 0) >= 0 ? 'text-positive/80' : 'text-negative/80'}`}>{pct(r.ret_5d)}</td>
                      <td className="pl-4">
                        <EvidenceBar r={r} />
                        <div className="mt-1 flex gap-2 text-micro text-ink-mute">
                          <span title="Proven here">
                            <Votes c={r.proven} tone="text-ink" />
                          </span>
                          <span title="Model-backed">
                            <Votes c={r.supported} tone="text-ink-dim" />
                          </span>
                          <span title="Research-backed">
                            <Votes c={r.published} tone="text-ink-mute" />
                          </span>
                        </div>
                      </td>
                      <td className={`text-right font-mono font-semibold ${r.net > 0 ? 'text-positive' : r.net < 0 ? 'text-negative' : 'text-ink-mute'}`}>
                        {r.net > 0 ? '+' : ''}
                        {formatNumber(r.net, { decimals: 0 })}
                        <span className="text-ink-mute font-normal"> of {formatNumber(r.votes, { decimals: 0 })}</span>
                      </td>
                      <td className="text-right font-mono text-ink-dim">
                        {r.weekly_pct != null ? rankWords(r.weekly_pct) : ABSENT}
                      </td>
                      <td className="text-right font-mono text-ink-dim">
                        {r.cost_bps_1m != null ? `${formatNumber(r.cost_bps_1m / 100, { decimals: 2 })}%` : ABSENT}
                      </td>
                      <td className="text-right text-ink-dim" title={`Unusual-activity score ${formatNumber(r.anomaly_score, { decimals: 2 })} of 1`}>
                        {unusualWord(r.anomaly_score)}
                      </td>
                    </tr>
                    {open === r.ticker && (
                      <tr>
                        <td colSpan={10} className="bg-inset/40">
                          <Expanded ticker={r.ticker} />
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
            {rows.length > limit && (
              <div className="pt-3 text-center">
                <button
                  type="button"
                  onClick={() => setLimit((n) => n + 60)}
                  className="rounded-full border border-line px-4 py-1.5 text-accent hover:bg-overlay"
                >
                  Show more ({formatNumber(rows.length - limit, { decimals: 0 })} left)
                </button>
              </div>
            )}
          </div>
        </>
      )}
      {data?.generated_at && (
        <p className="text-micro text-ink-mute">
          {formatNumber(data.tickers ?? 0, { decimals: 0 })} tickers · built {formatAge(data.generated_at)} ago · model as of{' '}
          {data.model_as_of ?? ABSENT}. Click a row for every signal in plain English.
        </p>
      )}
    </section>
  );
}

export function TickerSignalMatrix({ ticker }: { ticker: string }) {
  const { data } = useSWR<MatrixDoc>(ticker ? `/signals/matrix/${encodeURIComponent(ticker)}` : null, fetcher, {
    refreshInterval: POLL.slow,
  });
  return (
    <div className="rounded-lg border border-line p-3 space-y-2">
      <div className="flex items-center justify-between">
        <span className="stat-label">Every signal, in plain English</span>
        {data?.tally && (
          <span className="text-micro font-mono text-ink-dim">
            net {data.tally.net > 0 ? '+' : ''}
            {formatNumber(data.tally.net, { decimals: 0 })} of {formatNumber(data.tally.weighted_votes, { decimals: 0 })} weighted votes
          </span>
        )}
      </div>
      {!data ? (
        <LoadingLines />
      ) : !data.signals?.length ? (
        <p className="text-micro text-ink-mute" title={data.note}>
          Nothing unusual has happened around {ticker} in the last three days, so there is no signal breakdown yet.
        </p>
      ) : (
        <>
          <VerdictLine v={data.verdict} />
          <MatrixList doc={data} />
        </>
      )}
    </div>
  );
}

export default SignalBoard;
