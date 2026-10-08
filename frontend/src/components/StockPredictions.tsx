'use client';

/**
 * Stock predictions: every directional call the agents have made on a listed
 * stock, open and resolved, with the record that decides what any of them is worth.
 *
 * Read from /predictions/stocks (services/api_gateway/routes/predictions.py).
 * The hit rate is shown shrunk toward a coin flip with a 90% interval, because
 * a raw rate on a few dozen calls is mostly noise; the calibration table shows
 * whether a stated conviction has matched what happened.
 */

import React, { useMemo, useState } from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { ConvergingEvidence } from './ConvergingEvidence';
import { IconCheck, IconClose } from './ui/icons';
import { ModelRanking } from './ModelRanking';
import { TradeIdeas } from './TradeIdeas';

interface OpenCall {
  /** 'mismatch' when the recorded entry is not this stock's price. */
  entry_check: 'ok' | 'mismatch' | 'no_entry' | 'unverified';
  prediction_id: string;
  agent: string;
  ticker: string;
  direction: 'up' | 'down' | 'flat';
  conviction: number | null;
  entry_price: number | null;
  target_price: number | null;
  price: number | null;
  move_in_favour_pct: number | null;
  progress_to_target: number | null;
  horizon_hours: number | null;
  hours_left: number | null;
  created_at: string;
  evidence_status: 'proven' | 'unproven' | null;
  trigger_families: string[];
}

interface Band {
  band: string;
  calls: number;
  mean_conviction: number | null;
  hit_rate: number | null;
}

interface Summary {
  resolved: number;
  excluded_invalid: number;
  hits: number;
  raw_hit_rate: number | null;
  shrunk_hit_rate: number | null;
  interval_90: [number, number] | null;
  brier: number | null;
  calibration: Band[];
}

interface Resolved {
  entry_check?: 'ok' | 'mismatch' | 'no_entry' | 'unverified';
  prediction_id: string;
  agent_name: string | null;
  ticker: string;
  direction: string;
  confidence: number | null;
  entry_price: number | null;
  resolved_at: string;
  outcome_correct: boolean | null;
}

interface PredictionsResponse {
  generated_at: string;
  window_days: number;
  open: OpenCall[];
  summary: Summary;
  by_agent: Record<string, Summary>;
  recent: Resolved[];
}

function pct(v: number | null | undefined, digits = 0): string {
  return v == null ? ABSENT : `${formatNumber(v * 100, { decimals: digits })}%`;
}

export function TickerLink({ ticker }: { ticker: string }) {
  return (
    <Link
      href={`/ticker?symbol=${encodeURIComponent(ticker)}`}
      className="font-mono font-bold text-white hover:text-accent underline-offset-2 hover:underline"
    >
      {ticker}
    </Link>
  );
}

function Stat({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-inset border border-line rounded-lg p-3">
      <div className="stat-label">{label}</div>
      <div className="text-lg font-bold text-white font-mono">{value}</div>
      {sub && <div className="text-micro text-ink-mute">{sub}</div>}
    </div>
  );
}

function Progress({ value }: { value: number | null }) {
  if (value == null) return <span className="text-ink-mute">{ABSENT}</span>;
  const pctWidth = Math.max(0, Math.min(100, value * 100));
  const tone = value >= 1 ? 'bg-emerald-400' : value >= 0 ? 'bg-cyan-400' : 'bg-rose-400';
  return (
    <div className="flex items-center gap-2" title={`${formatNumber(value * 100, { decimals: 0 })}% of the way to target`}>
      <div className="h-1.5 w-20 rounded bg-line overflow-hidden">
        <div className={`h-full ${tone}`} style={{ width: `${value < 0 ? 100 : pctWidth}%`, opacity: value < 0 ? 0.5 : 1 }} />
      </div>
      <span className="font-mono text-micro text-ink-dim">{formatNumber(value * 100, { decimals: 0 })}%</span>
    </div>
  );
}

export default function StockPredictions() {
  const [days, setDays] = useState(30);
  const [filter, setFilter] = useState('');
  const { data, error } = useSWR<PredictionsResponse>(`/predictions/stocks?days=${days}`, fetcher, {
    refreshInterval: POLL.standard,
  });

  const open = useMemo(() => {
    const q = filter.trim().toUpperCase();
    return (data?.open ?? []).filter((c) => !q || c.ticker.includes(q) || (c.agent || '').toUpperCase().includes(q));
  }, [data, filter]);
  const s = data?.summary;

  return (
    <div className="h-full overflow-y-auto space-y-4 text-xs">
      <section className="bg-raised border border-line rounded-xl p-4 space-y-3">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <h2 className="text-sm font-bold text-white">Stock predictions</h2>
          <div className="flex gap-1">
            {[7, 30, 90].map((d) => (
              <button
                key={d}
                onClick={() => setDays(d)}
                aria-pressed={days === d}
                className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium ${
                  days === d ? 'border-line-accent text-accent' : 'border-line text-ink-mute hover:text-ink-dim'
                }`}
              >
                {d}d
              </button>
            ))}
          </div>
        </div>
        <p className="text-ink-dim">
          Every directional call an agent has made on a listed stock. The hit rate is shown shrunk toward a coin flip
          with its 90% range, because a raw rate on a few dozen calls is mostly noise. Calls marked unproven rest on
          evidence the reaction study has not yet validated.
        </p>
        {error ? (
          <p className="text-ink-mute">The predictions record could not be read.</p>
        ) : (
          <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
            <Stat label="Open calls" value={formatNumber(data?.open.length ?? null, { decimals: 0 })} />
            <Stat
              label={`Resolved, ${days}d`}
              value={formatNumber(s?.resolved ?? null, { decimals: 0 })}
              sub={s ? `${s.hits} hit${s.excluded_invalid ? ` · ${s.excluded_invalid} invalid excluded` : ''}` : undefined}
            />
            <Stat
              label="Hit rate (shrunk)"
              value={pct(s?.shrunk_hit_rate)}
              sub={s?.interval_90 ? `90% range ${pct(s.interval_90[0])}–${pct(s.interval_90[1])} · raw ${pct(s.raw_hit_rate)}` : undefined}
            />
            <Stat label="Brier score" value={s?.brier != null ? formatNumber(s.brier, { decimals: 3 }) : ABSENT} sub="0.25 = coin flip; lower is better" />
            <Stat
              label="Edge vs coin flip"
              value={s?.interval_90 ? (s.interval_90[0] > 0.5 ? 'yes' : s.interval_90[1] < 0.5 ? 'worse' : 'not shown') : ABSENT}
              sub="whether the 90% range clears 50%"
            />
          </div>
        )}
      </section>

      <TradeIdeas />
      <ModelRanking />

      <section className="bg-raised border border-line rounded-xl p-4 space-y-2">
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <h3 className="font-bold text-white">Open calls</h3>
          <input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter ticker or agent"
            aria-label="Filter open calls"
            className="w-44 rounded-md border border-line bg-page px-2 py-1 font-mono text-micro text-ink outline-none focus:border-line-accent"
          />
        </div>
        {open.length === 0 ? (
          <p className="text-ink-mute">No open stock calls{filter ? ' match the filter' : ''}.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead className="text-ink-mute">
                <tr className="border-b border-line">
                  <th className="py-1.5 pr-2">Ticker</th>
                  <th className="pr-2">Call</th>
                  <th className="pr-2 text-right">Conviction</th>
                  <th className="pr-2 text-right">Entry → target</th>
                  <th className="pr-2 text-right">Now</th>
                  <th className="pr-2 text-right">In favour</th>
                  <th className="pr-2">Progress</th>
                  <th className="pr-2 text-right">Left</th>
                  <th className="pr-2">Evidence</th>
                  <th>Agent</th>
                </tr>
              </thead>
              <tbody>
                {open.map((c) => (
                  <tr
                    key={c.prediction_id}
                    className={`border-b border-line/60 ${c.entry_check === 'mismatch' || c.entry_check === 'no_entry' ? 'opacity-40' : ''}`}
                    title={c.entry_check === 'mismatch' ? "Recorded entry is not this stock's price; excluded from the record" : undefined}
                  >
                    <td className="py-1.5 pr-2">
                      <TickerLink ticker={c.ticker} />
                    </td>
                    <td className={`pr-2 font-bold ${c.direction === 'up' ? 'text-emerald-300' : c.direction === 'down' ? 'text-rose-300' : 'text-ink-dim'}`}>
                      {c.direction === 'up' ? 'LONG' : c.direction === 'down' ? 'SHORT' : 'FLAT'}
                    </td>
                    <td className="pr-2 text-right font-mono">{pct(c.conviction)}</td>
                    <td className="pr-2 text-right font-mono text-ink-dim">
                      {c.entry_price != null ? formatNumber(c.entry_price, { decimals: 2 }) : ABSENT} →{' '}
                      {c.target_price != null ? formatNumber(c.target_price, { decimals: 2 }) : ABSENT}
                    </td>
                    <td className="pr-2 text-right font-mono">{c.price != null ? formatNumber(c.price, { decimals: 2 }) : ABSENT}</td>
                    <td className={`pr-2 text-right font-mono ${c.move_in_favour_pct == null ? '' : c.move_in_favour_pct >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                      {c.move_in_favour_pct == null ? ABSENT : `${c.move_in_favour_pct >= 0 ? '+' : ''}${formatNumber(c.move_in_favour_pct, { decimals: 2 })}%`}
                    </td>
                    <td className="pr-2">
                      <Progress value={c.progress_to_target} />
                    </td>
                    <td className="pr-2 text-right font-mono text-ink-dim">
                      {c.hours_left == null ? ABSENT : c.hours_left <= 0 ? 'due' : `${formatNumber(c.hours_left, { decimals: 0 })}h`}
                    </td>
                    <td className="pr-2">
                      <span
                        title={c.trigger_families.join(', ') || undefined}
                        className={`px-1.5 py-0.5 rounded border text-micro font-bold ${
                          c.evidence_status === 'proven' ? 'border-emerald-500/40 text-emerald-300' : 'border-line text-ink-mute'
                        }`}
                      >
                        {c.entry_check === 'mismatch'
                          ? 'BAD ENTRY'
                          : c.evidence_status === 'proven'
                            ? 'PROVEN'
                            : c.evidence_status === 'unproven'
                              ? 'UNPROVEN'
                              : 'n/a'}
                      </span>
                    </td>
                    <td className="text-ink-mute">{c.agent}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <section className="bg-raised border border-line rounded-xl p-4 space-y-2">
          <h3 className="font-bold text-white">Calibration</h3>
          <p className="text-ink-dim">Realised hit rate by stated conviction. Well calibrated means the two columns match.</p>
          <table className="w-full text-left">
            <thead className="text-ink-mute">
              <tr className="border-b border-line">
                <th className="py-1.5">Conviction</th>
                <th className="text-right">Calls</th>
                <th className="text-right">Stated</th>
                <th className="text-right">Realised</th>
                <th className="pl-3">Gap</th>
              </tr>
            </thead>
            <tbody>
              {(s?.calibration ?? []).map((b) => {
                const gap = b.hit_rate != null && b.mean_conviction != null ? b.hit_rate - b.mean_conviction : null;
                return (
                  <tr key={b.band} className="border-b border-line/60">
                    <td className="py-1.5 font-mono">{b.band}</td>
                    <td className="text-right font-mono">{b.calls}</td>
                    <td className="text-right font-mono">{pct(b.mean_conviction)}</td>
                    <td className="text-right font-mono">{pct(b.hit_rate)}</td>
                    <td className={`pl-3 font-mono ${gap == null ? 'text-ink-mute' : gap < -0.1 ? 'text-rose-300' : gap > 0.1 ? 'text-emerald-300' : 'text-ink-dim'}`}>
                      {gap == null ? ABSENT : `${gap >= 0 ? '+' : ''}${formatNumber(gap * 100, { decimals: 0 })} pts`}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <h3 className="font-bold text-white pt-2">By agent</h3>
          <ul className="space-y-1">
            {Object.entries(data?.by_agent ?? {}).map(([agent, a]) => (
              <li key={agent} className="flex justify-between gap-2 border-b border-line/60 pb-1">
                <span className="text-ink">{agent}</span>
                <span className="font-mono text-ink-dim">
                  {a.hits}/{a.resolved} · shrunk {pct(a.shrunk_hit_rate)} · Brier {a.brier != null ? formatNumber(a.brier, { decimals: 3 }) : ABSENT}
                </span>
              </li>
            ))}
          </ul>
        </section>

        <section className="bg-raised border border-line rounded-xl p-4 space-y-2">
          <h3 className="font-bold text-white">Recently resolved</h3>
          {(data?.recent.length ?? 0) === 0 ? (
            <p className="text-ink-mute">Nothing resolved in this window.</p>
          ) : (
            <ul className="space-y-1 max-h-[420px] overflow-y-auto">
              {data?.recent.map((r) => (
                <li
                  key={r.prediction_id}
                  className={`flex items-center justify-between gap-2 border-b border-line/60 pb-1 ${
                    r.entry_check === 'mismatch' || r.entry_check === 'no_entry' ? 'opacity-40 line-through' : ''
                  }`}
                  title={r.entry_check === 'mismatch' || r.entry_check === 'no_entry' ? 'Excluded: the entry was not this stock’s price' : undefined}
                >
                  <span className="flex items-center gap-2">
                    <span className={r.outcome_correct ? 'text-emerald-300' : 'text-rose-300'} aria-label={r.outcome_correct ? 'hit' : 'miss'}>
                      {r.outcome_correct ? <IconCheck /> : <IconClose />}
                    </span>
                    <TickerLink ticker={r.ticker} />
                    <span className="text-ink-dim">{r.direction === 'up' ? 'long' : r.direction === 'down' ? 'short' : r.direction}</span>
                    <span className="text-ink-mute">@ {r.entry_price != null ? formatNumber(r.entry_price, { decimals: 2 }) : ABSENT}</span>
                  </span>
                  <span className="font-mono text-micro text-ink-mute">
                    {pct(r.confidence)} · {r.agent_name || 'unattributed'} · {formatAge(r.resolved_at)} ago
                  </span>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>

      <ConvergingEvidence />
    </div>
  );
}
