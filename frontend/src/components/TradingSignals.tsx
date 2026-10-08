'use client';

/**
 * Trading signals, and the evidence that earned or refused them.
 *
 * A signal appears here only when every signal family behind it has passed
 * the reaction study (services/agents/signal_engine.py). The study table below
 * shows every family -- including the ones that failed and why -- so nothing
 * is asserted without its working.
 */

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { ConvergingEvidence } from './ConvergingEvidence';

interface Evidence {
  family: string;
  direction: number;
  event_id: string;
  headline: string;
  hit: number;
  mean_bps: number | null;
  units: number | null;
  q: number | null;
  at: string;
}

interface ActiveSignal {
  id: string;
  instrument: string;
  horizon: string;
  direction: 'long' | 'short';
  probability: number;
  expected_bps: number;
  cost_bps: number;
  created_at: string;
  expires_at: string;
  entry_price: number | null;
  evidence: Evidence[];
  families: string[];
}

interface FamilyRecord {
  signals: number;
  hits: number;
  hit_rate: number;
  mean_return_bps: number;
}

interface TrackRecord {
  closed: number;
  scored: number;
  hits: number;
  hit_rate: number | null;
  mean_return_bps: number | null;
  by_family: Record<string, FamilyRecord>;
}

interface Outcome {
  id: string;
  instrument: string;
  horizon: string;
  direction: string;
  probability: number;
  families: string[];
  created_at: string;
  return_bps: number | null;
  hit: boolean | null;
}

interface SignalsResponse {
  enabled: boolean;
  active: ActiveSignal[];
  track_record: TrackRecord;
  recent_outcomes: Outcome[];
  study: {
    generated_at?: string | null;
    episodes?: number;
    instruments?: number;
    verdicts?: Record<string, number>;
    lookback_days?: number;
  };
}

interface StudyCell {
  family: string;
  description: string;
  horizon: string;
  episodes: number;
  units: number;
  instruments: string[];
  mean_bps: number | null;
  cost_bps: number | null;
  hit_rate: number | null;
  q: number | null;
  t: number | null;
  validation_t: number | null;
  sign: number;
  verdict: string;
  reason: string;
}

interface StudyResponse {
  generated_at: string | null;
  cells: StudyCell[];
  note?: string;
}

const VERDICT_STYLE: Record<string, string> = {
  proven: 'text-emerald-300 border-emerald-500/60 bg-emerald-500/10',
  unconfirmed: 'text-amber-300 border-amber-500/50',
  below_costs: 'text-amber-300 border-amber-500/50',
  low_hit_rate: 'text-amber-300 border-amber-500/50',
  contradicts_prior: 'text-rose-300 border-rose-500/50',
  no_edge: 'text-ink-mute border-line',
  insufficient_data: 'text-ink-mute border-line',
  unmeasurable: 'text-ink-mute border-line',
};

function Verdict({ v }: { v: string }) {
  return (
    <span className={`px-1.5 py-0.5 rounded border text-micro uppercase font-bold ${VERDICT_STYLE[v] ?? 'border-line'}`}>
      {v.replace(/_/g, ' ')}
    </span>
  );
}

function pct(v: number | null | undefined): string {
  return v == null ? ABSENT : `${formatNumber(v * 100, { decimals: 0 })}%`;
}

export default function TradingSignals() {
  const { data } = useSWR<SignalsResponse>('/signals', fetcher, { refreshInterval: POLL.standard });
  const { data: study } = useSWR<StudyResponse>('/signals/study', fetcher, { refreshInterval: POLL.slow });
  const [allCells, setAllCells] = useState(false);
  const [open, setOpen] = useState<string | null>(null);

  const cells = useMemo(() => {
    const list = study?.cells ?? [];
    return allCells ? list : list.filter((c) => c.verdict !== 'insufficient_data' && c.verdict !== 'unmeasurable');
  }, [study, allCells]);

  const tr = data?.track_record;
  const proven = data?.study?.verdicts?.proven ?? 0;

  return (
    <div className="h-full overflow-y-auto space-y-4 text-xs">
      <section className="bg-raised border border-line rounded-xl p-4 space-y-2">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <h2 className="text-sm font-bold text-white">Trading signals</h2>
          <span
            className={`px-2 py-0.5 rounded border font-bold uppercase text-micro ${
              data?.enabled === false ? 'text-rose-300 border-rose-500/60' : 'text-emerald-300 border-emerald-500/60'
            }`}
          >
            {data?.enabled === false ? 'Halted' : 'Live'}
          </span>
        </div>
        <p className="text-ink-dim">
          A signal is raised only from signal families the reaction study has proven: enough independent episodes,
          significant after correcting for every family tested, confirmed on held-out later data, larger than
          trading costs, and in the direction its reason predicts. Everything else is listed below with the reason
          it does not trade.
        </p>
        <div className="text-ink-mute">
          Study {data?.study?.generated_at ? `${formatAge(data.study.generated_at)} ago` : 'pending'} ·{' '}
          {formatNumber(data?.study?.episodes ?? null, { decimals: 0 })} episodes over{' '}
          {data?.study?.lookback_days ?? ABSENT} days · {proven} proven family-horizon
          {proven === 1 ? '' : 's'}
        </div>
      </section>

      <section className="bg-raised border border-line rounded-xl p-4">
        <h3 className="font-bold text-white pb-2">Active signals</h3>
        {(data?.active.length ?? 0) === 0 ? (
          <p className="text-ink-mute">
            None. {proven === 0
              ? 'No signal family has yet proven an edge, so nothing is allowed to raise a signal. That is the gate working, not a fault.'
              : 'No proven family has fired recently.'}
          </p>
        ) : (
          <ul className="space-y-2">
            {data?.active.map((s) => (
              <li key={s.id} className="border-b border-line pb-2">
                <button
                  onClick={() => setOpen(open === s.id ? null : s.id)}
                  className="w-full text-left flex items-center justify-between gap-2 cursor-pointer"
                >
                  <span>
                    <span className={s.direction === 'long' ? 'text-emerald-300 font-bold' : 'text-rose-300 font-bold'}>
                      {s.direction.toUpperCase()}
                    </span>{' '}
                    <span className="font-mono font-bold text-white">{s.instrument}</span>{' '}
                    <span className="text-ink-dim">over {s.horizon}</span>
                  </span>
                  <span className="font-mono text-ink-dim">
                    p={formatNumber(s.probability, { decimals: 2 })} · {formatNumber(s.expected_bps, { decimals: 0 })} bps
                    expected vs {formatNumber(s.cost_bps, { decimals: 0 })} cost
                  </span>
                </button>
                <div className="text-ink-mute">
                  {s.families.join(', ')} · raised {formatAge(s.created_at)} ago · expires {formatAge(s.expires_at)}
                </div>
                {open === s.id && (
                  <ul className="pt-1 pl-3 space-y-1">
                    {s.evidence.map((e) => (
                      <li key={`${e.family}-${e.event_id}`} className="text-ink">
                        <span className="font-bold">{e.family}</span> · hit {pct(e.hit)} over {e.units ?? ABSENT} units ·
                        q={e.q ?? ABSENT}
                        {e.headline && <div className="text-ink-dim">{e.headline}</div>}
                      </li>
                    ))}
                  </ul>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      <ConvergingEvidence />

      <section className="bg-raised border border-line rounded-xl p-4 space-y-2">
        <h3 className="font-bold text-white">Track record</h3>
        <p className="text-ink-dim">
          Every expired signal, scored against what the price did over its horizon, net of costs. Misses included.
        </p>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
          <div>
            <div className="text-ink-mute">Closed</div>
            <div className="font-mono text-white">{formatNumber(tr?.closed ?? null, { decimals: 0 })}</div>
          </div>
          <div>
            <div className="text-ink-mute">Scored</div>
            <div className="font-mono text-white">{formatNumber(tr?.scored ?? null, { decimals: 0 })}</div>
          </div>
          <div>
            <div className="text-ink-mute">Hit rate</div>
            <div className="font-mono text-white">{pct(tr?.hit_rate)}</div>
          </div>
          <div>
            <div className="text-ink-mute">Mean return</div>
            <div className="font-mono text-white">
              {tr?.mean_return_bps == null ? ABSENT : `${formatNumber(tr.mean_return_bps, { decimals: 0 })} bps`}
            </div>
          </div>
        </div>
      </section>

      <section className="bg-raised border border-line rounded-xl p-4 space-y-2">
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <h3 className="font-bold text-white">The study</h3>
          <button
            onClick={() => setAllCells(!allCells)}
            className="px-2 py-0.5 rounded border border-line text-ink-dim cursor-pointer"
          >
            {allCells ? 'Hide families without enough data' : 'Show every family'}
          </button>
        </div>
        {study?.note && <p className="text-ink-mute">{study.note}</p>}
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-ink-mute text-left">
                <th className="py-1 pr-2">Family</th>
                <th className="py-1 pr-2">Horizon</th>
                <th className="py-1 pr-2">Verdict</th>
                <th className="py-1 pr-2 text-right">Units</th>
                <th className="py-1 pr-2 text-right">Mean move</th>
                <th className="py-1 pr-2 text-right">Hit</th>
                <th className="py-1 pr-2 text-right">q</th>
                <th className="py-1">Why</th>
              </tr>
            </thead>
            <tbody>
              {cells.map((c) => (
                <tr key={`${c.family}-${c.horizon}`} className="border-t border-line align-top">
                  <td className="py-1 pr-2" title={c.description}>
                    {c.family}
                  </td>
                  <td className="py-1 pr-2">{c.horizon}</td>
                  <td className="py-1 pr-2">
                    <Verdict v={c.verdict} />
                  </td>
                  <td className="py-1 pr-2 text-right font-mono">
                    {formatNumber(c.units, { decimals: 0 })}/{formatNumber(c.episodes, { decimals: 0 })}
                  </td>
                  <td className="py-1 pr-2 text-right font-mono">
                    {c.mean_bps == null ? ABSENT : `${formatNumber(c.mean_bps, { decimals: 0 })} bps`}
                  </td>
                  <td className="py-1 pr-2 text-right font-mono">{pct(c.hit_rate)}</td>
                  <td className="py-1 pr-2 text-right font-mono">{c.q == null ? ABSENT : formatNumber(c.q, { decimals: 2 })}</td>
                  <td className="py-1 text-ink-dim">{c.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
