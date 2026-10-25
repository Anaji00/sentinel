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
import Link from 'next/link';
import useSWR from 'swr';
import { familyName, horizonWords } from '../lib/signalNames';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { ConvergingEvidence } from './ConvergingEvidence';
import { ConvictionMeter } from './ui/ConvictionMeter';

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

export interface ActiveSignal {
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
  proven: 'text-positive border-positive/30 bg-positive/10',
  unconfirmed: 'text-caution border-caution/30',
  below_costs: 'text-caution border-caution/30',
  low_hit_rate: 'text-caution border-caution/30',
  contradicts_prior: 'text-negative border-negative/30',
  no_edge: 'text-ink-mute border-line',
  insufficient_data: 'text-ink-mute border-line',
  unmeasurable: 'text-ink-mute border-line',
};

/** The study's verdict codes in words a reader can act on. */
export const VERDICT_WORD: Record<string, string> = {
  proven: 'Works',
  unconfirmed: 'Not confirmed yet',
  below_costs: 'Too small to trade',
  low_hit_rate: 'Too unreliable',
  contradicts_prior: 'Goes the wrong way',
  no_edge: "Doesn't work",
  insufficient_data: 'Not enough data',
  unmeasurable: "Can't be measured",
};

function Verdict({ v }: { v: string }) {
  return (
    <span className={`px-1.5 py-0.5 rounded border text-micro font-semibold whitespace-nowrap ${VERDICT_STYLE[v] ?? 'border-line'}`}>
      {VERDICT_WORD[v] ?? v.replace(/_/g, ' ')}
    </span>
  );
}

/** The study's q-value as the chance the pattern is a fluke: "under 1%". */
export function flukeChance(q: number | null): string {
  if (q == null) return ABSENT;
  if (q < 0.01) return 'under 1%';
  return `${formatNumber(q * 100, { decimals: 0 })}%`;
}

function pct(v: number | null | undefined): string {
  return v == null ? ABSENT : `${formatNumber(v * 100, { decimals: 0 })}%`;
}

/** Live signals that share a reason, horizon and direction: one rule firing on
 * many stocks reads as one card listing them, not forty identical rows. */
export interface SignalGroup {
  key: string;
  families: string[];
  horizon: string;
  direction: 'long' | 'short';
  signals: ActiveSignal[];
  probability: number;
  expected_bps: number;
  cost_bps: number;
}

export function groupSignals(active: ActiveSignal[]): SignalGroup[] {
  const groups = new Map<string, SignalGroup>();
  for (const s of active) {
    const key = `${[...s.families].sort().join('+')}|${s.horizon}|${s.direction}`;
    const g = groups.get(key);
    if (g) g.signals.push(s);
    else
      groups.set(key, {
        key,
        families: s.families,
        horizon: s.horizon,
        direction: s.direction,
        signals: [s],
        probability: 0,
        expected_bps: 0,
        cost_bps: 0,
      });
  }
  const out = [...groups.values()];
  for (const g of out) {
    g.signals.sort((a, b) => b.probability - a.probability);
    const n = g.signals.length;
    g.probability = g.signals.reduce((t, s) => t + s.probability, 0) / n;
    g.expected_bps = g.signals.reduce((t, s) => t + s.expected_bps, 0) / n;
    g.cost_bps = g.signals.reduce((t, s) => t + s.cost_bps, 0) / n;
  }
  // The strongest reasons first: likelihood, then how far the move clears its cost.
  return out.sort((a, b) => b.probability - a.probability || b.expected_bps - b.cost_bps - (a.expected_bps - a.cost_bps));
}

/** Expected move against its cost on one scale: the part past the cost is the edge. */
function EdgeBar({ expected, cost }: { expected: number; cost: number }) {
  const scale = Math.max(expected, cost, 1);
  return (
    <div className="space-y-1">
      <div className="flex items-baseline justify-between text-micro">
        <span className="text-ink-mute">Expected move vs cost</span>
        <span className="font-mono text-ink">
          {formatNumber(expected / 100, { decimals: 2 })}%{' '}
          <span className="text-ink-mute">vs {formatNumber(cost / 100, { decimals: 2 })}%</span>
        </span>
      </div>
      <div className="relative h-1.5 rounded-full bg-inset overflow-hidden">
        <div className="absolute inset-y-0 left-0 bg-accent/70" style={{ width: `${(expected / scale) * 100}%` }} />
        <div className="absolute inset-y-0 left-0 bg-caution" style={{ width: `${(Math.min(cost, expected) / scale) * 100}%` }} />
      </div>
    </div>
  );
}

function GroupCard({ g }: { g: SignalGroup }) {
  const [open, setOpen] = useState(false);
  const up = g.direction === 'long';
  const lead = g.signals[0];
  const SHOWN = 18;
  return (
    <article
      className={`rounded-2xl border border-line bg-raised p-4 space-y-3 border-t-2 ${up ? 'border-t-positive/70' : 'border-t-negative/70'}`}
    >
      <header className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span
              className={`rounded-full px-2 py-0.5 text-micro font-semibold ${up ? 'bg-positive/15 text-positive' : 'bg-negative/15 text-negative'}`}
            >
              {up ? 'Leans up' : 'Leans down'}
            </span>
            <span className="rounded-md border border-line px-1.5 py-0.5 text-micro text-ink-dim">{horizonWords(g.horizon)}</span>
          </div>
          <h3 className="mt-1.5 text-sm font-semibold text-ink">{g.families.map(familyName).join(' + ')}</h3>
        </div>
        <div className="text-right shrink-0">
          <div className={`font-mono text-lead leading-none ${up ? 'text-positive' : 'text-negative'}`}>
            {formatNumber(g.signals.length, { decimals: 0 })}
          </div>
          <div className="text-micro text-ink-mute">{g.signals.length === 1 ? 'stock' : 'stocks'}</div>
        </div>
      </header>

      <ConvictionMeter p={g.probability} label={g.signals.length > 1 ? 'Likely to go this way (average)' : 'Likely to go this way'} />
      <EdgeBar expected={g.expected_bps} cost={g.cost_bps} />

      <div className="flex flex-wrap gap-1.5">
        {(open ? g.signals : g.signals.slice(0, SHOWN)).map((s) => (
          <Link
            key={s.id}
            href={`/ticker?symbol=${encodeURIComponent(s.instrument)}`}
            title={`${formatNumber(s.probability * 100, { decimals: 0 })}% likely · raised ${formatAge(s.created_at)} ago`}
            className="rounded-md border border-line bg-inset px-2 py-0.5 font-mono text-micro text-ink hover:border-line-accent hover:text-accent"
          >
            {s.instrument}
          </Link>
        ))}
        {g.signals.length > SHOWN && !open && (
          <span className="px-1 py-0.5 text-micro text-ink-mute">+{g.signals.length - SHOWN}</span>
        )}
      </div>

      <div className="flex items-center justify-between gap-2 text-micro text-ink-mute">
        <span>
          Latest raised {formatAge(g.signals.reduce((m, s) => (s.created_at > m ? s.created_at : m), lead.created_at))} ago
        </span>
        <button type="button" onClick={() => setOpen(!open)} className="text-accent hover:underline">
          {open ? 'Hide the evidence' : 'Show the evidence'}
        </button>
      </div>

      {open && (
        <ul className="space-y-1.5 border-t border-line pt-2">
          {g.signals.map((s) => (
            <li key={s.id} className="text-micro">
              <span className="font-mono text-ink">{s.instrument}</span>
              <span className="text-ink-mute">
                {' '}
                · {formatNumber(s.probability * 100, { decimals: 0 })}% · expires {formatAge(s.expires_at)}
              </span>
              {s.evidence.map((e) => (
                <div key={`${e.family}-${e.event_id}`} className="pl-3 text-ink-dim">
                  {`${familyName(e.family)}: right ${pct(e.hit)} of the time over ${e.units ?? ABSENT} past events; chance it's a fluke ${flukeChance(e.q)}`}
                  {e.headline && <div className="text-ink-mute">{e.headline}</div>}
                </div>
              ))}
            </li>
          ))}
        </ul>
      )}
    </article>
  );
}

function Stat({ label, value, sub, tone = 'text-ink' }: { label: string; value: string; sub?: string; tone?: string }) {
  return (
    <div className="rounded-xl border border-line bg-inset px-4 py-3">
      <div className="text-micro text-ink-mute">{label}</div>
      <div className={`mt-1 font-mono text-lead leading-none ${tone}`}>{value}</div>
      {sub && <div className="mt-1 text-micro text-ink-mute">{sub}</div>}
    </div>
  );
}

const STUDY_FILTERS: { key: string; label: string; match: (v: string) => boolean }[] = [
  { key: 'proven', label: 'Proven', match: (v) => v === 'proven' },
  { key: 'close', label: 'Close but not yet', match: (v) => ['unconfirmed', 'below_costs', 'low_hit_rate'].includes(v) },
  { key: 'failed', label: 'No edge', match: (v) => ['no_edge', 'contradicts_prior'].includes(v) },
  { key: 'all', label: 'Everything measured', match: (v) => v !== 'insufficient_data' && v !== 'unmeasurable' },
];

export default function TradingSignals() {
  const { data } = useSWR<SignalsResponse>('/signals', fetcher, { refreshInterval: POLL.standard });
  const { data: study } = useSWR<StudyResponse>('/signals/study', fetcher, { refreshInterval: POLL.slow });
  const [allCells, setAllCells] = useState(false);
  const [studyFilter, setStudyFilter] = useState('proven');

  const groups = useMemo(() => groupSignals(data?.active ?? []), [data]);
  const cells = useMemo(() => {
    const list = study?.cells ?? [];
    if (allCells) return list;
    const f = STUDY_FILTERS.find((x) => x.key === studyFilter) ?? STUDY_FILTERS[0];
    return list.filter((c) => f.match(c.verdict));
  }, [study, allCells, studyFilter]);

  const tr = data?.track_record;
  const proven = data?.study?.verdicts?.proven ?? 0;
  const active = data?.active.length ?? 0;
  const longs = (data?.active ?? []).filter((s) => s.direction === 'long').length;

  return (
    <div className="space-y-4 text-xs">
      <section className="bg-raised border border-line rounded-2xl p-4 sm:p-5 space-y-4">
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div className="max-w-3xl space-y-1">
            <div className="flex items-center gap-2">
              <h2 className="text-head font-semibold text-ink">Proven signals, live now</h2>
              <span
                className={`flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-micro font-semibold ${
                  data?.enabled === false ? 'text-negative border-negative/50' : 'text-positive border-positive/50'
                }`}
              >
                <span className={`h-1.5 w-1.5 rounded-full ${data?.enabled === false ? 'bg-negative' : 'bg-positive animate-pulse'}`} />
                {data?.enabled === false ? 'Halted' : 'Live'}
              </span>
            </div>
            <p className="text-ink-dim">
              A signal is raised only from a pattern the reaction study has proven: enough independent episodes, significant
              after correcting for every pattern tested, confirmed on later data held back, larger than trading costs, and in
              the direction its reason predicts.
            </p>
          </div>
        </div>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
          <Stat
            label="Live signals"
            value={formatNumber(active, { decimals: 0 })}
            sub={`${formatNumber(longs, { decimals: 0 })} up · ${formatNumber(active - longs, { decimals: 0 })} down · ${formatNumber(groups.length, { decimals: 0 })} distinct reasons`}
          />
          <Stat
            label="Patterns proven"
            value={formatNumber(proven, { decimals: 0 })}
            sub={`of ${formatNumber(data?.study?.episodes ?? null, { decimals: 0 })} episodes over ${data?.study?.lookback_days ?? ABSENT} days`}
            tone="text-accent"
          />
          <Stat
            label="Hit rate, closed signals"
            value={pct(tr?.hit_rate)}
            sub={`${formatNumber(tr?.scored ?? null, { decimals: 0 })} scored of ${formatNumber(tr?.closed ?? null, { decimals: 0 })} closed`}
            tone={tr?.hit_rate == null ? 'text-ink' : tr.hit_rate >= 0.5 ? 'text-positive' : 'text-caution'}
          />
          <Stat
            label="Average return per signal"
            value={tr?.mean_return_bps == null ? ABSENT : `${tr.mean_return_bps >= 0 ? '+' : ''}${formatNumber(tr.mean_return_bps / 100, { decimals: 2 })}%`}
            sub="after costs, misses included"
            tone={tr?.mean_return_bps == null ? 'text-ink' : tr.mean_return_bps >= 0 ? 'text-positive' : 'text-negative'}
          />
        </div>
        <p className="text-micro text-ink-mute">
          Study {data?.study?.generated_at ? `${formatAge(data.study.generated_at)} ago` : 'pending'}. A hit rate under half
          with a positive average means the wins are larger than the misses.
        </p>
      </section>

      {active === 0 ? (
        <section className="rounded-2xl border border-dashed border-line p-5 text-ink-mute">
          No live signals.{' '}
          {proven === 0
            ? 'No pattern has yet proven an edge, so nothing is allowed to raise a signal. That is the gate working, not a fault.'
            : 'No proven pattern has fired recently.'}
        </section>
      ) : (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {groups.map((g) => (
            <GroupCard key={g.key} g={g} />
          ))}
        </div>
      )}

      <ConvergingEvidence />

      <section className="bg-raised border border-line rounded-2xl p-4 sm:p-5 space-y-3">
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <div>
            <h3 className="text-sm font-semibold text-ink">The study behind it</h3>
            <p className="text-ink-mute">Every pattern tested, and why each does or does not trade.</p>
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <div className="flex rounded-lg border border-line bg-inset p-0.5" role="group" aria-label="Filter the study">
              {STUDY_FILTERS.map((f) => (
                <button
                  key={f.key}
                  type="button"
                  aria-pressed={!allCells && studyFilter === f.key}
                  onClick={() => {
                    setStudyFilter(f.key);
                    setAllCells(false);
                  }}
                  className={`rounded-md px-2.5 py-1 ${!allCells && studyFilter === f.key ? 'bg-overlay text-ink font-semibold' : 'text-ink-dim hover:text-ink'}`}
                >
                  {f.label}
                </button>
              ))}
            </div>
            <button
              onClick={() => setAllCells(!allCells)}
              className={`px-2.5 py-1 rounded-md border border-line cursor-pointer ${allCells ? 'text-accent' : 'text-ink-dim'}`}
            >
              {allCells ? 'Hide patterns without enough data' : 'Include patterns without enough data'}
            </button>
          </div>
        </div>
        {study?.note && <p className="text-ink-mute">{study.note}</p>}
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-micro text-ink-mute text-left">
                <th className="py-1.5 pr-2 font-normal">Pattern</th>
                <th className="py-1.5 pr-2 font-normal">Over</th>
                <th className="py-1.5 pr-2 font-normal">Verdict</th>
                <th className="py-1.5 pr-2 text-right font-normal" title="Events far enough apart to count separately, of all events seen">
                  Separate events
                </th>
                <th className="py-1.5 pr-2 text-right font-normal">Average move</th>
                <th className="py-1.5 pr-2 text-right font-normal">How often right</th>
                <th className="py-1.5 pr-2 text-right font-normal" title="False-discovery-adjusted q-value: the chance this pattern is a fluke after testing many">
                  Chance it&apos;s a fluke
                </th>
                <th className="py-1.5 font-normal">Why</th>
              </tr>
            </thead>
            <tbody>
              {cells.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-3 text-ink-mute">
                    Nothing in this group.
                  </td>
                </tr>
              ) : (
                cells.map((c) => (
                  <tr key={`${c.family}-${c.horizon}`} className={`border-t border-line align-top ${c.verdict === 'proven' ? 'bg-positive/[0.04]' : ''}`}>
                    <td className="py-1.5 pr-2 text-ink" title={`${c.family}: ${c.description ?? ''}`}>
                      {familyName(c.family)}
                    </td>
                    <td className="py-1.5 pr-2 text-ink-dim">{c.horizon}</td>
                    <td className="py-1.5 pr-2">
                      <Verdict v={c.verdict} />
                    </td>
                    <td className="py-1.5 pr-2 text-right font-mono">
                      {formatNumber(c.units, { decimals: 0 })}/{formatNumber(c.episodes, { decimals: 0 })}
                    </td>
                    <td className={`py-1.5 pr-2 text-right font-mono ${c.mean_bps == null ? '' : c.mean_bps >= 0 ? 'text-positive' : 'text-negative'}`}>
                      {c.mean_bps == null ? ABSENT : `${c.mean_bps >= 0 ? '+' : ''}${formatNumber(c.mean_bps / 100, { decimals: 2 })}%`}
                    </td>
                    <td className="py-1.5 pr-2 text-right font-mono">{pct(c.hit_rate)}</td>
                    <td className="py-1.5 pr-2 text-right font-mono">{flukeChance(c.q)}</td>
                    <td className="py-1.5 text-ink-dim">{c.reason}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
