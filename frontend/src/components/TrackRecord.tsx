'use client';

/**
 * What the platform said, graded against what prices did, net of SPY
 * (/predictions/track-record; services/agents/grader.py). Trade ideas by
 * horizon; the signal board by its net vote and by the tier that carried it.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { LoadingLines } from './ui/Skeleton';

interface Summary {
  n: number;
  hit_rate: number | null;
  mean_bps: number | null;
  t: number | null;
  /** The board's t with one observation per day (and overlapping windows allowed for). */
  t_days?: number | null;
  days?: number;
}

interface BoardHorizon {
  by_net_bucket: Record<string, Summary>;
  signed_by_tier: Record<string, Summary>;
}

interface Record_ {
  ideas: Record<string, Summary>;
  board: Record<string, BoardHorizon>;
  pending: { ideas?: number; snapshots?: number };
}

interface Freshness {
  in_session: boolean;
  sources: { source: string; newest: string | null; age_sec: number | null; stale: boolean }[];
  stale: string[];
}

const BUCKETS = ['strong bear (net <= -3)', 'bear (-2..-1)', 'none (0)', 'bull (1..2)', 'strong bull (net >= 3)'];

function Row({ label, s }: { label: string; s?: Summary }) {
  return (
    <tr className="border-b border-line/60">
      <td className="py-0.5 text-ink-dim">{label}</td>
      <td className="text-right font-mono">{s?.n ?? 0}</td>
      <td className="text-right font-mono">{s?.hit_rate != null ? `${formatNumber(s.hit_rate * 100, { decimals: 0 })}%` : ABSENT}</td>
      <td
        className={`text-right font-mono ${
          s?.mean_bps == null ? '' : s.mean_bps > 0 ? 'text-positive' : s.mean_bps < 0 ? 'text-negative' : ''
        }`}
      >
        {s?.mean_bps != null ? `${s.mean_bps > 0 ? '+' : ''}${formatNumber(s.mean_bps / 100, { decimals: 2 })}%` : ABSENT}
      </td>
      <td
        className="text-right font-mono"
        title={s?.t_days !== undefined ? `Counted per day over ${s.days ?? 0} days; per stock-day it would read ${s.t ?? ABSENT}` : undefined}
      >
        {(s?.t_days !== undefined ? s.t_days : s?.t) != null
          ? formatNumber((s?.t_days !== undefined ? s.t_days : s?.t) as number, { decimals: 2 })
          : ABSENT}
      </td>
    </tr>
  );
}

const Head = ({ first, mean }: { first: string; mean: string }) => (
  <thead className="text-ink-mute">
    <tr className="border-b border-line">
      <th className="py-1 text-left">{first}</th>
      <th className="text-right">Calls</th>
      <th className="text-right">Right</th>
      <th className="text-right">{mean}</th>
      <th className="text-right" title="t-statistic: above 2 means unlikely to be luck">Not luck?</th>
    </tr>
  </thead>
);

export function FreshnessStrip() {
  const { data } = useSWR<Freshness>('/signals/freshness', fetcher, { refreshInterval: POLL.slow });
  if (!data) return null;
  return (
    <div className="flex flex-wrap gap-1 text-micro">
      {data.sources.map((s) => (
        <span
          key={s.source}
          title={s.newest ?? 'no data'}
          className={`px-1.5 py-0.5 rounded border ${s.stale ? 'border-negative/30 text-negative' : 'border-line text-ink-dim'}`}
        >
          {s.source} {s.newest ? `${formatAge(s.newest)} ago` : 'none'}
        </span>
      ))}
    </div>
  );
}

export function TrackRecord() {
  const { data, error } = useSWR<Record_>('/predictions/track-record', fetcher, { refreshInterval: POLL.slow });
  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-3 text-xs">
      <h2 className="font-bold text-ink text-sm">Track record</h2>
      <p className="text-ink-dim">
        Every trade idea graded over its horizon from the price it was given at, and every signal-board row graded one
        and five sessions on, all net of SPY. If the board&apos;s votes carry information, strong-bull rows beat strong-bear
        rows and the signed tiers sit above zero. Published as it accumulates; nothing here is asserted in advance.
      </p>
      {error ? (
        <p className="text-ink-mute">The track record could not be read.</p>
      ) : !data ? (
        <LoadingLines />
      ) : (
        <>
          <p className="text-micro text-ink-mute">
            Awaiting their horizon: {data.pending.ideas ?? 0} ideas, {data.pending.snapshots ?? 0} board snapshots.
          </p>
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <table className="w-full">
              <Head first="Stock calls, by holding period" mean="Average gain vs S&P 500" />
              <tbody>
                {['daily', 'weekly', 'monthly', 'all'].map((h) => (
                  <Row key={h} label={h} s={data.ideas[h]} />
                ))}
              </tbody>
            </table>
            {(['excess_1d', 'excess_5d'] as const).map((k) => (
              <div key={k} className="space-y-2">
                <table className="w-full">
                  <Head first={`Board, ${k === 'excess_1d' ? '1 session' : '5 sessions'}, by which way the evidence leaned`} mean="Average gain vs S&P 500" />
                  <tbody>
                    {BUCKETS.map((b) => (
                      <Row key={b} label={b} s={data.board[k]?.by_net_bucket[b]} />
                    ))}
                  </tbody>
                </table>
                <table className="w-full">
                  <Head first="By how well-tested the evidence was" mean="Average gain" />
                  <tbody>
                    {['proven', 'supported', 'published only', 'any vote'].map((t) => (
                      <Row key={t} label={t} s={data.board[k]?.signed_by_tier[t]} />
                    ))}
                  </tbody>
                </table>
              </div>
            ))}
          </div>
        </>
      )}
    </section>
  );
}

interface LedgerHorizon extends Summary {
  gross: Summary;
  pnl_usd: number;
  pnl_scaled_usd: number;
  mean_cost_bps: number | null;
  stopped: number;
  /** Trades still open, marked at the latest close with both ways of costs. */
  open_marked?: Summary;
  /** Closed and open together: the fair read while a horizon is young. */
  with_open?: Summary;
}

/** Sessions each horizon holds for, as the ledger trades it. */
const HOLD_SESSIONS: Record<string, number> = { daily: 1, weekly: 5, monthly: 21, events: 5 };

/** A horizon whose closed trades are all stop-outs has not had time to end any other way. */
export function onlyStopsSoFar(r: LedgerHorizon | undefined): boolean {
  return !!r && r.n > 0 && r.stopped === r.n && (r.open_marked?.n ?? 0) > 0;
}

const pct = (bps: number | null | undefined): string =>
  bps != null ? `${bps > 0 ? '+' : ''}${formatNumber(bps / 100, { decimals: 2 })}%` : ABSENT;

interface Ledger {
  by_horizon: Record<string, LedgerHorizon>;
  open: number;
  pending: number;
}

/**
 * Every idea traded on paper (/predictions/paper-ledger): filled at the next
 * session's open, costed both ways, exited at its stop or horizon, net of SPY.
 */
export function PaperLedger() {
  const { data, error } = useSWR<Ledger>('/predictions/paper-ledger', fetcher, { refreshInterval: POLL.slow });
  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-3 text-xs">
      <h2 className="font-bold text-ink text-sm">Paper ledger</h2>
      <p className="text-ink-dim">
        What the trade ideas would actually have made: each one bought or sold at the next morning&apos;s open, $250,000
        a trade, paying the spread and market impact both ways, closed at its stop or at the end of its horizon, and
        measured against simply holding the S&amp;P 500.
      </p>
      {error ? (
        <p className="text-ink-mute">The ledger could not be read.</p>
      ) : !data ? (
        <LoadingLines />
      ) : (
        <>
          <p className="text-micro text-ink-mute">
            Open now: {data.open}; waiting for their first session: {data.pending}.
          </p>
          <table className="w-full">
            <thead className="text-ink-mute">
              <tr className="border-b border-line">
                <th className="py-1 text-left">Closed trades</th>
                <th className="text-right">Trades</th>
                <th className="text-right">Won</th>
                <th className="text-right">Average, after costs</th>
                <th className="text-right">Before costs</th>
                <th className="text-right">Cost each way</th>
                <th className="text-right">Stopped</th>
                <th className="text-right" title="Trades still open, valued at the latest close as if sold there">
                  Still open
                </th>
                <th className="text-right" title="Closed and open trades together: the fair read while a horizon is young">
                  All, incl. open
                </th>
                <th className="text-right">P&amp;L</th>
                <th className="text-right" title="Each trade sized up when markets are calm and down when they are volatile">
                  P&amp;L, sized by risk
                </th>
              </tr>
            </thead>
            <tbody>
              {['daily', 'weekly', 'monthly', 'events', 'all'].map((h) => {
                const r = data.by_horizon[h];
                return (
                  <tr key={h} className="border-b border-line/60">
                    <td className="py-0.5 text-ink-dim">{h}</td>
                    <td className="text-right font-mono">{r?.n ?? 0}</td>
                    <td className="text-right font-mono">
                      {r?.hit_rate != null ? `${formatNumber(r.hit_rate * 100, { decimals: 0 })}%` : ABSENT}
                    </td>
                    <td
                      className={`text-right font-mono ${
                        r?.mean_bps == null ? '' : r.mean_bps > 0 ? 'text-positive' : 'text-negative'
                      }`}
                    >
                      {r?.mean_bps != null ? `${formatNumber(r.mean_bps / 100, { decimals: 2 })}%` : ABSENT}
                    </td>
                    <td className="text-right font-mono">
                      {r?.gross?.mean_bps != null ? `${formatNumber(r.gross.mean_bps / 100, { decimals: 2 })}%` : ABSENT}
                    </td>
                    <td className="text-right font-mono">
                      {r?.mean_cost_bps != null ? `${formatNumber(r.mean_cost_bps / 100, { decimals: 2 })}%` : ABSENT}
                    </td>
                    <td className="text-right font-mono">{r?.stopped ?? 0}</td>
                    <td className="text-right font-mono" title={`Average ${pct(r?.open_marked?.mean_bps)}`}>
                      {r?.open_marked?.n ?? 0}
                    </td>
                    <td
                      className={`text-right font-mono ${
                        r?.with_open?.mean_bps == null ? '' : r.with_open.mean_bps > 0 ? 'text-positive' : 'text-negative'
                      }`}
                    >
                      {pct(r?.with_open?.mean_bps)}
                    </td>
                    <td className={`text-right font-mono ${!r ? '' : r.pnl_usd >= 0 ? 'text-positive' : 'text-negative'}`}>
                      {r ? `$${formatNumber(r.pnl_usd, { decimals: 0 })}` : ABSENT}
                    </td>
                    <td className={`text-right font-mono ${!r ? '' : r.pnl_scaled_usd >= 0 ? 'text-positive' : 'text-negative'}`}>
                      {r ? `$${formatNumber(r.pnl_scaled_usd, { decimals: 0 })}` : ABSENT}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {['weekly', 'monthly', 'events']
            .filter((h) => onlyStopsSoFar(data.by_horizon[h]))
            .map((h) => (
              <p key={h} className="text-micro text-caution">
                No {h} trade has reached the end of its {HOLD_SESSIONS[h]}-session hold yet, so every closed one so far
                was cut by its stop. Read the &ldquo;All, incl. open&rdquo; column for {h} until they have.
              </p>
            ))}
        </>
      )}
    </section>
  );
}

interface GradeRow {
  group: string;
  n: number;
  mean_ratio: number;
  t: number;
  q: number | null;
  verdict: string;
}

interface Placebo {
  n: number;
  hit_rate: number | null;
  placebo_rate: number | null;
  skill: number | null;
}

interface ProducerGradesData {
  generated_at?: string;
  correlations_1d?: GradeRow[];
  correlations_5d?: GradeRow[];
  scenarios_5d?: GradeRow[];
  agent_calls?: Record<string, Summary>;
  entity_claims?: Record<string, Placebo>;
  note?: string;
}

const VERDICT_STYLE: Record<string, string> = {
  'moves more': 'text-positive',
  'moves less': 'text-caution',
  'no difference': 'text-ink-dim',
  'too few': 'text-ink-mute',
};

function RatioTable({ title, rows }: { title: string; rows?: GradeRow[] }) {
  const shown = (rows ?? []).filter((r) => r.verdict !== 'too few').slice(0, 10);
  return (
    <table className="w-full">
      <thead className="text-ink-mute">
        <tr className="border-b border-line">
          <th className="py-1 text-left">{title}</th>
          <th className="text-right">n</th>
          <th className="text-right">Move vs usual</th>
          <th className="text-right">Verdict</th>
        </tr>
      </thead>
      <tbody>
        {shown.length === 0 ? (
          <tr>
            <td className="py-1 text-ink-mute" colSpan={4}>
              Not enough graded yet.
            </td>
          </tr>
        ) : (
          shown.map((r) => (
            <tr key={r.group} className="border-b border-line/60">
              <td className="py-0.5 text-ink-dim">{r.group}</td>
              <td className="text-right font-mono">{r.n}</td>
              <td className="text-right font-mono">{formatNumber(r.mean_ratio, { decimals: 2 })}x</td>
              <td className={`text-right ${VERDICT_STYLE[r.verdict] ?? ''}`}>{r.verdict}</td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

/**
 * The platform's other voices graded against prices (/predictions/producer-grades):
 * correlation rules and scenarios by whether their stocks then moved more than
 * usual; agent calls net of SPY; entity claims against a placebo.
 */
export function ProducerGrades() {
  const { data, error } = useSWR<ProducerGradesData>('/predictions/producer-grades', fetcher, {
    refreshInterval: POLL.slow,
  });
  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-3 text-xs">
      <h2 className="font-bold text-ink text-sm">Graded against prices</h2>
      <p className="text-ink-dim">
        Correlation alerts, scenarios and agent calls, checked against what prices did. &ldquo;Move vs usual&rdquo; compares
        how much a stock moved over the five sessions after an alert with how much it normally moves: about 1.0x means the
        alert told you nothing about what came next. Agent calls are scored after the market&apos;s own move; claims about
        ships and aircraft are compared with simply assuming yesterday&apos;s activity continues.
      </p>
      {error ? (
        <p className="text-ink-mute">The grades could not be read.</p>
      ) : !data ? (
        <LoadingLines />
      ) : data.note ? (
        <p className="text-ink-mute">{data.note}</p>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <RatioTable title="Correlation rules, 5 sessions" rows={data.correlations_5d} />
          <div className="space-y-3">
            <RatioTable title="Scenarios by status, 5 sessions" rows={data.scenarios_5d} />
            <table className="w-full">
              <Head first="AI analyst calls, vs the S&P 500" mean="Average gain" />
              <tbody>
                {Object.entries(data.agent_calls ?? {}).map(([a, s]) => (
                  <Row key={a} label={a.replace(/_/g, ' ')} s={s} />
                ))}
              </tbody>
            </table>
            <table className="w-full">
              <thead className="text-ink-mute">
                <tr className="border-b border-line">
                  <th className="py-1 text-left">Entity claims</th>
                  <th className="text-right">n</th>
                  <th className="text-right">Came true</th>
                  <th className="text-right">Already active</th>
                  <th className="text-right">Skill</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(data.entity_claims ?? {}).map(([a, p]) => (
                  <tr key={a} className="border-b border-line/60">
                    <td className="py-0.5 text-ink-dim">{a.replace(/_/g, ' ')}</td>
                    <td className="text-right font-mono">{p.n}</td>
                    <td className="text-right font-mono">
                      {p.hit_rate != null ? `${formatNumber(p.hit_rate * 100, { decimals: 0 })}%` : ABSENT}
                    </td>
                    <td className="text-right font-mono">
                      {p.placebo_rate != null ? `${formatNumber(p.placebo_rate * 100, { decimals: 0 })}%` : ABSENT}
                    </td>
                    <td
                      className={`text-right font-mono ${
                        p.skill == null ? '' : p.skill > 0 ? 'text-positive' : 'text-negative'
                      }`}
                    >
                      {p.skill != null ? `${p.skill > 0 ? '+' : ''}${formatNumber(p.skill * 100, { decimals: 0 })} pts` : ABSENT}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  );
}

export default TrackRecord;
