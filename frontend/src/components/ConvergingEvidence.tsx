'use client';

/**
 * Independent evidence per instrument, weighted by each family's own record.
 *
 * Read from /signals/convergence and /signals/dossier/{ticker}
 * (shared/utils/evidence_fusion.py). A family counts once however many events
 * it produced; its weight is its reaction-study record -- proven 1.0,
 * unstudied 0.25, measured-with-no-edge 0. This is a ranking of evidence, not a
 * trade: only the signal engine raises signals.
 */

import React, { useState } from 'react';
import Link from 'next/link';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';

export interface EvidenceFactor {
  family: string;
  /** The underlying source; families sharing one count as one vote. */
  source: string;
  direction: number;
  events: number;
  last_seen: string;
  status: 'proven' | 'unstudied' | 'no_edge';
  weight: number;
  study: {
    verdict?: string;
    horizon?: string;
    units?: number;
    hit_rate?: number;
    mean_bps?: number;
    reason?: string;
  };
}

export interface EvidenceDossier {
  instrument: string;
  score: number;
  stance: 'long' | 'short' | 'mixed' | 'attention' | 'none';
  agreeing_families: number;
  opposing_families: number;
  attention_families: number;
  proven_families: number;
  summary: string;
  factors: EvidenceFactor[];
}

interface ConvergenceResponse {
  generated_at: string;
  window_hours: number;
  events: number;
  instruments_seen: number;
  converging: EvidenceDossier[];
}

interface DossierResponse {
  generated_at: string;
  window_hours: number;
  instrument: string;
  dossier: EvidenceDossier | null;
}

const STANCE_STYLE: Record<string, string> = {
  long: 'text-emerald-300 border-emerald-500/50',
  short: 'text-rose-300 border-rose-500/50',
  mixed: 'text-amber-300 border-amber-500/50',
  attention: 'text-ink-dim border-line',
  none: 'text-ink-mute border-line',
};

const STATUS_STYLE: Record<string, string> = {
  proven: 'text-emerald-300',
  unstudied: 'text-ink-dim',
  no_edge: 'text-ink-mute line-through',
};

function arrow(d: number): string {
  return d > 0 ? '▲' : d < 0 ? '▼' : '•';
}

function FactorList({ factors }: { factors: EvidenceFactor[] }) {
  return (
    <ul className="space-y-1 pt-1">
      {factors.map((f) => (
        <li key={f.family} className="flex flex-wrap items-baseline gap-x-2 font-mono text-micro">
          <span className={f.direction > 0 ? 'text-emerald-300' : f.direction < 0 ? 'text-rose-300' : 'text-ink-mute'}>
            {arrow(f.direction)}
          </span>
          <span className={STATUS_STYLE[f.status] ?? ''}>{f.family.replace(/_/g, ' ')}</span>
          <span className="text-ink-mute">
            {f.source} · ×{f.events} · {f.status === 'no_edge' ? 'studied: no edge' : f.status}
            {f.study.units != null ? ` · ${f.study.units} units` : ''}
            {f.study.hit_rate != null ? ` · hit ${formatNumber(f.study.hit_rate * 100, { decimals: 0 })}%` : ''}
            {f.study.mean_bps != null ? ` · ${formatNumber(f.study.mean_bps, { decimals: 0 })} bps` : ''}
            {' · '}
            {formatAge(f.last_seen)} ago
          </span>
        </li>
      ))}
    </ul>
  );
}

export function ConvergingEvidence() {
  const [hours, setHours] = useState(24);
  const [open, setOpen] = useState<string | null>(null);
  const { data, error } = useSWR<ConvergenceResponse>(`/signals/convergence?hours=${hours}&limit=25`, fetcher, {
    refreshInterval: POLL.standard,
  });

  return (
    <section className="bg-raised border border-line rounded-xl p-4 space-y-2">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h3 className="font-bold text-white">Converging evidence</h3>
        <div className="flex gap-1">
          {[6, 24, 72].map((h) => (
            <button
              key={h}
              onClick={() => setHours(h)}
              aria-pressed={hours === h}
              className={`cursor-pointer rounded-md border px-2 py-0.5 text-micro font-medium ${
                hours === h ? 'border-line-accent text-accent' : 'border-line text-ink-mute hover:text-ink-dim'
              }`}
            >
              {h}h
            </button>
          ))}
        </div>
      </div>
      <p className="text-ink-dim">
        Instruments where two or more independent sources agree -- options, insiders, 13F, news, shipping, the tape.
        Families reading the same source count once, and each is weighted by its own reaction-study record: proven
        1.0, not yet measurable 0.25, measured with no edge 0. A ranking of evidence, not a trade.
      </p>
      {error ? (
        <p className="text-ink-mute">The evidence layer could not be read.</p>
      ) : !data ? (
        <p className="text-ink-mute">Reading the window…</p>
      ) : data.converging.length === 0 ? (
        <p className="text-ink-mute">
          No instrument has two independent sources agreeing in the last {data.window_hours}h (
          {formatNumber(data.instruments_seen, { decimals: 0 })} instruments, {formatNumber(data.events, { decimals: 0 })}{' '}
          events read).
        </p>
      ) : (
        <ul className="space-y-1.5">
          {data.converging.map((d) => (
            <li key={d.instrument} className="border-b border-line/60 pb-1.5">
              <div className="flex flex-wrap items-center gap-2">
                <span className={`px-1.5 py-0.5 rounded border text-micro uppercase font-bold ${STANCE_STYLE[d.stance]}`}>
                  {d.stance}
                </span>
                <Link
                  href={`/ticker?symbol=${encodeURIComponent(d.instrument)}`}
                  className="font-mono font-bold text-white hover:text-accent hover:underline"
                >
                  {d.instrument}
                </Link>
                <button
                  onClick={() => setOpen(open === d.instrument ? null : d.instrument)}
                  aria-expanded={open === d.instrument}
                  className="flex-1 text-left flex flex-wrap items-center justify-between gap-2 cursor-pointer"
                >
                  <span className="text-ink-dim">
                    {d.agreeing_families} source{d.agreeing_families === 1 ? '' : 's'} agreeing
                    {d.opposing_families ? `, ${d.opposing_families} opposing` : ''}
                    {d.attention_families ? `, ${d.attention_families} attention` : ''}
                  </span>
                  <span className="font-mono text-micro">
                    <span className={d.proven_families ? 'text-emerald-300 font-bold' : 'text-ink-mute'}>
                      {d.proven_families ? `${d.proven_families} proven` : 'unproven only'}
                    </span>
                    <span className="text-ink-mute"> · score {formatNumber(d.score, { decimals: 2 })}</span>
                  </span>
                </button>
              </div>
              {open === d.instrument && <FactorList factors={d.factors} />}
            </li>
          ))}
        </ul>
      )}
      {data && (
        <p className="text-micro text-ink-mute">
          {formatNumber(data.events, { decimals: 0 })} events · {formatNumber(data.instruments_seen, { decimals: 0 })}{' '}
          instruments · computed {formatAge(data.generated_at)} ago
        </p>
      )}
    </section>
  );
}

export function TickerEvidence({ ticker }: { ticker: string }) {
  const { data, error } = useSWR<DossierResponse>(
    ticker ? `/signals/dossier/${encodeURIComponent(ticker)}?hours=72` : null,
    fetcher,
    { refreshInterval: POLL.standard },
  );
  const d = data?.dossier;
  return (
    <div className="rounded-lg border border-line p-3 space-y-1">
      <div className="flex items-center justify-between gap-2">
        <span className="stat-label">Evidence, last 72h</span>
        {d && (
          <span className={`px-1.5 py-0.5 rounded border text-micro uppercase font-bold ${STANCE_STYLE[d.stance]}`}>
            {d.stance}
          </span>
        )}
      </div>
      {error ? (
        <p className="text-micro text-ink-mute">The evidence layer could not be read.</p>
      ) : !data ? (
        <p className="text-micro text-ink-mute">Reading…</p>
      ) : !d ? (
        <p className="text-micro text-ink-mute">No signal family has observed {ticker} in the last 72 hours.</p>
      ) : (
        <>
          <p className="text-micro text-ink-dim">{d.summary}</p>
          <FactorList factors={d.factors} />
          <p className="text-micro text-ink-mute">
            score {formatNumber(d.score, { decimals: 2 })} · {d.proven_families || ABSENT} proven
          </p>
        </>
      )}
    </div>
  );
}

export default ConvergingEvidence;
