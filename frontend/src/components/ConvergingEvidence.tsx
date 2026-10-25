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
import { formatAge, formatNumber } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { familyName } from '../lib/signalNames';
import { LoadingLines } from './ui/Skeleton';

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

/** Each stance in words: the codes read like order tickets. */
export const STANCE_WORD: Record<string, string> = {
  long: 'Leans up',
  short: 'Leans down',
  mixed: 'Mixed',
  attention: 'Worth watching',
  none: 'No lean',
};

/** How a pattern has done in testing, in a phrase a reader can weigh. */
export function testedWords(f: EvidenceFactor): string {
  const right = f.study.hit_rate != null ? `right ${formatNumber(f.study.hit_rate * 100, { decimals: 0 })}% of the time` : '';
  if (f.status === 'proven') return right ? `proven: ${right}` : 'proven';
  if (f.status === 'no_edge') return "tested: doesn't predict prices";
  return 'not tested yet';
}

const STANCE_STYLE: Record<string, string> = {
  long: 'text-positive border-positive/30',
  short: 'text-negative border-negative/30',
  mixed: 'text-caution border-caution/30',
  attention: 'text-ink-dim border-line',
  none: 'text-ink-mute border-line',
};

const STATUS_STYLE: Record<string, string> = {
  proven: 'text-positive',
  unstudied: 'text-ink-dim',
  no_edge: 'text-ink-mute line-through',
};

function arrow(d: number): string {
  return d > 0 ? '▲' : d < 0 ? '▼' : '•';
}

/** The evidence on one stock in a sentence: how many signs, which way, how many proven. */
export function evidenceLine(factors: EvidenceFactor[]): string {
  const up = factors.filter((f) => f.direction > 0).length;
  const down = factors.filter((f) => f.direction < 0).length;
  const proven = factors.filter((f) => f.status === 'proven').length;
  const n = factors.length;
  const ways = [up ? `${up} point${up === 1 ? 's' : ''} up` : '', down ? `${down} point${down === 1 ? 's' : ''} down` : '']
    .filter(Boolean)
    .join(', ');
  return `${n} sign${n === 1 ? '' : 's'} in 3 days${ways ? `: ${ways}` : ''}. ${
    proven ? `${proven} ${proven === 1 ? 'is a pattern' : 'are patterns'} proven on years of data.` : 'None is a proven pattern yet.'
  }`;
}

function FactorList({ factors }: { factors: EvidenceFactor[] }) {
  return (
    <ul className="space-y-1 pt-1">
      {factors.map((f) => (
        <li
          key={f.family}
          className="flex flex-wrap items-baseline gap-x-2 text-micro"
          title={`${f.family} · source ${f.source}${f.study.units != null ? ` · ${f.study.units} past events` : ''}${
            f.study.mean_bps != null ? ` · average move ${formatNumber(f.study.mean_bps / 100, { decimals: 2 })}%` : ''
          }`}
        >
          <span className={f.direction > 0 ? 'text-positive' : f.direction < 0 ? 'text-negative' : 'text-ink-mute'}>
            {arrow(f.direction)}
          </span>
          <span className={STATUS_STYLE[f.status] ?? ''}>{familyName(f.family)}</span>
          <span className="text-ink-mute">
            {f.events === 1 ? 'once' : `${f.events} times`} · {testedWords(f)} · last {formatAge(f.last_seen)} ago
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
        <h3 className="font-bold text-ink">Where the evidence agrees</h3>
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
        Stocks where two or more separate sources point the same way: options traders, company insiders, big funds,
        the news, shipping, or trading itself. Patterns that come from the same source count once, and proven patterns
        count more than untested ones. A list of evidence, not a trade.
      </p>
      {error ? (
        <p className="text-ink-mute">The evidence layer could not be read.</p>
      ) : !data ? (
        <LoadingLines />
      ) : data.converging.length === 0 ? (
        <p className="text-ink-mute">
          No stock has two separate sources agreeing in the last {data.window_hours} hours (
          {formatNumber(data.instruments_seen, { decimals: 0 })} stocks and {formatNumber(data.events, { decimals: 0 })}{' '}
          events read).
        </p>
      ) : (
        <ul className="space-y-1.5">
          {data.converging.map((d) => (
            <li key={d.instrument} className="border-b border-line/60 pb-1.5">
              <div className="flex flex-wrap items-center gap-2">
                <span className={`px-2 py-0.5 rounded-full border text-micro font-semibold ${STANCE_STYLE[d.stance]}`}>
                  {STANCE_WORD[d.stance] ?? d.stance}
                </span>
                <Link
                  href={`/ticker?symbol=${encodeURIComponent(d.instrument)}`}
                  className="font-mono font-bold text-ink hover:text-accent hover:underline"
                >
                  {d.instrument}
                </Link>
                <button
                  onClick={() => setOpen(open === d.instrument ? null : d.instrument)}
                  aria-expanded={open === d.instrument}
                  className="flex-1 text-left flex flex-wrap items-center justify-between gap-2 cursor-pointer"
                >
                  <span className="text-ink-dim">
                    {d.agreeing_families} source{d.agreeing_families === 1 ? '' : 's'} agree
                    {d.opposing_families ? `, ${d.opposing_families} disagree` : ''}
                    {d.attention_families ? `, ${d.attention_families} worth watching` : ''}
                  </span>
                  <span className="text-micro" title={`Evidence score ${formatNumber(d.score, { decimals: 2 })} (−1 to +1)`}>
                    <span className={d.proven_families ? 'text-positive font-semibold' : 'text-ink-mute'}>
                      {d.proven_families
                        ? `${d.proven_families} proven pattern${d.proven_families === 1 ? '' : 's'}`
                        : 'nothing proven yet'}
                    </span>
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
          {formatNumber(data.events, { decimals: 0 })} events across {formatNumber(data.instruments_seen, { decimals: 0 })}{' '}
          stocks · updated {formatAge(data.generated_at)} ago
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
        <span className="stat-label">What happened, last 3 days</span>
        {d && (
          <span className={`px-2 py-0.5 rounded-full border text-micro font-semibold ${STANCE_STYLE[d.stance]}`}>
            {STANCE_WORD[d.stance] ?? d.stance}
          </span>
        )}
      </div>
      {error ? (
        <p className="text-micro text-ink-mute">The evidence layer could not be read.</p>
      ) : !data ? (
        <LoadingLines />
      ) : !d ? (
        <p className="text-micro text-ink-mute">Nothing unusual has happened around {ticker} in the last 3 days.</p>
      ) : (
        <>
          <p className="text-micro text-ink-dim" title={d.summary}>
            {evidenceLine(d.factors)}
          </p>
          <FactorList factors={d.factors} />
        </>
      )}
    </div>
  );
}

export default ConvergingEvidence;
