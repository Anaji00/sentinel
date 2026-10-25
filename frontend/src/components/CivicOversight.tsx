'use client';

/**
 * Who decides (/civic/oversight; shared/utils/oversight.py).
 *
 * Local "Community Control Over Police Surveillance" laws: places where the
 * council, not the police department alone, approves surveillance technology
 * and its rules. Each law links to its text and to where its adoption is
 * reported. The headline is the gap: how few documented agencies answer to one.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatNumber } from '../lib/format';
import { LoadingLines } from './ui/Skeleton';

export interface OversightLaw {
  place: string;
  state: string;
  level: string;
  year: number | null;
  law_url: string;
  source: string;
  note: string | null;
  ask_for: string;
}

interface CoveredLaw extends OversightLaw {
  agencies: { agency: string; state: string; tech: string[] }[];
  portals: { slug: string; cameras: number | null }[];
}

interface OversightData {
  laws: CoveredLaw[];
  covered_agencies: number;
  documented_agencies: number;
  campaign: string;
  caveat: string;
}

/** What an oversight law (or its absence) means for one agency, in a sentence. */
export function oversightLine(law: Pick<OversightLaw, 'place' | 'level' | 'year'> | null | undefined): string {
  if (!law) {
    return 'No local oversight law on record here: the department can usually buy and switch on new surveillance without a public vote.';
  }
  const who = law.level === 'county' ? 'the county board' : law.level === 'transit' ? 'the transit board' : 'the city council';
  return `${law.place} has a surveillance oversight law${law.year ? ` (${law.year})` : ''}: ${who} must approve surveillance technology and the rules for using it, in public.`;
}

export function CivicOversight({ onOpenPortal }: { onOpenPortal: (slug: string) => void }) {
  const { data, error } = useSWR<OversightData>('/civic/oversight', fetcher);
  if (error) return <p className="text-ink-mute">We couldn&apos;t load the oversight laws right now.</p>;
  if (!data) return <LoadingLines />;
  const share = data.documented_agencies ? (data.covered_agencies / data.documented_agencies) * 100 : null;
  return (
    <div className="space-y-4">
      <div className="rounded-xl border border-line bg-inset p-4 flex flex-wrap items-end gap-6">
        <div>
          <div className="text-3xl font-semibold font-mono text-caution">
            {formatNumber(data.covered_agencies, { decimals: 0 })}
            <span className="text-ink-mute text-lg"> of {data.documented_agencies ? formatNumber(data.documented_agencies, { decimals: 0 }) : ABSENT}</span>
          </div>
          <div className="text-sm text-ink-dim">
            documented agencies answer to a local oversight law
            {share != null ? ` (${share < 1 ? share.toFixed(2) : share.toFixed(1)}%)` : ''}.
          </div>
        </div>
        <p className="text-sm text-ink-dim max-w-xl">
          Everywhere else, police can usually buy and switch on plate readers, face recognition or drones without a public
          vote. {data.laws.length}{' '}places have changed that. If yours hasn&apos;t, the model law is a good place to start.{' '}
          <a href={data.campaign} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
            How to ask your council &rarr;
          </a>
        </p>
      </div>
      <ul className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-2">
        {data.laws.map((l) => (
          <li key={`${l.state}-${l.place}`} className="rounded-lg border border-line bg-raised p-3 space-y-1.5 text-xs">
            <div className="flex items-baseline justify-between gap-2">
              <span className="text-sm font-semibold text-ink">{l.place}</span>
              <span className="text-micro text-ink-mute font-mono">
                {l.state}
                {l.year ? ` · ${l.year}` : ''}
                {l.level !== 'city' ? ` · ${l.level}` : ''}
              </span>
            </div>
            {l.note && <p className="text-ink-dim">{l.note}</p>}
            {l.agencies.length > 0 ? (
              <ul className="text-ink-mute space-y-0.5">
                {l.agencies.slice(0, 4).map((a) => (
                  <li key={a.agency}>
                    <span className="text-ink-dim">{a.agency}</span>
                    {a.tech.length ? `: ${a.tech.join(', ')}` : ''}
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-ink-mute">No documented surveillance at its agencies yet.</p>
            )}
            <div className="flex flex-wrap gap-3 pt-1">
              <a href={l.law_url} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
                Read the law
              </a>
              <a href={l.source} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
                Source
              </a>
              {l.portals.map((p) => (
                <button key={p.slug} type="button" onClick={() => onOpenPortal(p.slug)} className="text-accent hover:underline">
                  Its Flock page{p.cameras ? ` (${p.cameras} cameras)` : ''}
                </button>
              ))}
            </div>
          </li>
        ))}
      </ul>
      <p className="text-micro text-ink-mute">{data.caveat}</p>
    </div>
  );
}

export default CivicOversight;
