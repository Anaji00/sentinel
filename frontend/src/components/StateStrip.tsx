'use client';

/**
 * What is true right now, above everything else on the page.
 *
 * The command centre opened on four equally-weighted panels and a row of view
 * toggles. Nothing on the screen answered the first question an operator has --
 * is this thing actually seeing anything? -- so the answer had to be inferred
 * from whether the panels below happened to have rows in them. An empty feed
 * and a dead ingest look identical that way.
 *
 * `/health/sources` has been serving exactly this for the whole life of the
 * project and no component ever called it. Measured against the running stack
 * while this was written: 55 sources, 15 of them silent past their own cadence
 * and 25 with a collapsed arrival rate. None of that reached a screen.
 *
 * Every figure here is allowed to be unknown. A probe that fails renders as an
 * em dash with the reason underneath, never as zero -- "no sources reporting"
 * and "could not ask" are different facts and the strip is the one place in
 * the app where confusing them would be worst.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { ABSENT, formatCompact, formatDuration } from '../lib/format';
import { IconAlert, IconCheck, IconRadar, IconStore } from './ui/icons';
import { POLL } from './ui/DataProvider';

/** One entry of `/health/sources`. Matches `SourceHealth` on the gateway. */
interface SourceHealth {
  source: string;
  stale: boolean;
  rate_collapsed: boolean;
  filtered_not_silent: boolean;
  silent_seconds: number | null;
  expected_interval_seconds: number | null;
  observations: number;
  basis: string | null;
}

/** `/search/status`: whether semantic retrieval can answer at all. */
interface SearchStatus {
  available: boolean;
  reason?: string;
  collection?: string;
  indexed_events: number | null;
}

interface PlatformHealth {
  status: string;
  redis_connected: boolean;
  timescale_connected: boolean;
  neo4j_connected: boolean;
  active_configuration?: {
    maritime_dark_thresholds?: number;
    tracked_financial_instruments?: number;
  };
}

type Tone = 'neutral' | 'positive' | 'caution' | 'negative' | 'unknown';

const TONE_CLASS: Record<Tone, string> = {
  neutral: 'text-ink',
  positive: 'text-positive',
  caution: 'text-caution',
  negative: 'text-negative',
  unknown: 'text-ink-mute',
};

interface FigureProps {
  label: string;
  /** The figure itself, already formatted. `null` renders as absent. */
  value: string | null;
  /** One line under the figure saying what it is measured against. */
  basis: string;
  tone?: Tone;
  icon?: React.ReactNode;
}

/**
 * A label, a figure, and what the figure is measured against.
 *
 * The basis line is not decoration. "15 silent" means nothing without "of 55",
 * and "4 stores" means nothing without knowing that four is all of them.
 */
function Figure({ label, value, basis, tone = 'neutral', icon }: FigureProps) {
  const absent = value === null;
  return (
    <div className="flex min-w-0 flex-col gap-0.5 pr-3">
      <span className="stat-label flex items-center gap-1.5 whitespace-nowrap">
        {icon}
        {label}
      </span>
      <span
        className={`font-mono text-head font-semibold leading-none ${
          absent ? TONE_CLASS.unknown : TONE_CLASS[tone]
        }`}
      >
        {absent ? ABSENT : value}
      </span>
      <span className="truncate text-micro text-ink-mute" title={basis}>
        {basis}
      </span>
    </div>
  );
}

export default function StateStrip() {
  const { data: sources, error: sourcesError } = useSWR<SourceHealth[]>(
    '/health/sources',
    fetcher,
    { refreshInterval: POLL.slow },
  );
  // No trailing slash, despite the gateway wanting one.
  //
  // The gateway answers `/api/v1/health/` and 307s `/api/v1/health`. Reaching
  // for the slash here looks right and is wrong: Next normalises trailing
  // slashes on the proxy route, so `/api/proxy/api/v1/health/` 308s to the
  // slashless form in the *browser* before anything is forwarded -- a client
  // round trip, on a refresh timer. Without it the proxy's own server-side
  // fetch follows the gateway's 307 internally and the browser sees one 200.
  //
  // Measured through the deployment: `health` -> 200, `health/` -> 308.
  const { data: platform, error: platformError } = useSWR<PlatformHealth>('/health', fetcher, {
    refreshInterval: POLL.slow,
  });

  // A failed probe and an empty response are different. SWR gives `undefined`
  // for both "not yet" and "failed", so the error is checked first and the
  // loading case is told apart from a genuine empty list by `Array.isArray`.
  const sourceList = Array.isArray(sources) ? sources : null;

  const reporting = sourceList ? sourceList.filter((s) => !s.stale).length : null;
  const collapsed = sourceList ? sourceList.filter((s) => s.rate_collapsed).length : null;

  // The longest any source has been silent, among those that are late. This is
  // the figure that says how bad the staleness is rather than how wide it is.
  const worstSilence = sourceList
    ? sourceList
        .filter((s) => s.stale && typeof s.silent_seconds === 'number')
        .reduce<number | null>(
          (worst, s) => (worst === null || s.silent_seconds! > worst ? s.silent_seconds! : worst),
          null,
        )
    : null;

  const stores = platform
    ? [platform.redis_connected, platform.timescale_connected, platform.neo4j_connected].filter(
        Boolean,
      ).length
    : null;

  const instruments = platform?.active_configuration?.tracked_financial_instruments ?? null;

  // Semantic retrieval reported itself unavailable for the life of the project
  // -- against 493,878 indexed vectors -- and nothing on any screen said so.
  // A capability that is off is a different fact from one that is quiet.
  const { data: search, error: searchError } = useSWR<SearchStatus>('/search/status', fetcher, {
    refreshInterval: POLL.slow,
  });

  const sourceTone: Tone =
    sourceList === null
      ? 'unknown'
      : reporting === sourceList.length
        ? 'positive'
        : reporting! === 0
          ? 'negative'
          : 'caution';

  const sourceBasis = sourcesError
    ? 'health probe unreachable'
    : sourceList === null
      ? 'asking…'
      : `of ${sourceList.length} within their own cadence`;

  return (
    <section
      aria-label="Platform state"
      className="panel grid w-full grid-cols-2 gap-x-1 gap-y-1 px-3.5 py-2 sm:grid-cols-3 lg:grid-cols-6"
    >
      <Figure
        label="Sources reporting"
        value={sourceList === null ? null : String(reporting)}
        basis={sourceBasis}
        tone={sourceTone}
        icon={<IconRadar />}
      />
      <Figure
        label="Rate collapsed"
        value={collapsed === null ? null : String(collapsed)}
        basis={
          collapsed === null
            ? sourcesError
              ? 'health probe unreachable'
              : 'asking…'
            : collapsed === 0
              ? 'every source at its usual rate'
              : 'arriving far below their usual rate'
        }
        tone={collapsed === null ? 'unknown' : collapsed > 0 ? 'caution' : 'positive'}
        icon={collapsed ? <IconAlert /> : <IconCheck />}
      />
      <Figure
        label="Longest silence"
        value={worstSilence === null ? null : formatDuration(worstSilence)}
        basis={
          worstSilence === null
            ? sourceList === null
              ? 'asking…'
              : 'no source is past its cadence'
            : 'since the latest source last delivered'
        }
        tone={worstSilence === null ? 'positive' : 'caution'}
      />
      <Figure
        label="Stores"
        value={stores === null ? null : `${stores}/3`}
        basis={
          platformError
            ? 'health probe unreachable'
            : stores === null
              ? 'asking…'
              : 'Timescale, Neo4j, Redis'
        }
        tone={stores === null ? 'unknown' : stores === 3 ? 'positive' : 'negative'}
        icon={<IconStore />}
      />
      <Figure
        label="Instruments tracked"
        value={instruments === null ? null : String(instruments)}
        basis={instruments === null ? 'asking…' : 'symbols under active collection'}
      />
      <Figure
        label="Semantic index"
        value={
          searchError || !search
            ? null
            : search.available && search.indexed_events !== null
              ? formatCompact(search.indexed_events)
              : 'off'
        }
        basis={
          searchError
            ? 'index probe unreachable'
            : !search
              ? 'asking…'
              : search.available
                ? 'events retrievable by similarity'
                : (search.reason ?? 'unavailable')
        }
        tone={!search ? 'unknown' : search.available ? 'positive' : 'caution'}
        icon={<IconStore />}
      />
    </section>
  );
}
