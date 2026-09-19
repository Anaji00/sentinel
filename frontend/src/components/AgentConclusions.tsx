'use client';

/**
 * What the agent tier has concluded and stored.
 *
 * Seven Redis keys were written by one service each and read by nothing — each
 * appeared exactly once in the whole repository, at the line that wrote it. The
 * macro engine's rates regime, its inverse-correlation pairs, a hundred-point
 * history of the 2s10s spread, the Hawkes branching ratios the correlation
 * service learns, the backfill report, the curated 13F filer set. These are not
 * bookkeeping; they are the output of the work those services exist to do.
 *
 * `/agents/conclusions` gave them a reader. This gives them a surface.
 *
 * Every field renders `null` distinctly from empty: "the engine has not run"
 * and "the engine ran and found nothing" are different facts, and a dash in
 * the same weight as a number is how the difference gets lost.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { PALETTE } from '../lib/palette';
import { EmptyState } from './ui/EmptyState';
import { DataGrid } from './ui/DataGrid';
import { POLL } from './ui/DataProvider';

interface SpreadSeries {
  points: number[];
  latest: number | null;
  inverted: boolean | null;
}

interface KeyedRow {
  key: string;
  value: unknown;
}

interface Conclusions {
  rates_regime: Record<string, unknown> | string | null;
  hawkes_branching_ratios: Record<string, number> | null;
  tradfi_backfill: Record<string, unknown> | string | null;
  yield_spread_2y10y_bps: SpreadSeries | null;
  inverse_correlations: KeyedRow[];
  correlation_analysis: KeyedRow[];
  prominent_ciks: string[] | null;
}

const Card: React.FC<{ title: string; note?: string; children: React.ReactNode }> = ({
  title,
  note,
  children,
}) => (
  <section className="rounded-panel border border-line bg-raised p-3.5">
    <div className="mb-2 flex items-baseline justify-between gap-2">
      <h3 className="text-micro font-semibold text-ink">{title}</h3>
      {note && <span className="text-micro text-ink-mute">{note}</span>}
    </div>
    {children}
  </section>
);

/** A dash, in muted weight, so an absent value never reads as a measured one. */
const Absent = ({ why }: { why: string }) => (
  <span className="text-ink-mute" title={why}>
    —
  </span>
);

/**
 * The 2s10s spread, drawn.
 *
 * A hundred points were being kept and never read. An inverted curve is a
 * recession signal, so the zero line is drawn explicitly rather than left for
 * the reader to infer from the axis.
 */
function SpreadSparkline({ series }: { series: SpreadSeries }) {
  const pts = series.points.filter((p) => Number.isFinite(p));
  if (pts.length < 2) {
    return <Absent why="Fewer than two points recorded" />;
  }

  const w = 240;
  const h = 44;
  const min = Math.min(...pts, 0);
  const max = Math.max(...pts, 0);
  const span = max - min || 1;
  const x = (i: number) => (i / (pts.length - 1)) * w;
  const y = (v: number) => h - ((v - min) / span) * h;

  const path = pts
    .map((v, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${y(v).toFixed(1)}`)
    .join(' ');
  const zeroY = y(0);
  const inverted = series.inverted === true;

  return (
    <div>
      <svg
        viewBox={`0 0 ${w} ${h}`}
        className="w-full"
        role="img"
        aria-label={`2s10s spread, ${pts.length} points, latest ${series.latest ?? 'unknown'} basis points`}
      >
        {/* Zero is the line that matters: below it the curve is inverted. */}
        <line
          x1={0}
          x2={w}
          y1={zeroY}
          y2={zeroY}
          stroke={PALETTE.borderStrong}
          strokeDasharray="2 3"
        />
        <path
          d={path}
          fill="none"
          strokeWidth={1.5}
          stroke={inverted ? PALETTE.negative : PALETTE.accent}
        />
      </svg>
      <div className="mt-1 flex items-baseline gap-2 text-micro">
        <span
          className="tabular-nums font-medium"
          style={{ color: inverted ? PALETTE.negative : PALETTE.textPrimary }}
        >
          {series.latest != null ? `${series.latest.toFixed(1)} bps` : '—'}
        </span>
        <span className="text-ink-mute">
          {series.inverted == null ? 'no reading' : inverted ? 'inverted' : 'positive slope'}
        </span>
        <span className="text-ink-mute">· {pts.length} points</span>
      </div>
    </div>
  );
}

/**
 * The cross-domain excitation matrix, as bars.
 *
 * A branching ratio above 1.0 means a domain is currently exciting itself or
 * another faster than its own baseline — which is the whole reason the Hawkes
 * process is fitted, and it had never been shown.
 */
function BranchingRatios({ ratios }: { ratios: Record<string, number> }) {
  const rows = Object.entries(ratios)
    .filter(([, v]) => Number.isFinite(v))
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);
  if (rows.length === 0) return <Absent why="No ratios recorded" />;

  const max = Math.max(...rows.map(([, v]) => v), 1.5);
  return (
    <ul className="space-y-1">
      {rows.map(([pair, value]) => {
        const excited = value > 1.0;
        return (
          <li key={pair} className="flex items-center gap-2">
            <span className="w-32 shrink-0 truncate text-micro text-ink-dim">{pair}</span>
            <span className="relative h-1.5 flex-1 overflow-hidden rounded-full bg-white/5">
              <span
                className="absolute inset-y-0 left-0 rounded-full"
                style={{
                  width: `${Math.min(100, (value / max) * 100)}%`,
                  background: excited ? PALETTE.caution : PALETTE.accent,
                }}
              />
            </span>
            <span
              className="w-10 shrink-0 text-right text-micro tabular-nums"
              style={{ color: excited ? PALETTE.caution : PALETTE.textMuted }}
            >
              {value.toFixed(2)}
            </span>
          </li>
        );
      })}
    </ul>
  );
}

/**
 * A stored document, rendered through the grid this codebase already has.
 *
 * This was a hand-rolled <dl> that fell back to `JSON.stringify(...).slice(0, 60)`
 * for nested values -- which `test_no_raw_json_dumps_rendered_in_the_ui` caught
 * immediately. `DataGrid` exists for exactly this: it humanises snake_case keys
 * into labels, bounds float precision so a 17-digit value cannot reach the
 * panel, and keeps the raw path as a tooltip so an operator can cross-reference
 * against the API response. Reinventing it badly is how a codebase ends up with
 * two conventions.
 */
function Facts({ value }: { value: unknown }) {
  if (value == null) return <Absent why="Key absent — the engine has not written one" />;
  if (typeof value === 'string') {
    return <p className="text-micro leading-relaxed text-ink-dim">{value}</p>;
  }
  return <DataGrid data={value} emptyLabel="Stored, and empty" className="text-micro" />;
}

export default function AgentConclusions() {
  const { data, error, isLoading } = useSWR<Conclusions>('/api/v1/agents/conclusions', fetcher, {
    refreshInterval: POLL.rare,
  });

  if (error) {
    return (
      <EmptyState
        kind="error"
        title="Could not read the agent tier's conclusions"
        detail="These are stored by the macro engine, the correlation service and the filings collector. Unreachable is not the same as unwritten."
      />
    );
  }
  if (isLoading || !data) {
    return <EmptyState kind="loading" title="Reading stored conclusions…" />;
  }

  return (
    <div className="grid grid-cols-1 gap-3 lg:grid-cols-2 xl:grid-cols-3">
      <Card title="Rates regime" note="macro engine">
        <Facts value={data.rates_regime} />
      </Card>

      <Card
        title="2s10s spread"
        note={data.yield_spread_2y10y_bps ? 'last 100 readings' : undefined}
      >
        {data.yield_spread_2y10y_bps ? (
          <SpreadSparkline series={data.yield_spread_2y10y_bps} />
        ) : (
          <Absent why="No series recorded" />
        )}
      </Card>

      <Card title="Cross-domain excitation" note="Hawkes branching ratios">
        {data.hawkes_branching_ratios ? (
          <BranchingRatios ratios={data.hawkes_branching_ratios} />
        ) : (
          <Absent why="The correlation service has not persisted a fit" />
        )}
      </Card>

      <Card title="Inverse correlations" note={`${data.inverse_correlations?.length ?? 0} pairs`}>
        {data.inverse_correlations?.length ? (
          <ul className="space-y-0.5">
            {data.inverse_correlations.slice(0, 8).map((r) => (
              <li key={r.key} className="truncate text-micro text-ink-dim">
                {r.key}
              </li>
            ))}
          </ul>
        ) : (
          <Absent why="No pairs stored" />
        )}
      </Card>

      <Card
        title="Agent correlation analysis"
        note={`${data.correlation_analysis?.length ?? 0} agents`}
      >
        {data.correlation_analysis?.length ? (
          <ul className="space-y-0.5">
            {data.correlation_analysis.slice(0, 8).map((r) => (
              <li key={r.key} className="truncate text-micro text-ink-dim">
                {r.key}
              </li>
            ))}
          </ul>
        ) : (
          <Absent why="Nothing cached — the agent publishes to a topic instead" />
        )}
      </Card>

      <Card title="Historical backfill" note="last run">
        <Facts value={data.tradfi_backfill} />
      </Card>

      <Card
        title="Prominent 13F filers"
        note={data.prominent_ciks ? `${data.prominent_ciks.length} CIKs` : undefined}
      >
        {data.prominent_ciks?.length ? (
          <p className="text-micro leading-relaxed text-ink-dim">
            {data.prominent_ciks.slice(0, 24).join(',')}
            {data.prominent_ciks.length > 24 && `+${data.prominent_ciks.length - 24}`}
          </p>
        ) : (
          <Absent why="The filings collector has not curated a set" />
        )}
      </Card>
    </div>
  );
}
