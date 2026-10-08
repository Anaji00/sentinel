'use client';

/**
 * The conflict-zone list beside the map, and the detail for one zone: why it
 * is a zone, what it moves, what the news and the navigational warnings say,
 * and what this platform observed there in the last day.
 */

import React from 'react';
import { ABSENT, formatAge, formatNumber } from '../../lib/format';
import {
  ACTIVITY_LABELS,
  ConflictZone,
  ExposedInstrument,
  ZONE_COLOURS,
} from '../../lib/conflict';
import { IconClose } from '../ui/icons';

function Sparkline({ series }: { series: number[] }) {
  if (series.length < 2) return null;
  const w = 220;
  const h = 28;
  const max = Math.max(...series, 1);
  const step = w / (series.length - 1);
  const points = series
    .map((v, i) => `${(i * step).toFixed(1)},${(h - (v / max) * h).toFixed(1)}`)
    .join(' ');
  return (
    <svg width={w} height={h} className="block" aria-label="Hourly article volume, seven days">
      <polyline points={points} fill="none" stroke="#f87171" strokeWidth={1} />
    </svg>
  );
}

function StatusBadge({ zone }: { zone: ConflictZone }) {
  const c = ZONE_COLOURS[zone.status] ?? ZONE_COLOURS.watch;
  return (
    <span
      className="px-1.5 py-0.5 rounded text-micro font-bold uppercase tracking-wide border"
      style={{ color: c.stroke, borderColor: c.stroke, background: c.fill }}
    >
      {c.label}
    </span>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="pt-2">
      <div className="text-ink-dim uppercase text-micro font-bold tracking-wide pb-1">{title}</div>
      {children}
    </div>
  );
}

function trendText(zone: ConflictZone): string {
  if (zone.news_ratio == null) return 'No baseline yet';
  const pct = Math.round((zone.news_ratio - 1) * 100);
  return `${pct >= 0 ? '+' : ''}${pct}% vs 7-day daily mean`;
}

export function ConflictZoneList({
  zones,
  onSelect,
}: {
  zones: ConflictZone[];
  onSelect: (zoneId: string) => void;
}) {
  return (
    <div className="space-y-1">
      {zones.map((z) => {
        const c = ZONE_COLOURS[z.status] ?? ZONE_COLOURS.watch;
        return (
          <button
            key={z.zone_id}
            onClick={() => onSelect(z.zone_id)}
            className="w-full text-left flex items-center justify-between gap-2 px-2 py-1 rounded hover:bg-overlay cursor-pointer"
          >
            <span className="flex items-center gap-2 min-w-0">
              <span className="h-2 w-2 rounded-sm shrink-0" style={{ background: c.stroke }} />
              <span className="truncate">{z.name}</span>
            </span>
            <span className="font-mono text-ink-dim shrink-0">
              {z.trend === 'rising' ? '▲ ' : z.trend === 'falling' ? '▼ ' : ''}
              {formatNumber(z.news_volume_24h, { decimals: 0 })}
            </span>
          </button>
        );
      })}
    </div>
  );
}

export function ConflictZoneDetail({
  zone,
  onBack,
}: {
  zone: ConflictZone;
  onBack: () => void;
}) {
  const activity = Object.entries(zone.activity_24h).filter(([, n]) => n > 0);
  return (
    <div className="space-y-1 text-micro text-ink-dim">
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="text-white font-bold text-xs">{zone.name}</div>
          <div className="pt-1 flex items-center gap-2 flex-wrap">
            <StatusBadge zone={zone} />
            <span>{zone.prior === 'active' ? 'Standing: active war' : 'Standing: flashpoint'}</span>
          </div>
        </div>
        <button
          onClick={onBack}
          aria-label="Back to zone list"
          className="text-ink-dim hover:text-white font-bold bg-overlay px-2 py-0.5 rounded cursor-pointer"
        >
          <IconClose />
        </button>
      </div>

      <Section title="News intensity (GDELT, all languages)">
        <div>
          <span className="text-white font-bold">{formatNumber(zone.news_volume_24h, { decimals: 0 })}</span>{' '}
          articles in 24h &middot; {trendText(zone)}
        </div>
        <Sparkline series={zone.volume_series} />
        <div className="text-ink-mute">
          Updated {zone.live_updated_at ? `${formatAge(zone.live_updated_at)} ago` : ABSENT}
        </div>
      </Section>

      <Section title="Parties">
        <div className="text-ink">{zone.parties.join(' · ')}</div>
      </Section>

      <Section title="Why it is a zone">
        <ul className="list-disc pl-4 space-y-0.5 text-ink">
          {zone.drivers.map((d) => (
            <li key={d}>{d}</li>
          ))}
        </ul>
      </Section>

      <Section title="Ripple effects">
        <ul className="space-y-1">
          {zone.ripple.map((r) => (
            <li key={r.target}>
              <span
                className={
                  'font-bold font-mono ' +
                  (r.kind === 'instrument'
                    ? 'text-cyan-300'
                    : r.kind === 'chokepoint'
                      ? 'text-amber-300'
                      : 'text-purple-300')
                }
              >
                {r.target}
              </span>{' '}
              <span className="text-ink-mute">({r.kind})</span> <span className="text-ink">{r.why}</span>
              {r.latest_move && (
                <div className="pl-3 text-emerald-300">
                  Latest move {formatAge(r.latest_move.occurred_at)} ago: {r.latest_move.headline ?? ABSENT}
                </div>
              )}
            </li>
          ))}
        </ul>
      </Section>

      <Section title="Observed by this platform, 24h">
        {activity.length === 0 ? (
          <div className="text-ink-mute">
            Nothing observed{zone.regions.length === 0 ? ' (no tracked regions here; news only)' : ''}.
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-x-3">
            {activity.map(([type, n]) => (
              <div key={type} className="flex justify-between">
                <span>{ACTIVITY_LABELS[type] ?? type}</span>
                <span className="text-white font-mono">{formatNumber(n, { decimals: 0 })}</span>
              </div>
            ))}
          </div>
        )}
      </Section>

      {zone.nav_warnings.length > 0 && (
        <Section title={`Navigational warnings (${zone.nav_warnings.length})`}>
          <ul className="space-y-1">
            {zone.nav_warnings.slice(0, 6).map((w) => (
              <li key={w.id}>
                <span className={w.military ? 'text-rose-400 font-bold' : 'text-amber-300'}>{w.id}</span>{' '}
                <span className="text-ink">{w.area}</span>
                <div className="text-ink-mute whitespace-pre-line line-clamp-3">{w.text}</div>
              </li>
            ))}
          </ul>
        </Section>
      )}

      <Section title="Latest reporting">
        {zone.news.length === 0 && zone.platform_headlines.length === 0 ? (
          <div className="text-ink-mute">No headlines yet.</div>
        ) : (
          <ul className="space-y-1">
            {zone.platform_headlines.slice(0, 5).map((h) => (
              <li key={`p-${h.occurred_at}-${h.headline}`}>
                <span className="text-ink-mute">{formatAge(h.occurred_at)} · {h.source ?? ABSENT} · </span>
                {h.url ? (
                  <a href={h.url} target="_blank" rel="noopener noreferrer" className="text-ink hover:text-accent">
                    {h.headline}
                  </a>
                ) : (
                  <span className="text-ink">{h.headline}</span>
                )}
              </li>
            ))}
            {zone.news.slice(0, 10).map((a) => (
              <li key={a.url}>
                <span className="text-ink-mute">{a.domain ?? ABSENT} · </span>
                <a href={a.url} target="_blank" rel="noopener noreferrer" className="text-ink hover:text-accent">
                  {a.title}
                </a>
              </li>
            ))}
          </ul>
        )}
      </Section>
    </div>
  );
}

/** Instruments ranked by how much the conflict zones bear on them. */
export function ConflictExposureList({ instruments }: { instruments: ExposedInstrument[] }) {
  if (instruments.length === 0) {
    return <div className="text-ink-mute">No exposure measured yet.</div>;
  }
  return (
    <ul className="space-y-2">
      {instruments.map((i) => (
        <li key={i.symbol} className="border-b border-line pb-1.5">
          <div className="flex items-center justify-between gap-2">
            <span className="font-mono font-bold text-cyan-300">{i.symbol}</span>
            <span className="font-mono text-ink-dim">{formatNumber(i.score, { decimals: 2 })}</span>
          </div>
          <div className="flex flex-wrap gap-1 pt-0.5">
            {i.zones.map((z) => {
              const c = ZONE_COLOURS[z.status] ?? ZONE_COLOURS.watch;
              return (
                <span
                  key={z.zone_id}
                  title={z.why}
                  className="px-1 rounded border text-micro"
                  style={{ color: c.stroke, borderColor: c.stroke }}
                >
                  {z.name}
                </span>
              );
            })}
          </div>
          {(i.mentions > 0 || i.observations > 0) && (
            <div className="text-ink-mute">
              {formatNumber(i.mentions, { decimals: 0 })} zone headlines ·{' '}
              {formatNumber(i.observations, { decimals: 0 })} observations
            </div>
          )}
          {i.latest_move && (
            <div className="text-emerald-300">
              {formatAge(i.latest_move.occurred_at)} ago: {i.latest_move.headline ?? ABSENT}
            </div>
          )}
          {i.holders && <div className="text-ink-mute">Holders: {i.holders}</div>}
        </li>
      ))}
    </ul>
  );
}
