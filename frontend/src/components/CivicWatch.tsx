'use client';

/**
 * Surveillance Watch (/civic/*; services/api_gateway/routes/civic.py).
 *
 * One connected place rather than a map with eight tabs underneath. Three
 * layers, each leading to the next:
 *
 *   1. Find a place: search a town or agency, or click the map.
 *   2. Its story, in the side panel: what watches here, who runs it, whether
 *      that follows the law, who gets a say (local oversight), and what you can
 *      do -- each step a click from the last, with a trail back.
 *   3. Explore: what is changing, accountability, the money, and protecting
 *      yourself. The sidebar's Civic entries open these by hash (#money).
 *
 * Agencies and vendors are the subject: nothing here looks up a plate or a
 * person, and a clicked place is not stored.
 */

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Input } from './ui/Input';
import { Button } from './ui/Button';
import useSWR from 'swr';
import { useSearchParams } from 'next/navigation';
import { usePreferences } from '../lib/preferences';
import { TickerChip } from './ui/TickerChip';
import { IconFollow } from './ui/icons';
import 'leaflet/dist/leaflet.css';
import type * as Leaflet from 'leaflet';
import { fetcher } from '../lib/api';
import { ABSENT, formatNumber } from '../lib/format';
import { Disclosure } from './ui/Disclosure';
import { arc, councilQuestion, trailLines, verdictHeadline, verdictLine, type VerdictPortal } from '../lib/watchlight';
import {
  CourtCases,
  DataBrokers,
  Departures,
  DroneWaivers,
  FederalMoney,
  Findings,
  Lobbying,
  PortalPanel,
  SearchLogs,
  Stat,
  WatchFeed,
} from './CivicLegal';
import { CivicOversight, oversightLine, type OversightLaw } from './CivicOversight';
import { PrivacyPlan } from './PrivacyPlan';

interface Count {
  name: string;
  count: number;
}

interface Summary {
  totals: { devices?: number; agencies?: number; records?: number; devices_updated?: string; agencies_updated?: string };
  devices_by_kind?: Count[];
  devices_by_maker?: Count[];
  tech_by_type?: Count[];
  vendors?: Count[];
}

interface Device {
  osm_id: number;
  lat: number;
  lon: number;
  kind: string;
  manufacturer: string | null;
  operator: string | null;
  direction: string | null;
  km?: number;
}

interface AgencyRow {
  agency: string;
  state: string;
  city: string | null;
  lat: number | null;
  lon: number | null;
  technologies: string[];
  vendors: string[];
  intensity: number;
}

interface Evidence {
  summary: string | null;
  link: string | null;
  date: string | null;
}

interface ReportCard {
  portal_slug?: string | null;
  agency: string;
  state: string;
  city: string | null;
  type: string | null;
  placed_by: string | null;
  lat?: number | null;
  lon?: number | null;
  intensity: number;
  max_intensity: number;
  vendors: string[];
  nearby_devices: Record<string, number>;
  technologies: { technology: string; what_it_means: string | null; vendors: string[]; evidence: Evidence[] }[];
  oversight?: OversightLaw | null;
  oversight_campaign?: string;
}

interface Near {
  radius_km: number;
  device_counts: Record<string, number>;
  devices: Device[];
  agencies: { agency: string; state: string; city: string | null; km: number; technologies: string[]; vendors: string[] }[];
  caveat: string;
}

/** Plain names for the mapped device kinds. */
export const KIND_LABEL: Record<string, string> = {
  ALPR: 'Licence-plate reader',
  gunshot_detector: 'Gunshot detector',
};

export const KIND_COLOR: Record<string, string> = {
  ALPR: '#f43f5e',
  gunshot_detector: '#f59e0b',
};

/** An agency's dot: brighter the more invasive its documented toolkit. */
export function intensityColor(intensity: number): string {
  if (intensity >= 9) return '#e11d48';
  if (intensity >= 6) return '#f97316';
  if (intensity >= 3) return '#eab308';
  return '#64748b';
}

/** How much surveillance an agency's documented toolkit adds up to, in words. */
export function intensityWord(intensity: number): string {
  if (intensity >= 9) return 'Heavy';
  if (intensity >= 6) return 'Significant';
  if (intensity >= 3) return 'Moderate';
  return 'Light';
}

/** Sections of the explore area, opened by hash from the sidebar. */
export const SECTIONS = [
  { id: 'feed', label: "What's changing", blurb: 'News, new cameras, new contracts and new court cases, as they happen.' },
  { id: 'accountability', label: 'Accountability', blurb: 'Possible rule-breaking, who gets a say, the courts and the search logs.' },
  { id: 'money', label: 'Follow the money', blurb: 'Federal contracts, lobbying, drone permits and data brokers.' },
  { id: 'protect', label: 'Protect yourself', blurb: 'A short, practical privacy plan for your situation.' },
] as const;
export type SectionId = (typeof SECTIONS)[number]['id'];

const SUBVIEWS: Record<SectionId, readonly (readonly [string, string])[]> = {
  feed: [['feed', 'Timeline']],
  accountability: [
    ['findings', 'Possible rule-breaking'],
    ['oversight', 'Who gets a say'],
    ['courts', 'In court'],
    ['logs', 'Who searched the cameras'],
    ['departures', 'Places that dropped Flock'],
  ],
  money: [
    ['awards', 'Federal contracts & grants'],
    ['lobbying', 'Lobbying'],
    ['drones', 'Police drones'],
    ['brokers', 'Data brokers'],
  ],
  protect: [['plan', 'Your privacy plan']],
};

export function sectionFromHash(hash: string): SectionId | null {
  const id = hash.replace(/^#/, '');
  return SECTIONS.some((s) => s.id === id) ? (id as SectionId) : null;
}

const US_STATES = 'AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY'.split(' ');

type Story =
  | { kind: 'near'; near: Near; label: string }
  | { kind: 'agency'; card: ReportCard; label: string }
  | { kind: 'portal'; slug: string; label: string };

const DEVICE_MIN_ZOOM = 9;
/** Tiles through this deployment's gateway (/civic/tiles), never a tile server directly. */
const TILE_BASE = '/api/proxy/api/v1/civic/tiles/';

function plural(n: number, word: string): string {
  return `${formatNumber(n, { decimals: 0 })} ${word}${n === 1 ? '' : 's'}`;
}

function deviceWords(counts: Record<string, number>): string {
  const parts = Object.entries(counts).map(([k, n]) => plural(n, (KIND_LABEL[k] ?? k).toLowerCase()));
  return parts.length ? parts.join(' and ') : 'no mapped devices';
}

// ── Story panel pieces ───────────────────────────────────────────────────────

function Step({ n, title, children }: { n: number; title: string; children: React.ReactNode }) {
  return (
    <section className="space-y-1.5">
      <h3 className="flex items-center gap-2 text-sm font-semibold text-ink">
        <span className="grid place-items-center h-5 w-5 rounded-full bg-accent-dim text-accent text-micro font-mono">{n}</span>
        {title}
      </h3>
      <div className="pl-7 space-y-1.5">{children}</div>
    </section>
  );
}

function OversightBlock({ law, campaign }: { law: OversightLaw | null | undefined; campaign?: string }) {
  return (
    <div className={`rounded-lg border p-2.5 text-xs ${law ? 'border-positive/40 bg-positive/5' : 'border-caution/40 bg-caution/5'}`}>
      <p className="text-ink-dim">{oversightLine(law)}</p>
      {law ? (
        <p className="mt-1 text-ink-mute">
          You can ask for {law.ask_for}.{' '}
          <a href={law.law_url} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
            Read the law
          </a>
        </p>
      ) : (
        campaign && (
          <a href={campaign} target="_blank" rel="noopener noreferrer" className="mt-1 inline-block text-accent hover:underline">
            How to ask your council for one &rarr;
          </a>
        )
      )}
    </div>
  );
}

/** Saves this department as the person's own town, shown on their home page. */
function MakeMyTown({ agency, state }: { agency: string; state: string }) {
  const { prefs, save } = usePreferences();
  if (!prefs) return null;
  const mine = prefs.town?.agency === agency && prefs.town?.state === state;
  return (
    <button
      type="button"
      aria-pressed={mine}
      onClick={() => void save({ town: mine ? null : { agency, state } })}
      className={`shrink-0 rounded-full border px-2.5 py-0.5 text-micro ${
        mine ? 'border-civic/60 bg-civic-dim text-civic' : 'border-line text-ink-dim hover:text-ink'
      }`}
    >
      <IconFollow on={mine} className="mr-1 inline h-3 w-3 align-[-1px]" />
      {mine ? 'My town' : 'Make this my town'}
    </button>
  );
}

function AgencyStory({
  card,
  onOpenPortal,
  onExplore,
}: {
  card: ReportCard;
  onOpenPortal: (slug: string, label: string) => void;
  onExplore: (section: SectionId, view: string, state: string) => void;
}) {
  const pct = Math.min(100, (card.intensity / Math.max(1, card.max_intensity)) * 100);
  return (
    <div className="space-y-4">
      <header className="space-y-1">
        <div className="flex items-start justify-between gap-2">
          <h2 className="text-head font-semibold text-ink leading-tight">{card.agency}</h2>
          <MakeMyTown agency={card.agency} state={card.state} />
        </div>
        <div className="text-xs text-ink-mute">
          {[card.city, card.state, card.type].filter(Boolean).join(' · ')}
          {card.placed_by === 'county' ? ' · shown at the county centre' : ''}
        </div>
        <div className="flex items-center gap-2 pt-1">
          <div className="h-2 flex-1 rounded-full bg-inset overflow-hidden">
            <div className="h-full rounded-full" style={{ width: `${pct}%`, background: intensityColor(card.intensity) }} />
          </div>
          <span className="text-xs text-ink-dim">{intensityWord(card.intensity)} surveillance</span>
        </div>
      </header>

      <Step n={1} title="What they use">
        {card.technologies.length === 0 && <p className="text-xs text-ink-mute">Nothing documented yet.</p>}
        <ul className="space-y-2">
          {card.technologies.map((t) => (
            <li key={t.technology} className="text-xs">
              <div className="text-ink font-medium">
                {t.technology}
                {t.vendors.length ? (
                  <span className="text-ink-mute font-normal">
                    {' · '}
                    {t.vendors.map((v, i) => (
                      <React.Fragment key={v}>
                        {i ? ', ' : ''}
                        {v}
                        <TickerChip vendor={v} />
                      </React.Fragment>
                    ))}
                  </span>
                ) : null}
              </div>
              {t.what_it_means && <div className="text-ink-dim">{t.what_it_means}</div>}
              {t.evidence.slice(0, 2).map((e, i) => (
                <div key={i} className="text-ink-mute">
                  {e.date ? `${e.date}: ` : ''}
                  {e.summary}{' '}
                  {e.link && (
                    <a href={e.link} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
                      source
                    </a>
                  )}
                </div>
              ))}
            </li>
          ))}
        </ul>
      </Step>

      <Step n={2} title="Cameras and sensors nearby">
        <p className="text-xs text-ink-dim">
          {Object.keys(card.nearby_devices).length
            ? `Volunteers have mapped ${deviceWords(card.nearby_devices)} within 15 km.`
            : 'None mapped within 15 km yet. That can mean unmapped, not unwatched.'}
        </p>
      </Step>

      <Step n={3} title="Is it by the rules?">
        {card.portal_slug ? (
          <Button variant="link"
            type="button"
            onClick={() => onOpenPortal(card.portal_slug!, 'Flock page')} className="text-xs text-left"
          >
            They publish a Flock page. See what it says, checked against {card.state}{' '}law &rarr;
          </Button>
        ) : (
          <p className="text-xs text-ink-mute">They don&apos;t publish a Flock transparency page we can check.</p>
        )}
        <Button variant="link"
          type="button"
          onClick={() => onExplore('accountability', 'findings', card.state)} className="block text-xs"
        >
          Everything flagged in {card.state}{' '}&rarr;
        </Button>
      </Step>

      <Step n={4} title="Who gets a say">
        <OversightBlock law={card.oversight} campaign={card.oversight_campaign} />
      </Step>

      <Step n={5} title="What you can do">
        <ul className="text-xs text-ink-dim list-disc pl-4 space-y-1">
          {card.portal_slug && (
            <li>
              <Button variant="link" type="button" onClick={() => onOpenPortal(card.portal_slug!, 'Records request')}>
                Draft a public-records request
              </Button>{' '}
              (you send it yourself).
            </li>
          )}
          <li>Speak at a council meeting when surveillance contracts come up for renewal.</li>
          <li>
            <Button variant="link" type="button" onClick={() => onExplore('protect', 'plan', card.state)}>
              Make your own privacy plan
            </Button>
            .
          </li>
        </ul>
      </Step>
    </div>
  );
}

function NearStory({ near, onAgency }: { near: Near; onAgency: (name: string, state: string) => void }) {
  const total = Object.values(near.device_counts).reduce((a, b) => a + b, 0);
  return (
    <div className="space-y-4">
      <header>
        <h2 className="text-head font-semibold text-ink">Within {near.radius_km} km of this spot</h2>
        <p className="text-xs text-ink-mute">A spot you click is not saved anywhere.</p>
      </header>
      <div className="grid grid-cols-3 gap-2">
        <Stat value={total} label="mapped devices" />
        <Stat value={near.device_counts.ALPR ?? 0} label="plate readers" tone="text-negative" />
        <Stat value={near.agencies.length} label="agencies on record" />
      </div>
      {near.devices[0] && (
        <p className="text-xs text-ink-dim">The nearest is {formatNumber(near.devices[0].km ?? 0, { decimals: 2 })} km away.</p>
      )}
      <ul className="space-y-1.5">
        {near.agencies.map((a) => (
          <li key={`${a.agency}-${a.state}`}>
            <button
              type="button"
              className="w-full text-left rounded-lg border border-line bg-inset px-3 py-2 hover:border-line-strong hover:bg-overlay"
              onClick={() => onAgency(a.agency, a.state)}
            >
              <div className="text-sm text-ink">{a.agency}</div>
              <div className="text-micro text-ink-mute">
                {a.km} km · {a.technologies.join(', ')}
                {a.vendors.length ? ` · ${a.vendors.join(', ')}` : ''}
              </div>
            </button>
          </li>
        ))}
      </ul>
      <p className="text-micro text-ink-mute">{near.caveat}</p>
    </div>
  );
}

/** A figure inside a sentence that does something when pressed. A missing number shows a dash, never zero. */
function Fig({ value, tone = 'text-accent', onClick, hint }: { value: number | null | undefined; tone?: string; onClick: () => void; hint: string }) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={hint}
      className={`font-mono font-semibold tabular-nums underline decoration-dotted underline-offset-4 hover:decoration-solid ${tone}`}
    >
      {value == null ? ABSENT : formatNumber(value, { decimals: 0 })}
    </button>
  );
}

function Welcome({ summary, onExplore }: { summary?: Summary; onExplore: (section: SectionId) => void }) {
  const tech = (summary?.tech_by_type ?? []).slice(0, 8);
  const max = Math.max(1, ...tech.map((c) => c.count));
  const top = tech.slice(0, 3);
  const vendors = (summary?.vendors ?? []).slice(0, 8);
  const lead = vendors[0];
  return (
    <div className="space-y-4">
      <header className="space-y-2">
        <h2 className="text-head font-semibold text-ink">Pick a place</h2>
        <p className="text-sm text-ink-dim">Search above, tap an agency dot, or tap anywhere on the map.</p>
      </header>
      {top.length > 0 && (
        <p className="text-sm leading-relaxed text-ink">
          Police most often use {top.map((c, i) => (
            <React.Fragment key={c.name}>
              {i > 0 && (i === top.length - 1 ? ' and ' : ', ')}
              {c.name.toLowerCase()} (<span className="font-mono font-semibold tabular-nums">{formatNumber(c.count, { decimals: 0 })}</span> agencies)
            </React.Fragment>
          ))}
          .{lead && (
            <>
              {' '}
              <span className="font-semibold">{lead.name}</span> is behind <span className="font-mono font-semibold tabular-nums">{formatNumber(lead.count, { decimals: 0 })}</span> of the mapped devices.
            </>
          )}
        </p>
      )}
      <Disclosure variant="row" label="Everything police use, ranked" storageKey="civic:tech-ranking">
        <div className="space-y-3 pt-1">
          <ul className="space-y-1">
            {tech.map((c) => (
              <li key={c.name} className="text-xs">
                <div className="flex justify-between gap-2">
                  <span className="text-ink-dim truncate">{c.name}</span>
                  <span className="font-mono text-ink-mute">{formatNumber(c.count, { decimals: 0 })}</span>
                </div>
                <div className="h-1 rounded-full bg-inset overflow-hidden">
                  <div className="h-full bg-accent/70" style={{ width: `${(c.count / max) * 100}%` }} />
                </div>
              </li>
            ))}
          </ul>
          <p className="text-xs text-ink-mute">
            {vendors.map((v) => `${v.name} (${formatNumber(v.count, { decimals: 0 })})`).join(' · ') || ABSENT}
          </p>
        </div>
      </Disclosure>
      <div className="flex flex-wrap gap-1.5">
        {SECTIONS.map((s) => (
          <button
            key={s.id}
            type="button"
            title={s.blurb}
            onClick={() => onExplore(s.id)}
            className="rounded-full border border-line px-3 py-1 text-xs text-ink-dim hover:border-accent hover:text-ink"
          >
            {s.label}
          </button>
        ))}
      </div>
    </div>
  );
}

// ── Place finder ─────────────────────────────────────────────────────────────

function PlaceFinder({
  agencies,
  onPick,
  initial = '',
}: {
  agencies: AgencyRow[];
  onPick: (a: AgencyRow) => void;
  /** A town handed over from elsewhere (home's search: /watch?town=...). */
  initial?: string;
}) {
  const [q, setQ] = useState(initial);
  const [open, setOpen] = useState(initial.length > 0);
  const matches = useMemo(() => {
    const t = q.trim().toLowerCase();
    if (t.length < 2) return [];
    const scored: [number, AgencyRow][] = [];
    for (const a of agencies) {
      const hay = `${a.agency} ${a.city ?? ''} ${a.state}`.toLowerCase();
      const i = hay.indexOf(t);
      if (i >= 0) scored.push([(a.city?.toLowerCase().startsWith(t) ? 0 : 1) + i / 100 - a.intensity / 1000, a]);
    }
    return scored.sort((x, y) => x[0] - y[0]).slice(0, 8).map(([, a]) => a);
  }, [q, agencies]);
  return (
    <div className="relative w-full max-w-md">
      <Input
        label="Search a place or agency"
        hideLabel
        containerClassName="w-full"
        type="search"
        value={q}
        onChange={(e) => {
          setQ(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => window.setTimeout(() => setOpen(false), 150)}
        placeholder="Search a town, county or police department…"
      />
      {open && matches.length > 0 && (
        <ul className="absolute z-[1000] mt-1 w-full rounded-lg border border-line-strong bg-overlay shadow-overlay overflow-hidden">
          {matches.map((a) => (
            <li key={`${a.agency}-${a.state}`}>
              <button
                type="button"
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => {
                  onPick(a);
                  setQ('');
                  setOpen(false);
                }}
                className="w-full text-left px-3 py-2 hover:bg-accent-dim"
              >
                <div className="text-sm text-ink">{a.agency}</div>
                <div className="text-micro text-ink-mute">
                  {[a.city, a.state].filter(Boolean).join(', ')} · {a.technologies.slice(0, 3).join(', ')}
                </div>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ── Explore ──────────────────────────────────────────────────────────────────

function Explore({
  section,
  view,
  state,
  onSection,
  onView,
  onState,
  onOpenPortal,
}: {
  section: SectionId;
  view: string;
  state: string;
  onSection: (s: SectionId) => void;
  onView: (v: string) => void;
  onState: (s: string) => void;
  onOpenPortal: (slug: string) => void;
}) {
  const subs = SUBVIEWS[section];
  const active = subs.some(([k]) => k === view) ? view : subs[0][0];
  const meta = SECTIONS.find((s) => s.id === section)!;
  return (
    <section id="explore" className="scroll-mt-4 rounded-md border border-line bg-raised">
      <div className="sticky top-0 z-10 rounded-t-md border-b border-line bg-raised/95 px-4 pt-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div role="tablist" aria-label="Explore" className="flex flex-wrap gap-1">
            {SECTIONS.map((s) => (
              <button
                key={s.id}
                type="button"
                role="tab"
                aria-selected={section === s.id}
                onClick={() => onSection(s.id)}
                className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                  section === s.id ? 'bg-accent-dim text-ink font-semibold' : 'text-ink-dim hover:text-ink hover:bg-overlay'
                }`}
              >
                {s.label}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-2 text-xs text-ink-mute">
            Your state
            <select
              value={state}
              onChange={(e) => onState(e.target.value)}
              className="rounded-md border border-line bg-inset px-2 py-1 text-ink"
            >
              <option value="">Whole country</option>
              {US_STATES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
        </div>
        <p className="py-1.5 text-xs text-ink-mute">{meta.blurb}</p>
        {subs.length > 1 && (
          <div className="flex flex-wrap gap-1 pb-2">
            {subs.map(([k, label]) => (
              <button
                key={k}
                type="button"
                aria-pressed={active === k}
                onClick={() => onView(k)}
                className={`px-3 py-1 rounded-full border text-xs ${
                  active === k ? 'border-accent bg-accent-dim text-ink' : 'border-line text-ink-dim hover:text-ink'
                }`}
              >
                {label}
              </button>
            ))}
          </div>
        )}
      </div>
      <div className="p-4 text-xs">
        {active === 'feed' ? (
          <WatchFeed state={state} onOpenPortal={onOpenPortal} />
        ) : active === 'findings' ? (
          <Findings state={state} onOpenPortal={onOpenPortal} />
        ) : active === 'oversight' ? (
          <CivicOversight onOpenPortal={onOpenPortal} />
        ) : active === 'courts' ? (
          <CourtCases />
        ) : active === 'logs' ? (
          <SearchLogs state={state} onOpenPortal={onOpenPortal} />
        ) : active === 'departures' ? (
          <Departures onOpenPortal={onOpenPortal} />
        ) : active === 'awards' ? (
          <FederalMoney />
        ) : active === 'lobbying' ? (
          <Lobbying />
        ) : active === 'drones' ? (
          <DroneWaivers state={state} />
        ) : active === 'brokers' ? (
          <DataBrokers />
        ) : (
          <div className="max-w-2xl text-sm">
            <PrivacyPlan />
          </div>
        )}
      </div>
    </section>
  );
}

// ── The page ─────────────────────────────────────────────────────────────────

export function CivicWatch() {
  const townParam = (useSearchParams()?.get('town') ?? '').slice(0, 80);
  const { data: summary } = useSWR<Summary>('/civic/summary', fetcher);
  const { data: agencyData } = useSWR<{ agencies: AgencyRow[] }>('/civic/agencies?limit=10000', fetcher);
  const { data: oversight } = useSWR<{ laws: { agencies: { agency: string; state: string }[] }[]; covered_agencies: number }>(
    '/civic/oversight?',
    fetcher,
  );
  const { data: legal } = useSWR<{ counts: { severity: string; n: number }[] }>('/civic/legal?limit=1', fetcher);
  const mapEl = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Leaflet.Map | null>(null);
  const LRef = useRef<typeof Leaflet | null>(null);
  const deviceLayer = useRef<Leaflet.LayerGroup | null>(null);
  const agencyLayer = useRef<Leaflet.LayerGroup | null>(null);
  const [mapReady, setMapReady] = useState(false);
  const [layers, setLayers] = useState({ ALPR: true, gunshot_detector: true, agencies: true, oversight: true });
  const layersRef = useRef(layers);
  const [deviceNote, setDeviceNote] = useState<string>('');
  const [trail, setTrail] = useState<Story[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [section, setSection] = useState<SectionId>('feed');
  const [view, setView] = useState<string>('feed');
  const [state, setState] = useState<string>('');
  const [trailOn, setTrailOn] = useState(false);
  const [asking, setAsking] = useState(false);
  const [copied, setCopied] = useState(false);
  const [locating, setLocating] = useState(false);
  const trailLayer = useRef<Leaflet.LayerGroup | null>(null);
  const trailOnRef = useRef(false);
  const [more, setMore] = useState(false);

  const story = trail[trail.length - 1];
  const push = useCallback((s: Story, reset = false) => setTrail((t) => (reset ? [s] : [...t, s].slice(-6))), []);
  const portalSlug = story?.kind === 'agency' ? (story.card.portal_slug ?? null) : story?.kind === 'portal' ? story.slug : null;
  const { data: portal } = useSWR<VerdictPortal>(portalSlug ? `/civic/portal?slug=${encodeURIComponent(portalSlug)}` : null, fetcher);
  const verdict = story?.kind === 'agency' ? verdictLine(story.card, portal) : null;
  const headline = story?.kind === 'agency' ? verdictHeadline(story.card, portal) : null;
  const question = story?.kind === 'agency' ? councilQuestion(story.card, portal) : null;
  const nearVerdict =
    story?.kind === 'near'
      ? `${deviceWords(story.near.device_counts) || 'No mapped devices'} within ${story.near.radius_km} km, and ${plural(story.near.agencies.length, 'agency')} on record nearby.`
      : null;

  // The sidebar's Civic entries open a section by hash.
  useEffect(() => {
    const read = () => {
      const s = sectionFromHash(window.location.hash);
      if (!s) return;
      setSection(s);
      setView(SUBVIEWS[s][0][0]);
      // After the map and the hero have laid out; scrolling sooner lands short.
      window.setTimeout(() => document.getElementById('explore')?.scrollIntoView({ behavior: 'smooth', block: 'start' }), 450);
    };
    read();
    window.addEventListener('hashchange', read);
    return () => window.removeEventListener('hashchange', read);
  }, []);

  const explore = useCallback((s: SectionId, v?: string, st?: string) => {
    setSection(s);
    setView(v ?? SUBVIEWS[s][0][0]);
    if (st !== undefined) setState(st);
    history.replaceState(null, '', `#${s}`);
    window.dispatchEvent(new HashChangeEvent('hashchange'));
  }, []);

  const showMap = useCallback(() => document.getElementById('civic-map')?.scrollIntoView({ behavior: 'smooth', block: 'center' }), []);
  const soloed = !(layers.ALPR && layers.gunshot_detector && layers.agencies && layers.oversight);
  const showOversight = useCallback(() => {
    setLayers({ ALPR: false, gunshot_detector: false, agencies: false, oversight: true });
    showMap();
  }, [showMap]);
  const showAll = useCallback(() => setLayers({ ALPR: true, gunshot_detector: true, agencies: true, oversight: true }), []);

  const openAgency = useCallback(
    async (name: string, st: string, reset = false) => {
      setError(null);
      try {
        const card: ReportCard = await fetcher(`/civic/agency?name=${encodeURIComponent(name)}&state=${encodeURIComponent(st)}`);
        push({ kind: 'agency', card, label: card.agency }, reset);
        setTrailOn(false);
        setAsking(false);
        setCopied(false);
        setState(card.state);
        if (card.lat != null && card.lon != null) mapRef.current?.flyTo([card.lat, card.lon], Math.max(mapRef.current.getZoom(), 11));
      } catch {
        setError("We couldn't open that agency.");
      }
    },
    [push],
  );

  const openPortal = useCallback((slug: string, label = 'Flock page') => push({ kind: 'portal', slug, label }), [push]);

  const loadDevices = useCallback(async () => {
    const map = mapRef.current;
    const L = LRef.current;
    if (!map || !L || !deviceLayer.current) return;
    deviceLayer.current.clearLayers();
    if (map.getZoom() < DEVICE_MIN_ZOOM) {
      setDeviceNote('Zoom in to street level to see individual cameras.');
      return;
    }
    const b = map.getBounds();
    try {
      const res: { devices: Device[]; truncated: boolean } = await fetcher(
        `/civic/devices?bbox=${b.getSouth()},${b.getWest()},${b.getNorth()},${b.getEast()}`,
      );
      const shown = res.devices.filter((d) => layersRef.current[d.kind as 'ALPR' | 'gunshot_detector'] !== false);
      for (const d of shown) {
        L.circleMarker([d.lat, d.lon], { radius: 4, color: KIND_COLOR[d.kind] ?? '#94a3b8', weight: 1, fillOpacity: 0.85 })
          .bindTooltip(
            `${KIND_LABEL[d.kind] ?? d.kind}${d.manufacturer ? ` · ${d.manufacturer}` : ''}${d.operator ? ` · run by ${d.operator}` : ''}${d.direction ? ` · facing ${d.direction}` : ''}`,
          )
          .addTo(deviceLayer.current);
      }
      setDeviceNote(`${plural(shown.length, 'mapped device')} in view${res.truncated ? ' (the first 8,000)' : ''}.`);
    } catch {
      setDeviceNote("Couldn't load the cameras here.");
    }
  }, []);

  // The map, created once on the client.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const L = await import('leaflet');
      if (cancelled || !mapEl.current || mapRef.current) return;
      LRef.current = L;
      // The wheel scrolls the page until the reader clicks into the map: with
      // wheel zoom always on, scrolling past the map zoomed it out to the globe.
      const map = L.map(mapEl.current, {
        preferCanvas: true,
        worldCopyJump: true,
        zoomControl: false,
        scrollWheelZoom: false,
      }).setView([39.5, -98.35], 4);
      map.on('focus click', () => map.scrollWheelZoom.enable());
      map.on('blur mouseout', () => map.scrollWheelZoom.disable());
      L.control.zoom({ position: 'bottomright' }).addTo(map);
      // Tiles come through this deployment's gateway, which caches them: the
      // browser never tells a tile server where its reader is looking.
      L.tileLayer(`${TILE_BASE}{z}/{x}/{y}.png`, {
        maxZoom: 19,
        className: 'civic-tiles',
        attribution: '&copy; OpenStreetMap contributors · devices: OSM / DeFlock mappers · agencies: EFF Atlas of Surveillance',
      }).addTo(map);
      agencyLayer.current = L.layerGroup().addTo(map);
      deviceLayer.current = L.layerGroup().addTo(map);
      map.createPane('trail').style.zIndex = '450';
      trailLayer.current = L.layerGroup().addTo(map);
      map.on('moveend', () => void loadDevices());
      map.on('click', async (ev: Leaflet.LeafletMouseEvent) => {
        setError(null);
        try {
          const near: Near = await fetcher(`/civic/near?lat=${ev.latlng.lat.toFixed(5)}&lon=${ev.latlng.lng.toFixed(5)}&radius_km=5`);
          push({ kind: 'near', near, label: 'This spot' }, true);
        } catch {
          setError("We couldn't read what watches that spot.");
        }
      });
      mapRef.current = map;
      setMapReady(true);
      void loadDevices();
    })();
    return () => {
      cancelled = true;
      mapRef.current?.remove();
      mapRef.current = null;
    };
  }, [loadDevices, push]);

  const locate = useCallback(() => {
    if (!navigator.geolocation) {
      setError('This browser cannot share a location. Search for your town instead.');
      return;
    }
    setLocating(true);
    setError(null);
    navigator.geolocation.getCurrentPosition(
      async (pos) => {
        const { latitude, longitude } = pos.coords;
        try {
          const near: Near = await fetcher(`/civic/near?lat=${latitude.toFixed(3)}&lon=${longitude.toFixed(3)}&radius_km=5`);
          push({ kind: 'near', near, label: 'Near you' }, true);
          mapRef.current?.flyTo([latitude, longitude], 12);
        } catch {
          setError("We couldn't read what watches your area.");
        } finally {
          setLocating(false);
        }
      },
      () => {
        setLocating(false);
        setError('Your location was not shared. Search for your town instead.');
      },
      { timeout: 10000 },
    );
  }, [push]);

  // The data trail: arcs from the picked department to every state its plate data is shared with.
  useEffect(() => {
    const L = LRef.current;
    const layer = trailLayer.current;
    const map = mapRef.current;
    if (!mapReady || !L || !layer || !map) return;
    layer.clearLayers();
    const origin: [number, number] | null = story?.kind === 'agency' && story.card.lat != null && story.card.lon != null ? [story.card.lat, story.card.lon] : null;
    if (origin) {
      const dot = document.createElement('span');
      dot.className = 'watch-pulse';
      L.marker(origin, { icon: L.divIcon({ className: '', html: dot, iconSize: [16, 16], iconAnchor: [8, 8] }), interactive: false, keyboard: false }).addTo(layer);
    }
    if (!trailOn || !origin || !portal) return;
    const lines = trailLines(portal.shared_by_state);
    for (const l of lines) {
      L.polyline(arc(origin, l.to), { pane: 'trail', renderer: L.svg({ pane: 'trail' }), color: '#a5b4fc', weight: 1 + Math.min(5, Math.log2(l.count + 1)), opacity: 0.85 })
        .bindTooltip(`${plural(l.count, 'agency')} in ${l.state}`)
        .addTo(layer);
    }
    if (lines.length > 0) map.flyToBounds(L.latLngBounds([origin, ...lines.map((l) => l.to)]), { padding: [50, 50], maxZoom: 6 });
  }, [trailOn, portal, story, mapReady]);

  const covered = useMemo(() => {
    const s = new Set<string>();
    for (const l of oversight?.laws ?? []) for (const a of l.agencies) s.add(`${a.agency}|${a.state}`);
    return s;
  }, [oversight]);

  // Agencies, once they and the map are ready; redrawn when a layer toggles.
  useEffect(() => {
    layersRef.current = layers;
    trailOnRef.current = trailOn;
    const L = LRef.current;
    const layer = agencyLayer.current;
    if (!mapReady || !L || !layer) return;
    layer.clearLayers();
    for (const a of agencyData?.agencies ?? []) {
      if (a.lat == null || a.lon == null) continue;
      const watched = covered.has(`${a.agency}|${a.state}`);
      if (!layers.agencies && !(layers.oversight && watched)) continue;
      L.circleMarker([a.lat, a.lon], {
        radius: 3 + Math.min(5, a.intensity / 3),
        color: layers.oversight && watched ? '#34d399' : intensityColor(a.intensity),
        fillColor: intensityColor(a.intensity),
        weight: layers.oversight && watched ? 2.5 : 1,
        fillOpacity: trailOnRef.current ? 0.12 : 0.4,
      })
        .bindTooltip(`${a.agency} (${a.state}): ${a.technologies.join(', ')}${watched ? ' · local oversight law' : ''}`)
        .on('click', (ev: Leaflet.LeafletMouseEvent) => {
          LRef.current?.DomEvent.stopPropagation(ev);
          void openAgency(a.agency, a.state, true);
        })
        .addTo(layer);
    }
    void loadDevices();
  }, [agencyData, covered, layers, mapReady, openAgency, loadDevices, trailOn]);

  const t = summary?.totals;
  const conflicts = (legal?.counts ?? []).filter((c) => c.severity === 'conflict').reduce((a, c) => a + c.n, 0);
  const toggles: [keyof typeof layers, string, string][] = [
    ['ALPR', 'Plate readers', KIND_COLOR.ALPR],
    ['gunshot_detector', 'Gunshot detectors', KIND_COLOR.gunshot_detector],
    ['agencies', 'Police agencies', '#eab308'],
    ['oversight', 'Has an oversight law', '#34d399'],
  ];

  return (
    <div id="civic-top" className="space-y-4 pb-8">
      {/* Map tiles are dimmed to sit inside the dark interface, not glare out of it. */}
      {/* Dark tiles in the dark theme: the light basemap was the brightest thing
          on the page and competed with the dots. Inverting and turning the hue
          back keeps water blue-ish and land dark, with no second tile source. */}
      <style>{`.watch-pulse{display:block;width:16px;height:16px;border-radius:9999px;background:#a5b4fc;border:2px solid #fff;animation:watch-pulse 1.8s infinite}@keyframes watch-pulse{0%{box-shadow:0 0 0 0 rgba(165,180,252,.7)}70%{box-shadow:0 0 0 20px rgba(165,180,252,0)}100%{box-shadow:0 0 0 0 rgba(165,180,252,0)}}@media (prefers-reduced-motion:reduce){.watch-pulse{animation:none}}.civic-tiles{filter:invert(1) hue-rotate(180deg) brightness(.92) contrast(.88) saturate(.45)}:root[data-theme="light"] .civic-tiles{filter:saturate(.8)}.leaflet-container{background:var(--bg-inset)}`}</style>

      <section id="civic-map" aria-label="Watchlight map" className="relative isolate h-[82vh] min-h-[540px] overflow-hidden rounded-md border border-line">
        <div ref={mapEl} className="absolute inset-0" />

        {story && (
          <button
            type="button"
            onClick={() => setTrail([])}
            className="absolute left-3 top-3 z-[500] rounded-full border border-line bg-overlay/95 px-3 py-1.5 text-xs text-ink shadow-overlay md:hidden"
          >
            ← Search another place
          </button>
        )}
        <div className={`absolute inset-x-3 top-3 z-[500] max-w-xl space-y-3 rounded-lg border border-line bg-overlay/95 p-4 shadow-overlay ${story ? 'hidden md:block' : ''}`}>
          <p className="text-micro font-semibold uppercase tracking-[0.18em] text-accent">Watchlight</p>
          <h1 className="text-lead font-semibold leading-tight text-ink">{story ? 'Looking somewhere else?' : 'Where do you live?'}</h1>
          <PlaceFinder agencies={agencyData?.agencies ?? []} onPick={(a) => void openAgency(a.agency, a.state, true)} initial={townParam} />
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <button
              type="button"
              onClick={locate}
              disabled={locating}
              className="rounded-full border border-accent px-3 py-1 text-xs text-ink hover:bg-accent-dim disabled:opacity-60"
            >
              {locating ? 'Finding you…' : 'Use my location'}
            </button>
            <span className="text-micro text-ink-mute">or tap anywhere on the map. Your location is used once to look up what is nearby.</span>
          </div>
          {!story && (
            <p className="text-sm leading-relaxed text-ink">
              Volunteers have mapped <Fig value={t?.devices} onClick={showMap} hint="Look at the map" /> cameras and sensors run by{' '}
              <Fig value={t?.agencies} onClick={showMap} hint="Look at the map" /> agencies.{' '}
              <Fig value={conflicts} tone="text-negative" onClick={() => explore('accountability', 'findings')} hint="See the possible violations" />{' '}
              may be breaking their own rules, and only{' '}
              <Fig value={oversight?.covered_agencies} tone="text-positive" onClick={showOversight} hint="Show only agencies with an oversight law" />{' '}
              answer to a local oversight law.
              {soloed && (
                <>
                  {' '}
                  <button type="button" onClick={showAll} className="text-sm text-accent underline underline-offset-2">
                    Show everything again
                  </button>
                </>
              )}
            </p>
          )}
          {error && <p className="text-xs text-negative">{error}</p>}
        </div>

        {(verdict || nearVerdict) && (
          <div aria-live="polite" className="absolute inset-x-3 bottom-3 z-[500] space-y-2.5 rounded-lg border border-line bg-overlay/95 p-4 shadow-overlay md:left-auto md:w-[26rem]">
            {story?.kind === 'agency' && (
              <>
                <div className="flex items-start justify-between gap-2">
                  <h2 className="text-head font-semibold leading-tight text-ink">{story.card.agency}</h2>
                  <MakeMyTown agency={story.card.agency} state={story.card.state} />
                </div>
                <div className="flex items-center gap-2">
                  <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-inset">
                    <div className="h-full rounded-full" style={{ width: `${Math.min(100, (story.card.intensity / Math.max(1, story.card.max_intensity)) * 100)}%`, background: intensityColor(story.card.intensity) }} />
                  </div>
                  <span className="text-micro text-ink-dim">{intensityWord(story.card.intensity)} surveillance</span>
                </div>
              </>
            )}
            {headline && <p className="text-lg font-semibold leading-snug text-ink">{headline}</p>}
            <p className={`text-sm leading-relaxed ${headline ? 'text-ink-dim' : 'text-ink'} ${headline && !more ? 'hidden md:block' : ''}`}>{verdict ?? nearVerdict}</p>
            {headline && (
              <button type="button" onClick={() => setMore((v) => !v)} className="text-micro text-accent underline underline-offset-2 md:hidden">
                {more ? 'Less' : 'More about this place'}
              </button>
            )}
            {story?.kind === 'agency' && (
              <div className="flex flex-wrap gap-1.5">
                {trailLines(portal?.shared_by_state).length > 0 && (
                  <button
                    type="button"
                    aria-pressed={trailOn}
                    onClick={() => setTrailOn((v) => !v)}
                    className={`rounded-full border px-3 py-1 text-xs ${trailOn ? 'border-accent bg-accent-dim text-ink' : 'border-line text-ink-dim hover:border-accent hover:text-ink'}`}
                  >
                    {trailOn ? 'Hide the data trail' : 'Show where the data goes'}
                  </button>
                )}
                <button
                  type="button"
                  aria-pressed={asking}
                  onClick={() => setAsking((v) => !v)}
                  className={`rounded-full border px-3 py-1 text-xs ${asking ? 'border-accent bg-accent-dim text-ink' : 'border-line text-ink-dim hover:border-accent hover:text-ink'}`}
                >
                  Ask your council
                </button>
                <button type="button" onClick={() => document.getElementById('civic-deeper')?.scrollIntoView({ behavior: 'smooth' })} className="rounded-full border border-line px-3 py-1 text-xs text-ink-dim hover:border-accent hover:text-ink">
                  The full picture ↓
                </button>
              </div>
            )}
            {story?.kind === 'agency' && asking && question && (
              <div className="space-y-1.5">
                <textarea readOnly rows={7} value={question} className="w-full rounded-md border border-line bg-inset p-2 text-xs text-ink-dim" aria-label="A question for your council, drafted for you" />
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      navigator.clipboard
                        ?.writeText(question)
                        .then(() => setCopied(true))
                        .catch(() => setCopied(false));
                    }}
                    className="rounded-full border border-accent px-3 py-1 text-xs text-ink hover:bg-accent-dim"
                  >
                    {copied ? 'Copied' : 'Copy the question'}
                  </button>
                  <span className="text-micro text-ink-mute">A draft only. Nothing is sent: you send it.</span>
                </div>
              </div>
            )}
          </div>
        )}

        <details className={`absolute bottom-3 left-3 z-[500] rounded-md border border-line bg-overlay/95 text-xs shadow-overlay ${story ? 'hidden md:block' : ''}`}>
          <summary className="cursor-pointer select-none px-2.5 py-1.5 text-ink-dim hover:text-ink">Layers</summary>
          <div className="space-y-1.5 border-t border-line p-2.5">
            {toggles.map(([k, label, color]) => (
              <label key={k} className="flex cursor-pointer items-center gap-2 text-ink-dim hover:text-ink">
                <input type="checkbox" checked={layers[k]} onChange={(e) => setLayers((l) => ({ ...l, [k]: e.target.checked }))} className="accent-[var(--accent)]" />
                <span className="h-2.5 w-2.5 rounded-full" style={{ background: color }} />
                {label}
              </label>
            ))}
            <div className="text-micro text-ink-mute">Bigger, redder dots: more invasive tools.</div>
            {deviceNote && <div className="text-micro text-ink-mute">{deviceNote}</div>}
          </div>
        </details>
      </section>

      <section id="civic-deeper" aria-label="The full picture" className="rounded-md border border-line bg-raised p-4" aria-live="polite">
        {trail.length > 0 && (
          <nav aria-label="Trail" className="mb-3 flex flex-wrap items-center gap-1 text-micro text-ink-mute">
            <button type="button" onClick={() => setTrail([])} className="hover:text-accent">
              Start
            </button>
            {trail.map((s, i) => (
              <React.Fragment key={i}>
                <span>/</span>
                <button type="button" onClick={() => setTrail((tr) => tr.slice(0, i + 1))} className={`max-w-[10rem] truncate ${i === trail.length - 1 ? 'text-ink' : 'hover:text-accent'}`}>
                  {s.label}
                </button>
              </React.Fragment>
            ))}
          </nav>
        )}
        {!story ? (
          <Welcome summary={summary} onExplore={(s) => explore(s)} />
        ) : story.kind === 'near' ? (
          <NearStory near={story.near} onAgency={(n, st) => void openAgency(n, st)} />
        ) : story.kind === 'agency' ? (
          <AgencyStory card={story.card} onOpenPortal={openPortal} onExplore={explore} />
        ) : (
          <div className="space-y-3 text-xs">
            <PortalPanel slug={story.slug} />
          </div>
        )}
      </section>

      <Explore
        section={section}
        view={view}
        state={state}
        onSection={(s) => explore(s)}
        onView={setView}
        onState={setState}
        onOpenPortal={(slug) => {
          openPortal(slug);
          document.getElementById('civic-top')?.scrollIntoView({ behavior: 'smooth' });
        }}
      />
    </div>
  );
}

export default CivicWatch;
