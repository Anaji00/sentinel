'use client';

/**
 * Is it legal here? (/civic/legal, /civic/portal, /civic/laws;
 * shared/utils/civic_law.py). Each agency's published Flock practice --
 * retention, who it shares plate data with -- checked against its state's
 * licence-plate-reader law, plus ICE 287(g) agreements. Findings are questions
 * to ask with the evidence and citation attached, not legal conclusions.
 */

import React, { useState } from 'react';
import { Input } from './ui/Input';
import { Button } from './ui/Button';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { oversightLine, type OversightLaw } from './CivicOversight';
import { ABSENT, formatCompact, formatNumber } from '../lib/format';
import { TickerChip } from './ui/TickerChip';
import { LoadingLines } from './ui/Skeleton';

export interface Finding {
  slug?: string;
  code: string;
  severity: 'conflict' | 'check';
  state?: string;
  agency?: string;
  title: string;
  detail: string;
  citation: string | null;
  source: string | null;
  evidence: Record<string, unknown>;
  portal_url?: string | null;
  data_last_updated?: string | null;
}

interface StateLaw {
  state: string;
  citation: string;
  retention_text: string | null;
  immigration_use_banned: boolean;
  immigration_sharing_banned: boolean;
  out_of_state_sharing_banned: boolean;
  declaration_required: boolean;
  federal_sharing_banned: boolean;
  note: string | null;
  source: string;
}

/** A state's plate-reader law as the checks apply it, in one line. */
export function lawLine(law: StateLaw | undefined, state: string): string {
  if (!law)
    return `${state} has no statute that expressly addresses licence-plate readers (NCSL, Sept 2026).`;
  const parts: string[] = [];
  if (law.retention_text) parts.push(`routine data kept at most ${law.retention_text}`);
  if (law.immigration_use_banned) parts.push('no use for immigration enforcement');
  else if (law.immigration_sharing_banned) parts.push('no sharing for immigration enforcement');
  if (law.out_of_state_sharing_banned) parts.push('sharing outside the state restricted');
  if (law.declaration_required) parts.push('out-of-state recipients must sign a declaration');
  if (law.federal_sharing_banned) parts.push('federal sharing restricted');
  return `${state} (${law.citation}): ${parts.length ? parts.join('; ') : 'use and policy requirements'}.`;
}

interface LegalData {
  findings: Finding[];
  counts: { code: string; severity: string; state: string; n: number }[];
  totals: {
    portals?: number;
    cameras?: number;
    vehicles?: number;
    over_30_days?: number;
    ice_287g_agencies?: number;
    ice_287g_agreements?: number;
  };
}

export interface Portal {
  slug: string;
  portal_url: string | null;
  city: string | null;
  county: string | null;
  state: string;
  agency_type: string | null;
  total_cameras: number | null;
  total_searches: number | null;
  data_retention: number | null;
  vehicles_captured: number | null;
  hotlist_hit_rate: number | null;
  shared_with_count: number | null;
  received_from_count: number | null;
  prohibited_uses: string | null;
  public_search_audit: boolean | null;
  data_last_updated: string | null;
  shared_by_state: Record<string, number>;
  out_of_state_share: number;
  findings: Finding[];
  state_law: string | null;
  oversight?: OversightLaw | null;
  oversight_campaign?: string;
  attribution: string;
  audit?: AuditStats | null;
}

export interface AuditStats {
  days: number;
  searches: number;
  immigration_hits: number;
  immigration_terms: Record<string, number>;
  first_amendment_hits: number;
  first_amendment_terms: Record<string, number>;
  wide_searches: number;
  no_reason: number;
}

/** One agency's released search log, in a sentence of counts. */
export function auditSummary(a: AuditStats): string {
  const pct = (n: number) =>
    a.searches ? `${formatNumber((n / a.searches) * 100, { decimals: 1 })}%` : '0%';
  const parts = [
    `${formatNumber(a.searches, { decimals: 0 })} searches in the last ${a.days} days`,
  ];
  if (a.wide_searches) parts.push(`${pct(a.wide_searches)} reached 100+ agencies' networks`);
  if (a.no_reason) parts.push(`${pct(a.no_reason)} gave no reason`);
  if (a.immigration_hits) parts.push(`${a.immigration_hits} cite immigration terms`);
  if (a.first_amendment_hits)
    parts.push(`${a.first_amendment_hits} cite protests, journalists or similar`);
  return `${parts.join('; ')}.`;
}

const terms = (t: Record<string, number>) =>
  Object.entries(t)
    .sort((x, y) => y[1] - x[1])
    .map(([k, n]) => `${k} ${n}`)
    .join(', ');

/** Plain words for a finding's weight. */
export function severityLabel(sev: string): string {
  return sev === 'conflict' ? 'Possible violation' : 'Ask for records';
}

/** Where an agency's plate data goes, in one sentence. */
export function sharingSummary(
  p: Pick<Portal, 'state' | 'shared_by_state' | 'shared_with_count'>,
): string {
  const total = Object.values(p.shared_by_state).reduce((a, b) => a + b, 0);
  if (!total) return 'Shares its plate data with no other agency.';
  const inState = p.shared_by_state[p.state] ?? 0;
  const federal = p.shared_by_state.federal ?? 0;
  const otherStates = Object.keys(p.shared_by_state).filter(
    (k) => k !== p.state && k !== 'federal' && k !== 'unknown',
  );
  const outside = total - inState - federal - (p.shared_by_state.unknown ?? 0);
  const parts = [`Shares its plate data with ${formatNumber(total, { decimals: 0 })} agencies`];
  if (outside > 0)
    parts.push(
      `${formatNumber(outside, { decimals: 0 })} of them in ${otherStates.length} other state${otherStates.length === 1 ? '' : 's'}`,
    );
  if (federal > 0) parts.push(`${federal} federal`);
  return `${parts.join(': ')}.`;
}

const SEV_TONE: Record<string, string> = {
  conflict: 'border-negative/30 text-negative',
  check: 'border-caution/30 text-caution',
};

function FindingItem({ f, onOpen }: { f: Finding; onOpen?: (slug: string) => void }) {
  return (
    <li className="border-t border-line/60 pt-1">
      <div className="flex flex-wrap items-baseline gap-2">
        <span
          className={`px-1.5 rounded border text-micro font-bold ${SEV_TONE[f.severity] ?? ''}`}
        >
          {severityLabel(f.severity)}
        </span>
        {f.agency && f.slug && onOpen ? (
          <button
            type="button"
            className="text-ink hover:text-accent font-bold"
            onClick={() => onOpen(f.slug!)}
          >
            {f.agency}
            {f.state ? `, ${f.state}` : ''}
          </button>
        ) : null}
        <span className="text-ink">{f.title}</span>
      </div>
      <div className="text-micro text-ink-dim">{f.detail}</div>
      <div className="text-micro text-ink-mute">
        {f.citation}
        {f.source && (
          <>
            {' · '}
            <a
              href={f.source}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline"
            >
              law
            </a>
          </>
        )}
        {f.portal_url && (
          <>
            {' · '}
            <a
              href={f.portal_url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline"
            >
              agency&apos;s portal
            </a>
          </>
        )}
        {f.data_last_updated ? ` · portal data ${f.data_last_updated.slice(0, 10)}` : ''}
      </div>
    </li>
  );
}

interface RecordsRequest {
  subject: string;
  body: string;
  asks: string[];
  file_with: string;
  note: string;
}

/** A records-request letter the reader sends themselves (Sentinel never sends it). */
function RecordsRequestDraft({ slug }: { slug: string }) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const { data } = useSWR<RecordsRequest>(
    open ? `/civic/records-request?slug=${encodeURIComponent(slug)}` : null,
    fetcher,
  );
  if (!open) {
    return (
      <Button variant="link"
        type="button" className="text-micro"
        onClick={() => setOpen(true)}
      >
        Draft a public-records request to this agency
      </Button>
    );
  }
  if (!data) return <p className="text-micro text-ink-mute">Drafting…</p>;
  return (
    <div className="space-y-1">
      <div className="text-ink">{data.subject}</div>
      <textarea
        readOnly
        value={data.body}
        className="w-full h-48 bg-base border border-line rounded p-2 font-mono text-micro"
      />
      <div className="flex flex-wrap gap-3 text-micro">
        <Button variant="link"
          type="button"
          onClick={async () => {
            try {
              await navigator.clipboard.writeText(`${data.subject}\n\n${data.body}`);
              setCopied(true);
            } catch {
              setCopied(false);
            }
          }}
        >
          {copied ? 'Copied' : 'Copy letter'}
        </Button>
        <a
          href={data.file_with}
          target="_blank"
          rel="noopener noreferrer"
          className="text-accent hover:underline"
        >
          File and track it on MuckRock
        </a>
      </div>
      <p className="text-micro text-ink-mute">{data.note}</p>
    </div>
  );
}

export function PortalPanel({ slug }: { slug: string }) {
  const { data: p, error } = useSWR<Portal>(
    `/civic/portal?slug=${encodeURIComponent(slug)}`,
    fetcher,
  );
  if (error)
    return <p className="text-ink-mute">This agency&apos;s portal data could not be read.</p>;
  if (!p) return <LoadingLines />;
  const top = Object.entries(p.shared_by_state).slice(0, 10);
  return (
    <div className="space-y-2">
      <div>
        <div className="text-ink font-bold">
          {[p.city || p.county, p.agency_type].filter(Boolean).join(' ')} ({p.state})
        </div>
        <div className="text-micro text-ink-mute">Published Flock practice · {p.attribution}</div>
      </div>
      <div className="grid grid-cols-2 gap-2 font-mono text-micro">
        {[
          ['Cameras', p.total_cameras],
          ['Keeps data', p.data_retention != null ? `${p.data_retention} days` : null],
          ['Vehicles captured', p.vehicles_captured],
          ['Searches', p.total_searches],
          [
            'Hotlist hit rate',
            p.hotlist_hit_rate != null
              ? `${formatNumber(p.hotlist_hit_rate, { decimals: 2 })}%`
              : null,
          ],
          [
            'Search audit public',
            p.public_search_audit == null ? null : p.public_search_audit ? 'yes' : 'no',
          ],
        ].map(([k, v]) => (
          <div key={String(k)} className="rounded border border-line px-2 py-1">
            <div className="text-ink-mute">{k}</div>
            <div className="text-ink">
              {v == null ? ABSENT : typeof v === 'number' ? formatNumber(v, { decimals: 0 }) : v}
            </div>
          </div>
        ))}
      </div>
      <div className="text-micro text-ink-dim">{sharingSummary(p)}</div>
      {p.audit && p.audit.searches > 0 && (
        <div className="text-micro text-ink-dim">
          <span className="text-ink">Its released search log: </span>
          {auditSummary(p.audit)}
          {p.audit.immigration_hits > 0 && (
            <div>Immigration terms: {terms(p.audit.immigration_terms)}</div>
          )}
          {p.audit.first_amendment_hits > 0 && (
            <div>Protest and press terms: {terms(p.audit.first_amendment_terms)}</div>
          )}
        </div>
      )}
      {top.length > 0 && (
        <div className="flex flex-wrap gap-1 font-mono text-micro">
          {top.map(([k, n]) => (
            <span
              key={k}
              className={`px-1.5 rounded border ${k === p.state ? 'border-line text-ink-dim' : 'border-caution/30 text-caution'}`}
            >
              {k} {n}
            </span>
          ))}
        </div>
      )}
      {p.findings.length > 0 ? (
        <ul className="space-y-1">
          {p.findings.map((f) => (
            <FindingItem key={f.code} f={f} />
          ))}
        </ul>
      ) : (
        <p className="text-micro text-ink-mute">
          No conflict with {p.state} law found in the published data
          {p.state_law ? ` (${p.state_law})` : ' (no state ALPR statute)'}.
        </p>
      )}
      <div className="text-micro text-ink-dim">
        {oversightLine(p.oversight)}{' '}
        {p.oversight ? (
          <a
            href={p.oversight.law_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-accent hover:underline"
          >
            Read the law
          </a>
        ) : (
          p.oversight_campaign && (
            <a
              href={p.oversight_campaign}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline"
            >
              How to ask for one
            </a>
          )
        )}
      </div>
      {p.prohibited_uses && (
        <div className="text-micro text-ink-mute">Stated prohibited uses: {p.prohibited_uses}</div>
      )}
      {p.portal_url && (
        <a
          href={p.portal_url}
          target="_blank"
          rel="noopener noreferrer"
          className="text-micro text-accent hover:underline"
        >
          Open the agency&apos;s Flock transparency portal
        </a>
      )}
      <RecordsRequestDraft slug={p.slug} />
    </div>
  );
}

interface AuditRow extends AuditStats {
  slug: string;
  city: string | null;
  county: string | null;
  state: string;
  agency_type: string | null;
}

function SearchLogs({
  state,
  onOpenPortal,
}: {
  state: string;
  onOpenPortal: (slug: string) => void;
}) {
  const [sort, setSort] = useState<'immigration' | 'first_amendment' | 'wide'>('immigration');
  const q = new URLSearchParams({ sort, limit: '50' });
  if (state) q.set('state', state);
  const { data } = useSWR<{
    agencies: AuditRow[];
    keywords: Record<string, string[]>;
    caveat: string;
  }>(`/civic/audits?${q.toString()}`, fetcher);
  const value = (r: AuditRow) =>
    sort === 'wide'
      ? r.wide_searches
      : sort === 'first_amendment'
        ? r.first_amendment_hits
        : r.immigration_hits;
  return (
    <div className="space-y-1">
      <div className="flex flex-wrap items-center gap-2 text-micro text-ink-mute">
        Released search logs, ranked by{' '}
        <select
          value={sort}
          onChange={(e) => setSort(e.target.value as typeof sort)}
          className="bg-base border border-line rounded px-1"
        >
          <option value="immigration">searches citing immigration</option>
          <option value="first_amendment">searches citing protests or press</option>
          <option value="wide">nationwide lookups</option>
        </select>
      </div>
      {!data ? (
        <LoadingLines />
      ) : data.agencies.filter((r) => value(r) > 0).length === 0 ? (
        <p className="text-ink-mute">No released log matches this yet.</p>
      ) : (
        <ul className="space-y-0.5 max-h-72 overflow-y-auto">
          {data.agencies
            .filter((r) => value(r) > 0)
            .map((r) => (
              <li key={r.slug} className="flex justify-between gap-2">
                <button
                  type="button"
                  className="text-left text-ink hover:text-accent"
                  onClick={() => onOpenPortal(r.slug)}
                >
                  {[r.city || r.county, r.agency_type].filter(Boolean).join(' ')}, {r.state}
                </button>
                <span className="font-mono text-micro text-ink-dim">
                  {formatNumber(value(r), { decimals: 0 })} of{' '}
                  {formatNumber(r.searches, { decimals: 0 })}
                </span>
              </li>
            ))}
        </ul>
      )}
      {data && (
        <p className="text-micro text-ink-mute">
          Terms counted: immigration &mdash; {data.keywords.immigration?.join(', ')}; protest and
          press &mdash; {data.keywords.first_amendment?.join(', ')}. {data.caveat}
        </p>
      )}
    </div>
  );
}

interface Departed {
  slug: string;
  city: string | null;
  county: string | null;
  state: string;
  agency_type: string | null;
  total_cameras: number | null;
  removed_at: string;
}

function Departures({ onOpenPortal }: { onOpenPortal: (slug: string) => void }) {
  const { data } = useSWR<{ departed: Departed[]; caveat: string }>('/civic/departures', fetcher);
  if (!data) return <LoadingLines />;
  return (
    <div className="space-y-1">
      {data.departed.length === 0 ? (
        <p className="text-ink-mute">
          No portal has disappeared since tracking began. Each weekly pass compares the list; a
          portal that stops being published is shown here.
        </p>
      ) : (
        <ul className="space-y-0.5 max-h-72 overflow-y-auto">
          {data.departed.map((d) => (
            <li key={d.slug} className="flex justify-between gap-2">
              <button
                type="button"
                className="text-left text-ink hover:text-accent"
                onClick={() => onOpenPortal(d.slug)}
              >
                {[d.city || d.county, d.agency_type].filter(Boolean).join(' ')}, {d.state}
              </button>
              <span className="text-micro text-ink-mute">
                gone since {d.removed_at.slice(0, 10)}
                {d.total_cameras ? ` · ${d.total_cameras} cameras` : ''}
              </span>
            </li>
          ))}
        </ul>
      )}
      <p className="text-micro text-ink-mute">{data.caveat}</p>
    </div>
  );
}

interface Waiver {
  applicant: string;
  city: string | null;
  state: string;
  category: string | null;
  status: string | null;
  received: string | null;
  start_date: string | null;
}

function DroneWaivers({ state }: { state: string }) {
  const { data } = useSWR<{
    waivers: Waiver[];
    by_state: { state: string; n: number }[];
    source: string;
  }>(`/civic/drones?limit=500${state ? `&state=${state}` : ''}`, fetcher);
  if (!data) return <LoadingLines />;
  const total = data.by_state.reduce((a, r) => a + r.n, 0);
  return (
    <div className="space-y-1">
      <p className="text-micro text-ink-dim">
        {formatNumber(total, { decimals: 0 })} agencies hold FAA waivers to fly drones to 911 calls
        (drone-as-first-responder). Most waivered:{' '}
        {data.by_state
          .slice(0, 6)
          .map((r) => `${r.state} ${r.n}`)
          .join(', ')}
        . A waiver permits a program; it does not show one is flying.{' '}
        <a
          href={data.source}
          target="_blank"
          rel="noopener noreferrer"
          className="text-accent hover:underline"
        >
          EFF&apos;s FOIA release
        </a>
      </p>
      <ul className="space-y-0.5 max-h-72 overflow-y-auto">
        {data.waivers.map((w) => (
          <li key={`${w.applicant}-${w.state}`} className="flex justify-between gap-2">
            <span className="text-ink">
              {w.applicant}
              <span className="text-ink-mute">
                {' '}
                · {[w.city, w.state].filter(Boolean).join(', ')}
                {w.category ? ` · ${w.category}` : ''}
              </span>
            </span>
            <span className="text-micro text-ink-mute">
              {w.status ?? ''}
              {w.received ? ` ${w.received}` : ''}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

interface MoneyTotal {
  kind: string;
  matched: string;
  awards: number;
  total: number | null;
}

interface Award {
  award_id: string;
  kind: string;
  matched: string;
  recipient: string | null;
  amount: number | null;
  agency: string | null;
  sub_agency: string | null;
  start_date: string | null;
  description: string | null;
}

const dollars = (v: number | null | undefined) =>
  v == null
    ? ABSENT
    : v >= 1e6
      ? `$${formatNumber(v / 1e6, { decimals: 1 })}M`
      : `$${formatNumber(v, { decimals: 0 })}`;

interface LobbyVendor {
  vendor: string;
  total: number;
  years: Record<string, number>;
}

interface LobbyFiling {
  vendor: string;
  registrant: string | null;
  year: number | null;
  period: string | null;
  amount: number | null;
  issues: string[];
  entities: string[];
  url: string | null;
}

function Lobbying() {
  const [vendor, setVendor] = useState<string>('');
  const { data } = useSWR<{ by_vendor: LobbyVendor[]; filings: LobbyFiling[]; caveat: string }>(
    `/civic/lobbying?limit=40${vendor ? `&vendor=${encodeURIComponent(vendor)}` : ''}`,
    fetcher,
  );
  if (!data || !data.by_vendor.length) return null;
  return (
    <div className="space-y-1">
      <div className="text-micro text-ink-mute">Federal lobbying by surveillance vendors</div>
      <ul className="font-mono text-micro">
        {data.by_vendor.map((v) => (
          <li key={v.vendor}>
            <button
              type="button"
              className={`w-full flex justify-between gap-2 hover:text-accent ${vendor === v.vendor ? 'text-accent' : ''}`}
              onClick={() => setVendor(vendor === v.vendor ? '' : v.vendor)}
            >
              <span className="text-ink-dim">{v.vendor}</span>
              <span>
                {dollars(v.total)} ·{' '}
                {Object.entries(v.years)
                  .slice(-4)
                  .map(([y, t]) => `${y} ${dollars(t)}`)
                  .join(', ')}
              </span>
            </button>
          </li>
        ))}
      </ul>
      <ul className="space-y-0.5 max-h-48 overflow-y-auto text-micro">
        {data.filings.map((f, n) => (
          <li key={`${f.url ?? n}`} className="text-ink-mute">
            <span className="text-ink-dim">{f.vendor}</span>
            <TickerChip vendor={f.vendor} /> · {f.year} {f.period?.split(' (')[0]} ·{' '}
            {f.registrant} · {dollars(f.amount)} · {f.issues.join(', ')}
            {f.entities.length ? ` · lobbied ${f.entities.slice(0, 4).join(', ')}` : ''}
            {f.url && (
              <>
                {' '}
                <a
                  href={f.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-accent hover:underline"
                >
                  filing
                </a>
              </>
            )}
          </li>
        ))}
      </ul>
      <p className="text-micro text-ink-mute">{data.caveat}</p>
    </div>
  );
}

interface MoneyRow {
  key: string;
  label: string;
  total: number;
  n: number;
  unit: string;
}

/** A ranked list of money as bars: the biggest payee or payer reads first. */
function MoneyBars({
  title,
  rows,
  active,
  onPick,
}: {
  title: string;
  rows: MoneyRow[];
  active?: string;
  onPick?: (key: string) => void;
}) {
  const max = Math.max(1, ...rows.map((r) => r.total));
  return (
    <div className="space-y-1.5">
      <h4 className="text-xs font-semibold text-ink">{title}</h4>
      <ul className="space-y-1.5">
        {rows.map((r) => {
          const body = (
            <>
              <div className="flex items-baseline justify-between gap-2 text-micro">
                <span className={active === r.key ? 'text-civic' : 'text-ink-dim'}>{r.label}</span>
                <span className="shrink-0 font-mono text-ink">
                  {dollars(r.total)}{' '}
                  <span className="text-ink-mute">
                    · {formatNumber(r.n, { decimals: 0 })} {r.unit}
                  </span>
                </span>
              </div>
              <div className="h-1.5 rounded-full bg-inset overflow-hidden">
                <div
                  className="h-full rounded-full bg-civic/70"
                  style={{ width: `${Math.max(2, (r.total / max) * 100)}%` }}
                />
              </div>
            </>
          );
          return (
            <li key={r.key}>
              {onPick ? (
                <button
                  type="button"
                  aria-pressed={active === r.key}
                  className="w-full text-left space-y-0.5 hover:opacity-90"
                  onClick={() => onPick(active === r.key ? '' : r.key)}
                >
                  {body}
                </button>
              ) : (
                <div className="space-y-0.5">{body}</div>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function FederalMoney() {
  const [matched, setMatched] = useState<string>('');
  const { data } = useSWR<{
    totals: MoneyTotal[];
    agencies: { agency: string; sub_agency: string; awards: number; total: number }[];
    awards: Award[];
    caveat: string;
  }>(`/civic/money?limit=50${matched ? `&matched=${encodeURIComponent(matched)}` : ''}`, fetcher);
  if (!data) return <LoadingLines />;
  if (!data.totals.length)
    return <p className="text-ink-mute">Federal award data is being collected.</p>;
  const contracts = data.totals.filter((t) => t.kind === 'contract');
  const grants = data.totals.filter((t) => t.kind === 'grant');
  const contractTotal = contracts.reduce((n, t) => n + (t.total ?? 0), 0);
  const contractCount = contracts.reduce((n, t) => n + t.awards, 0);
  // Who is paying: the federal agencies behind the awards, largest first.
  const payers = [...(data.agencies ?? [])].sort((a, b) => b.total - a.total).slice(0, 8);
  return (
    <div className="space-y-4">
      <p className="text-sm text-ink">
        Federal agencies have paid surveillance companies{' '}
        <span className="font-semibold text-civic">${formatCompact(contractTotal, { decimals: 1 })}</span> across{' '}
        {formatNumber(contractCount, { decimals: 0 })} contracts. Pick a company to see what the
        money bought.
      </p>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <MoneyBars
          title="Who gets paid"
          rows={contracts.map((t) => ({
            key: t.matched,
            label: t.matched,
            total: t.total ?? 0,
            n: t.awards,
            unit: 'contracts',
          }))}
          active={matched}
          onPick={setMatched}
        />
        <MoneyBars
          title="Who pays"
          rows={payers.map((a) => ({
            key: `${a.agency}|${a.sub_agency}`,
            label:
              a.sub_agency && a.sub_agency !== a.agency
                ? `${a.sub_agency} (${a.agency})`
                : a.agency,
            total: a.total,
            n: a.awards,
            unit: 'awards',
          }))}
        />
        <MoneyBars
          title="Grants to local police, by what they buy"
          rows={grants.map((t) => ({
            key: t.matched,
            label: t.matched,
            total: t.total ?? 0,
            n: t.awards,
            unit: 'grants',
          }))}
          active={matched}
          onPick={setMatched}
        />
      </div>
      <div className="text-micro text-ink-mute">
        {matched ? (
          <>
            Largest awards for <span className="text-ink">{matched}</span>
            <TickerChip vendor={matched} />{' '}
            <button type="button" className="text-accent" onClick={() => setMatched('')}>
              (show all)
            </button>
          </>
        ) : (
          'Largest awards'
        )}
      </div>
      <ul className="space-y-1 max-h-72 overflow-y-auto">
        {data.awards.map((a) => (
          <li key={a.award_id} className="border-t border-line/60 pt-1">
            <div className="flex justify-between gap-2">
              <span className="text-ink">{a.recipient ?? ABSENT}</span>
              <span className="font-mono">{dollars(a.amount)}</span>
            </div>
            <div className="text-micro text-ink-mute">
              {[a.agency, a.sub_agency, a.start_date].filter(Boolean).join(' · ')}
              {a.description ? ` — ${a.description}` : ''}
            </div>
          </li>
        ))}
      </ul>
      <p className="text-micro text-ink-mute">{data.caveat}</p>
    </div>
  );
}

interface Broker {
  name: string;
  dba: string | null;
  website: string | null;
  privacy_url: string | null;
  city: string | null;
  state: string | null;
  flags: Record<string, boolean | null>;
  registry?: string;
}

/** Which state registries a broker is on, in words. */
export const REGISTRY_LABEL: Record<string, string> = {
  'CA,OR': 'California and Oregon',
  CA: 'California only',
  OR: 'Oregon only',
};

interface BrokerCount {
  flag: string;
  label: string;
  yes: number;
  answered: number;
}

/** A broker's declarations as short phrases, worst first. */
export function brokerFlags(
  flags: Record<string, boolean | null>,
  labels: Record<string, string>,
): string[] {
  const order = [
    'sold_to_law_enforcement',
    'sold_to_federal_government',
    'sold_to_foreign_actor',
    'precise_geolocation',
    'immigration_status',
    'biometrics',
    'reproductive_health',
    'sexual_orientation',
    'minors',
  ];
  return order.filter((k) => flags[k] === true).map((k) => labels[k] ?? k);
}

function DataBrokers() {
  const [flag, setFlag] = useState<string>('sold_to_law_enforcement');
  const [q, setQ] = useState<string>('');
  const [registry, setRegistry] = useState<string>('');
  const params = new URLSearchParams({ limit: '300' });
  if (flag) params.set('flag', flag);
  if (q) params.set('q', q);
  if (registry) params.set('registry', registry);
  const { data } = useSWR<{
    brokers: Broker[];
    counts: BrokerCount[];
    registries?: { registry: string; brokers: number }[];
    source: string;
    oregon_source?: string;
    note: string;
  }>(`/civic/brokers?${params.toString()}`, fetcher);
  if (!data) return <LoadingLines />;
  const labels = Object.fromEntries(data.counts.map((c) => [c.flag, c.label]));
  return (
    <div className="space-y-2">
      <p className="text-micro text-ink-dim">
        {data.note}{' '}
        <a
          href={data.source}
          target="_blank"
          rel="noopener noreferrer"
          className="text-accent hover:underline"
        >
          California&apos;s registry
        </a>
        {data.oregon_source && (
          <>
            {' '}
            ·{' '}
            <a
              href={data.oregon_source}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline"
            >
              Oregon&apos;s
            </a>
          </>
        )}
      </p>
      {(data.registries ?? []).length > 1 && (
        <div className="flex flex-wrap gap-1">
          {(data.registries ?? []).map((r) => (
            <button
              key={r.registry}
              type="button"
              onClick={() => {
                setRegistry(registry === r.registry ? '' : r.registry);
                setFlag('');
              }}
              className={`px-1.5 py-0.5 rounded border text-micro ${registry === r.registry ? 'border-accent text-accent' : 'border-line text-ink-dim hover:text-ink'}`}
            >
              {r.brokers} {REGISTRY_LABEL[r.registry] ?? r.registry}
            </button>
          ))}
        </div>
      )}
      <div className="flex flex-wrap gap-1">
        {data.counts.map((c) => (
          <button
            key={c.flag}
            type="button"
            onClick={() => setFlag(flag === c.flag ? '' : c.flag)}
            className={`px-1.5 py-0.5 rounded border text-micro ${flag === c.flag ? 'border-accent text-accent' : 'border-line text-ink-dim hover:text-ink'}`}
          >
            {c.yes} {c.label}
          </button>
        ))}
      </div>
      <Input
        label="Find a broker"
        hideLabel
        size="sm"
        containerClassName="w-full"
        value={q}
        onChange={(e) => setQ(e.target.value)}
        placeholder="Find a broker"
      />
      <ul className="space-y-1 max-h-72 overflow-y-auto">
        {data.brokers.map((b) => (
          <li key={b.name} className="border-t border-line/60 pt-1">
            <div className="flex justify-between gap-2">
              <span className="text-ink">
                {b.name}
                {b.dba ? <span className="text-ink-mute"> ({b.dba})</span> : null}
              </span>
              {b.privacy_url && (
                <a
                  href={b.privacy_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-micro text-accent hover:underline shrink-0"
                >
                  opt out
                </a>
              )}
            </div>
            <div className="text-micro text-ink-mute">
              {b.registry === 'OR'
                ? 'Registered in Oregon, which publishes no declarations'
                : brokerFlags(b.flags, labels).join('; ') || 'no sensitive declarations'}
              {b.registry && b.registry !== 'CA' && (
                <span className="text-ink-dim"> · {REGISTRY_LABEL[b.registry] ?? b.registry}</span>
              )}
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}

interface FeedItem {
  kind: string;
  at: string | null;
  state: string | null;
  title: string;
  detail: string | null;
  url: string | null;
  source?: string;
  ref?: string | null;
  topics: string[];
}

/** What each kind of feed item is, in a word or two. */
export const FEED_KIND: Record<string, string> = {
  news: 'News',
  finding: 'Legal finding',
  portal_dropped: 'Portal dropped',
  portal_new: 'New portal',
  drone_waiver: 'Drone waiver',
  federal_award: 'Federal award',
  deployment: 'Deployment',
  court_case: 'Court case',
  lobbying: 'Lobbying',
};

function WatchFeed({
  state,
  onOpenPortal,
}: {
  state: string;
  onOpenPortal: (slug: string) => void;
}) {
  const [kind, setKind] = useState<string>('');
  const [days, setDays] = useState<number>(30);
  const { data } = useSWR<{ items: FeedItem[] }>(
    `/civic/feed?days=${days}${state ? `&state=${state}` : ''}`,
    fetcher,
  );
  if (!data) return <LoadingLines />;
  const counts = data.items.reduce<Record<string, number>>(
    (a, i) => ({ ...a, [i.kind]: (a[i.kind] ?? 0) + 1 }),
    {},
  );
  const items = kind ? data.items.filter((i) => i.kind === kind) : data.items;
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-1 text-micro">
        {Object.entries(counts).map(([k, n]) => (
          <button
            key={k}
            type="button"
            onClick={() => setKind(kind === k ? '' : k)}
            className={`px-1.5 py-0.5 rounded border ${kind === k ? 'border-accent text-accent' : 'border-line text-ink-dim hover:text-ink'}`}
          >
            {FEED_KIND[k] ?? k} {n}
          </button>
        ))}
        <select
          value={days}
          onChange={(e) => setDays(Number(e.target.value))}
          className="bg-base border border-line rounded px-1 text-ink-mute"
        >
          <option value={7}>7 days</option>
          <option value={30}>30 days</option>
          <option value={90}>90 days</option>
        </select>
      </div>
      {items.length === 0 ? (
        <p className="text-ink-mute">Nothing in this window yet.</p>
      ) : (
        <ul className="space-y-1 max-h-96 overflow-y-auto">
          {items.map((i, n) => (
            <li key={`${i.kind}-${i.ref ?? i.url ?? n}`} className="border-t border-line/60 pt-1">
              <div className="flex flex-wrap items-baseline gap-2">
                <span className="px-1.5 rounded border border-line text-micro text-ink-dim">
                  {FEED_KIND[i.kind] ?? i.kind}
                </span>
                {i.url ? (
                  <a
                    href={i.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-ink hover:text-accent"
                  >
                    {i.title}
                  </a>
                ) : (i.kind === 'finding' || i.kind.startsWith('portal')) && i.ref ? (
                  <button
                    type="button"
                    className="text-left text-ink hover:text-accent"
                    onClick={() => onOpenPortal(i.ref!)}
                  >
                    {i.title}
                  </button>
                ) : (
                  <span className="text-ink">{i.title}</span>
                )}
              </div>
              <div className="text-micro text-ink-mute">
                {[i.at?.slice(0, 10), i.source, i.state, i.topics.join(', ')]
                  .filter(Boolean)
                  .join(' · ')}
                {i.detail ? ` — ${i.detail}` : ''}
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

interface JudgeTie {
  vendor: string;
  years: number[];
  latest_value: string | null;
  description: string | null;
}

interface CourtCase {
  docket_id: number;
  case_name: string;
  court_id: string | null;
  date_filed: string | null;
  date_terminated: string | null;
  judge: string | null;
  suit_nature: string | null;
  url: string;
  topics: string[];
  judge_ties: {
    ties: JudgeTie[];
    party_holdings: string[];
    possible_455_conflict: boolean;
    note: string | null;
  } | null;
}

/** A judge's disclosed vendor holdings in one line. */
export function tieLine(ties: JudgeTie[]): string {
  return ties
    .map(
      (t) =>
        `${t.vendor} (${t.years.join(', ') || 'year unknown'}${t.latest_value ? `, ${t.latest_value}` : ''})`,
    )
    .join('; ');
}

function CourtCases() {
  const [topic, setTopic] = useState<string>('');
  const [kind, setKind] = useState<'civil' | 'criminal' | ''>('civil');
  const { data } = useSWR<{ cases: CourtCase[]; totals: Record<string, number>; caveat: string }>(
    `/civic/courts?limit=300${kind ? `&kind=${kind}` : ''}${topic ? `&topic=${encodeURIComponent(topic)}` : ''}`,
    fetcher,
  );
  if (!data) return <LoadingLines />;
  const t = data.totals;
  const topics = Array.from(new Set(data.cases.flatMap((c) => c.topics))).sort();
  return (
    <div className="space-y-2">
      <p className="text-micro text-ink-dim">
        {formatNumber(t.cases ?? 0, { decimals: 0 })} federal cases over surveillance before{' '}
        {formatNumber(t.judges ?? 0, { decimals: 0 })} judges (
        {formatNumber(t.judges_checked ?? 0, { decimals: 0 })} disclosures checked so far).{' '}
        {t.cases_with_tied_judge ?? 0} cases have a judge who disclosed holding a surveillance
        vendor&apos;s stock; {t.possible_455_conflicts ?? 0} where that vendor is a party to the
        case.
      </p>
      <div className="flex flex-wrap gap-1 text-micro">
        {(
          [
            ['civil', `Civil suits ${t.civil ?? ''}`],
            ['criminal', `Criminal cases ${t.criminal ?? ''}`],
            ['', 'Both'],
          ] as const
        ).map(([k, label]) => (
          <button
            key={k || 'both'}
            type="button"
            onClick={() => setKind(k)}
            className={`px-1.5 py-0.5 rounded border font-bold ${kind === k ? 'border-accent text-accent' : 'border-line text-ink-dim hover:text-ink'}`}
          >
            {label}
          </button>
        ))}
      </div>
      <div className="flex flex-wrap gap-1 text-micro">
        {['', ...topics].map((k) => (
          <button
            key={k || 'all'}
            type="button"
            onClick={() => setTopic(k)}
            className={`px-1.5 py-0.5 rounded border ${topic === k ? 'border-accent text-accent' : 'border-line text-ink-dim hover:text-ink'}`}
          >
            {k || 'all'}
          </button>
        ))}
      </div>
      <ul className="space-y-1 max-h-96 overflow-y-auto">
        {data.cases.map((c) => (
          <li key={c.docket_id} className="border-t border-line/60 pt-1">
            <div className="flex flex-wrap items-baseline gap-2">
              {c.judge_ties?.possible_455_conflict && (
                <span className="px-1.5 rounded border border-negative/30 text-negative text-micro font-bold">
                  Judge holds a party&apos;s stock
                </span>
              )}
              <a
                href={c.url}
                target="_blank"
                rel="noopener noreferrer"
                className="text-ink hover:text-accent"
              >
                {c.case_name}
              </a>
            </div>
            <div className="text-micro text-ink-mute">
              {[
                c.date_filed,
                c.court_id?.toUpperCase(),
                c.judge,
                c.suit_nature,
                c.date_terminated ? `closed ${c.date_terminated}` : null,
              ]
                .filter(Boolean)
                .join(' · ')}
            </div>
            {c.judge_ties && c.judge_ties.ties.length > 0 && (
              <div className="text-micro text-caution">
                Judge&apos;s disclosed holdings: {tieLine(c.judge_ties.ties)}. {c.judge_ties.note}
              </div>
            )}
          </li>
        ))}
      </ul>
      <p className="text-micro text-ink-mute">{data.caveat}</p>
    </div>
  );
}

/** Possible rule-breaking: each agency's published Flock practice checked
 * against its state's plate-reader law. The state comes from the page, so the
 * map, the feed and this list stay on the same place. */
export function Findings({
  state,
  onOpenPortal,
}: {
  state: string;
  onOpenPortal: (slug: string) => void;
}) {
  const [severity, setSeverity] = useState<string>('');
  const q = new URLSearchParams();
  if (state) q.set('state', state);
  if (severity) q.set('severity', severity);
  q.set('limit', '300');
  const { data, error } = useSWR<LegalData>(`/civic/legal?${q.toString()}`, fetcher);
  const { data: laws } = useSWR<{ laws: StateLaw[]; caveat: string }>('/civic/laws', fetcher);
  const law = state ? laws?.laws.find((l) => l.state === state) : undefined;
  const conflicts = (data?.counts ?? [])
    .filter((c) => c.severity === 'conflict')
    .reduce((a, c) => a + c.n, 0);
  const checks = (data?.counts ?? [])
    .filter((c) => c.severity === 'check')
    .reduce((a, c) => a + c.n, 0);
  const t = data?.totals;
  return (
    <div className="space-y-3">
      <p className="text-sm text-ink-dim max-w-3xl">
        Agencies that publish a Flock page say how long they keep plate data and who they share it
        with. We check that against their state&apos;s law and against ICE immigration agreements.
        Laws have exceptions that published data can&apos;t show, so treat each item as a fair
        question to ask &mdash; the evidence and the law are attached.
      </p>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2">
        <Stat value={t?.portals} label="agencies publish a Flock page" />
        <Stat value={t?.cameras} label="Flock cameras they list" />
        <Stat value={t?.vehicles} label="vehicles captured (30 days)" />
        <Stat value={t?.ice_287g_agencies} label="agencies working with ICE (287(g))" />
        <Stat value={conflicts} label="possible violations" tone="text-negative" />
        <Stat value={checks} label="worth asking about" tone="text-caution" />
      </div>
      <div className="flex flex-wrap items-center gap-2">
        {(
          [
            ['', 'Everything'],
            ['conflict', 'Possible violations'],
            ['check', 'Worth asking about'],
          ] as const
        ).map(([k, label]) => (
          <button
            key={k || 'all'}
            type="button"
            aria-pressed={severity === k}
            onClick={() => setSeverity(k)}
            className={`px-3 py-1 rounded-full border text-xs ${severity === k ? 'border-accent bg-accent-dim text-ink' : 'border-line text-ink-dim hover:text-ink'}`}
          >
            {label}
          </button>
        ))}
      </div>
      {state && laws && (
        <p className="text-xs text-ink-dim">
          {lawLine(law, state)}
          {law?.note ? ` ${law.note}` : ''}{' '}
          {law && (
            <a
              href={law.source}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline"
            >
              Read the law
            </a>
          )}
        </p>
      )}
      {error ? (
        <p className="text-ink-mute">We couldn&apos;t load these right now.</p>
      ) : !data ? (
        <LoadingLines />
      ) : data.findings.length === 0 ? (
        <p className="text-ink-mute">Nothing to flag here{state ? ` in ${state}` : ''}.</p>
      ) : (
        <ul className="space-y-1.5 text-xs max-h-[32rem] overflow-y-auto pr-1">
          {data.findings.map((f) => (
            <FindingItem key={`${f.slug}-${f.code}`} f={f} onOpen={onOpenPortal} />
          ))}
        </ul>
      )}
    </div>
  );
}

/** One headline figure: big number, plain label. */
export function Stat({
  value,
  label,
  tone,
}: {
  value: number | null | undefined;
  label: string;
  tone?: string;
}) {
  return (
    <div className="rounded-lg border border-line bg-inset px-3 py-2">
      <div className={`text-lg font-semibold font-mono ${tone ?? 'text-ink'}`}>
        {value != null ? formatNumber(value, { decimals: 0 }) : ABSENT}
      </div>
      <div className="text-micro text-ink-mute leading-snug">{label}</div>
    </div>
  );
}

export {
  CourtCases,
  DataBrokers,
  Departures,
  DroneWaivers,
  FederalMoney,
  Lobbying,
  SearchLogs,
  WatchFeed,
};
