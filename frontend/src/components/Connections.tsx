'use client';

/**
 * The relationship explorer: pick any company, ship, flag or place the platform has stored links for,
 * see what it is connected to, and ask how two things are linked.
 *
 * Reads /graph/entities (the most connected things, to start from), /graph/entity/{id} (a thing's links,
 * one or two steps out) and /graph/shortest-path. All the reading rules live in lib/connections.ts: a link
 * that was not stored is never drawn, and a path that was not found says so.
 */

import React from 'react';
import useSWR from 'swr';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { formatNumber } from '../lib/format';
import {
  displayName,
  groupHubs,
  layoutRings,
  matchHubs,
  parseEntityGraph,
  pluralKind,
  parsePath,
  strengthWords,
  type EntityGraphResponse,
  type EntityView,
  type Hub,
} from '../lib/connections';
import { Button } from './ui/Button';
import { ClockTime } from './ui/ClockTime';
import { EmptyState } from './ui/EmptyState';
import { Input } from './ui/Input';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

const SIZE = 600;

/** A node's colour comes from its kind, through theme tokens so both themes work. */
const TYPE_FILL: Record<string, string> = {
  Company: 'fill-accent',
  Vessel: 'fill-info',
  Aircraft: 'fill-info',
  Flag: 'fill-civic',
  Region: 'fill-civic',
  Sector: 'fill-caution',
  Index: 'fill-positive',
  MacroFactor: 'fill-caution',
};
const fillFor = (type: string) => TYPE_FILL[type] ?? 'fill-ink-mute';

function useParam(name: string): [string, (v: string | null) => void] {
  const params = useSearchParams();
  const router = useRouter();
  const pathname = usePathname();
  const value = params?.get(name) ?? '';
  const set = React.useCallback(
    (v: string | null) => {
      const next = new URLSearchParams(params?.toString() ?? '');
      if (v) next.set(name, v);
      else next.delete(name);
      const qs = next.toString();
      router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });
    },
    [name, params, pathname, router],
  );
  return [value, set];
}

/** A text box with suggestions from the most connected things; Enter takes whatever is typed. */
function EntityPicker({
  label,
  placeholder,
  hubs,
  onPick,
}: {
  label: string;
  placeholder: string;
  hubs: Hub[];
  onPick: (id: string) => void;
}) {
  const [text, setText] = React.useState('');
  const [open, setOpen] = React.useState(false);
  const [cursor, setCursor] = React.useState(0);
  const hits = React.useMemo(() => matchHubs(hubs, text), [hubs, text]);
  const choose = (id: string) => {
    const v = id.trim();
    if (!v) return;
    setText('');
    setOpen(false);
    onPick(v);
  };
  return (
    <div className="relative min-w-[16rem] flex-1">
      <Input
        label={label}
        placeholder={placeholder}
        value={text}
        autoComplete="off"
        spellCheck={false}
        onChange={(e) => {
          setText(e.target.value);
          setCursor(0);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        onKeyDown={(e) => {
          if (e.key === 'ArrowDown') {
            e.preventDefault();
            setCursor((c) => Math.min(c + 1, Math.max(hits.length - 1, 0)));
          } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            setCursor((c) => Math.max(c - 1, 0));
          } else if (e.key === 'Enter') {
            e.preventDefault();
            choose(open && hits[cursor] ? hits[cursor].id : text);
          } else if (e.key === 'Escape') {
            setOpen(false);
          }
        }}
      />
      {open && hits.length > 0 && (
        <ul role="listbox" aria-label={`${label} suggestions`} className="absolute left-0 right-0 top-full z-20 mt-1 overflow-hidden rounded-lg border border-line-strong bg-inset py-1 shadow-overlay">
          {hits.map((h, i) => (
            <li
              key={h.id}
              role="option"
              aria-selected={i === cursor}
              onMouseDown={(e) => {
                e.preventDefault();
                choose(h.id);
              }}
              onMouseEnter={() => setCursor(i)}
              className={`flex cursor-pointer items-baseline gap-2 px-3 py-1.5 text-sm ${i === cursor ? 'bg-overlay/60 text-ink' : 'text-ink-dim'}`}
            >
              <span className="min-w-0 flex-1 truncate font-medium text-ink">{displayName(h.name)}</span>
              <span className="shrink-0 text-micro text-ink-mute">{h.type}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function Diagram({ view, onOpen }: { view: EntityView; onOpen: (name: string) => void }) {
  const pos = React.useMemo(() => layoutRings(view, SIZE), [view]);
  const types = [...new Set(view.nodes.map((n) => n.type))];
  const dense = view.nodes.length > 18;
  return (
    <div className="space-y-2">
      <svg viewBox={`0 0 ${SIZE} ${SIZE}`} className="w-full max-h-[70vh] rounded-md border border-line bg-inset" role="group" aria-label={`Connections of ${view.center}`}>
        {view.edges.map((e, i) => {
          const a = pos.get(e.from);
          const b = pos.get(e.to);
          if (!a || !b) return null;
          return (
            <line
              key={i}
              x1={a.x}
              y1={a.y}
              x2={b.x}
              y2={b.y}
              className={e.stored ? 'stroke-ink-dim' : 'stroke-ink-mute'}
              strokeDasharray={e.stored ? undefined : '4 4'}
              strokeOpacity={0.25 + 0.6 * Math.min(1, e.strength)}
              strokeWidth={1 + 2 * Math.min(1, e.strength)}
            />
          );
        })}
        {view.nodes.map((n) => {
          const p = pos.get(n.name);
          if (!p) return null;
          const isCenter = n.name === view.center;
          const shown = displayName(n.name);
          const max = dense && !isCenter ? 14 : 22;
          const label = shown.length > max ? `${shown.slice(0, max - 1)}…` : shown;
          // In a crowded ring each name sits just outside its dot, on the side it is on, so names do not overlap.
          const dx = p.x - SIZE / 2;
          const dy = p.y - SIZE / 2;
          const norm = Math.hypot(dx, dy) || 1;
          const outward = dense && !isCenter;
          const lx = outward ? p.x + (dx / norm) * 14 : p.x;
          const ly = outward ? p.y + (dy / norm) * 14 + 4 : p.y + (isCenter ? 32 : 24);
          const anchor = outward ? (dx > 8 ? 'start' : dx < -8 ? 'end' : 'middle') : 'middle';
          return (
            <g
              key={n.id}
              role="button"
              tabIndex={0}
              aria-label={isCenter ? `${n.name}, the centre` : `Show the links of ${n.name}`}
              onClick={() => !isCenter && onOpen(n.name)}
              onKeyDown={(e) => {
                if ((e.key === 'Enter' || e.key === ' ') && !isCenter) {
                  e.preventDefault();
                  onOpen(n.name);
                }
              }}
              className={isCenter ? '' : 'cursor-pointer'}
            >
              <title>{`${n.name} (${n.type})`}</title>
              <circle cx={p.x} cy={p.y} r={isCenter ? 15 : 9} className={`${fillFor(n.type)} stroke-inset`} strokeWidth={2} />
              <text
                x={lx}
                y={ly}
                textAnchor={anchor}
                className={`${isCenter ? 'fill-ink font-semibold' : 'fill-ink-dim'} select-none`}
                fontSize={isCenter ? 14 : dense ? 11 : 12}
              >
                {label}
              </text>
            </g>
          );
        })}
      </svg>
      <ul className="flex flex-wrap gap-x-4 gap-y-1 text-micro text-ink-mute" aria-label="Colour key">
        {types.map((t) => (
          <li key={t} className="flex items-center gap-1.5">
            <svg width="10" height="10" aria-hidden>
              <circle cx="5" cy="5" r="5" className={fillFor(t)} />
            </svg>
            {t}
          </li>
        ))}
        {view.edges.some((e) => !e.stored) && (
          <li className="flex items-center gap-1.5">
            <svg width="20" height="6" aria-hidden>
              <line x1="0" y1="3" x2="20" y2="3" className="stroke-ink-mute" strokeDasharray="4 4" strokeWidth="2" />
            </svg>
            from recent events
          </li>
        )}
      </ul>
    </div>
  );
}

function PathResult({ from, to }: { from: string; to: string }) {
  const { data, error } = useSWR<{ path?: unknown[] }>(
    `/graph/shortest-path?source_id=${encodeURIComponent(from)}&target_id=${encodeURIComponent(to)}`,
    fetcher,
    { revalidateOnFocus: false },
  );
  if (failedWithout(error, data)) return <PanelError what="the path search" inline />;
  if (!data) return <LoadingLines lines={2} label="Looking for a link" />;
  const path = parsePath(data as Parameters<typeof parsePath>[0]);
  if (!path.found) {
    return (
      <p role="status" className="text-sm text-ink-dim">
        No stored link joins {from} and {to} within six steps. That does not prove they are unrelated, only that this platform has not recorded a connection.
      </p>
    );
  }
  return (
    <ol className="flex flex-wrap items-center gap-1.5 text-sm" aria-label={`How ${from} is linked to ${to}`}>
      {path.steps.map((s, i) => (
        <React.Fragment key={`${s.name}-${i}`}>
          <li className="rounded-full border border-line-accent bg-accent-dim px-2.5 py-0.5 font-medium text-ink">{s.name}</li>
          {s.link && (
            <li aria-hidden className="text-micro text-ink-mute">
              — {s.link} →
            </li>
          )}
        </React.Fragment>
      ))}
    </ol>
  );
}

export default function Connections() {
  const [entity, setEntity] = useParam('e');
  const [target, setTarget] = useParam('to');
  const [hopsParam, setHops] = useParam('hops');
  const hops = hopsParam === '2' ? 2 : 1;

  const hubsQuery = useSWR<{ entities?: Hub[] }>('/graph/entities?limit=200', fetcher, { revalidateOnFocus: false });
  const hubs = React.useMemo(() => hubsQuery.data?.entities ?? [], [hubsQuery.data]);

  const graph = useSWR<EntityGraphResponse>(entity ? `/graph/entity/${encodeURIComponent(entity)}?hops=${hops}` : null, fetcher, {
    revalidateOnFocus: false,
  });
  const view = React.useMemo(() => parseEntityGraph(graph.data), [graph.data]);
  const groups = React.useMemo(() => groupHubs(hubs), [hubs]);

  const open = (name: string) => {
    setTarget(null);
    setEntity(name);
  };

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-end gap-3">
        <EntityPicker label="Start from" placeholder="A company, ship, flag or place" hubs={hubs} onPick={open} />
        {entity && (
          <>
            <EntityPicker label="How is it linked to…" placeholder="Another name" hubs={hubs} onPick={(id) => setTarget(id)} />
            <div role="group" aria-label="How far to look" className="flex items-center gap-1.5 pb-0.5">
              <Button variant={hops === 1 ? 'secondary' : 'outline'} size="sm" pressed={hops === 1} onClick={() => setHops(null)}>
                Direct links
              </Button>
              <Button variant={hops === 2 ? 'secondary' : 'outline'} size="sm" pressed={hops === 2} onClick={() => setHops('2')}>
                Two steps out
              </Button>
              <Button variant="ghost" size="sm" onClick={() => open('')} className="px-2 text-ink-mute">
                Clear
              </Button>
            </div>
          </>
        )}
      </div>

      {entity && target && (
        <section aria-label="How they are linked" className="space-y-2 rounded-md border border-line bg-raised p-4">
          <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">
            {entity} and {target}
          </h2>
          <PathResult from={entity} to={target} />
        </section>
      )}

      {!entity ? (
        failedWithout(hubsQuery.error, hubsQuery.data) ? (
          <PanelError what="the list of connected names" onRetry={() => void hubsQuery.mutate()} />
        ) : !hubsQuery.data ? (
          <LoadingLines lines={5} label="Loading the most connected names" />
        ) : groups.length === 0 ? (
          <EmptyState kind="empty" title="No connections stored yet" detail="The relationship graph fills as collectors link companies, ships and places. Try again later." />
        ) : (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {groups.map((g) => (
              <section key={g.type} aria-label={g.type} className="space-y-2 rounded-md border border-line bg-raised p-4">
                <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Most connected {pluralKind(g.type)}</h2>
                <ul className="space-y-1">
                  {g.hubs.map((h) => (
                    <li key={h.id}>
                      <Button variant="ghost" onClick={() => open(h.id)} className="w-full justify-between! gap-3 rounded-md px-2 py-1 text-left text-sm hover:bg-overlay/50">
                        <span className="min-w-0 truncate text-ink">{displayName(h.name)}</span>
                        <span className="shrink-0 font-mono text-micro text-ink-mute">{formatNumber(h.degree, { decimals: 0 })} links</span>
                      </Button>
                    </li>
                  ))}
                </ul>
              </section>
            ))}
          </div>
        )
      ) : failedWithout(graph.error, graph.data) ? (
        <PanelError what={`the links of ${entity}`} onRetry={() => void graph.mutate()} />
      ) : !graph.data ? (
        <LoadingLines lines={5} label={`Finding the links of ${entity}`} />
      ) : view.nodes.length === 0 ? (
        <EmptyState kind="empty" title={`No links stored for ${entity}`} detail="Check the spelling, or pick a name from the suggestions. The graph holds only what collectors have linked." />
      ) : (
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_24rem]">
          <Diagram view={view} onOpen={open} />
          <section aria-label="Links" className="space-y-3">
            {view.fromEvents && (
              <p role="note" className="rounded-lg border border-caution/30 px-3 py-2 text-micro text-caution">
                These links are inferred from recent events, not read from a stored relationship, so treat them as hints.
              </p>
            )}
            <h2 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">
              {view.edges.length} link{view.edges.length === 1 ? '' : 's'}, strongest first
            </h2>
            <ul className="max-h-[60vh] space-y-1.5 overflow-y-auto pr-1">
              {[...view.edges]
                .sort((a, b) => b.strength - a.strength)
                .map((e, i) => (
                  <li key={i} className="rounded-lg border border-line px-3 py-2 text-sm">
                    <div className="text-ink">
                      <Button variant="link" onClick={() => open(e.from)} className="font-medium text-ink hover:text-accent">
                        {displayName(e.from)}
                      </Button>{' '}
                      <span className="text-ink-mute">{e.relationship}</span>{' '}
                      <Button variant="link" onClick={() => open(e.to)} className="font-medium text-ink hover:text-accent">
                        {displayName(e.to)}
                      </Button>
                    </div>
                    <div className="mt-0.5 text-micro text-ink-mute">
                      {strengthWords(e.strength)} link{e.updated ? <>, last confirmed <ClockTime value={e.updated * 1000} withDate seconds={false} /></> : ', no date stored'}
                    </div>
                  </li>
                ))}
            </ul>
            <p className="text-micro text-ink-mute">A link fades as it ages without being confirmed, so a faint link is an old one.</p>
          </section>
        </div>
      )}
    </div>
  );
}
