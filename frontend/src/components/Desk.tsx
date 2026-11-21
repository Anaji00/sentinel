'use client';

/**
 * The desk: three panes read left to right. On the left, the stocks currently moving away from what their peers did, newest
 * first. In the middle, the one you pick: what changed, who it is measured against, and the facts behind it. On the right,
 * the optional bull-and-bear write-up. Pick with a click or the j and k keys. Every pane is built from pieces that already
 * exist elsewhere in the app, so nothing here shows a figure the rest of the app would not.
 */

import { peerMove } from '../lib/peerMove';
import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { openSituationList } from '../lib/divergence';
import { DivergenceBrief, StageBadge } from './DivergenceBrief';
import { Button } from './ui/Button';
import { EmptyState } from './ui/EmptyState';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';
import { POLL } from './ui/DataProvider';
import { Updated } from './ui/Updated';
import { ZWhisker } from './ui/ZWhisker';
import { StockLink } from './StockDrawer';
import { TickerHero } from './TickerHero';
import ThesisPanel from './ThesisPanel';
import WhatChanged from './WhatChanged';
import RelatedStocks from './RelatedStocks';
import SourceFacts from './SourceFacts';
import ComparePanel from './ComparePanel';
import HeldBack from './HeldBack';
import { nextRowIndex } from '../lib/rowKeys';
import { useCompare } from '../lib/useCompare';
import type { Brief, Situation } from '../lib/divergence';

interface Alert {
  correlation_id: string;
  detected_at?: string | null;
  description?: string | null;
  tags?: string[] | null;
  situation?: Situation | null;
  brief?: Brief | null;
  horizon?: string | null;
  breadth?: number | null;
  beta?: number | null;
  relative_volume?: number | null;
}

const KEY = '/correlations?limit=200&min_tier=1';

export default function Desk() {
  const { data, error, mutate } = useSWR<Alert[]>(KEY, fetcher, { refreshInterval: POLL.standard });
  const list = React.useMemo(() => openSituationList(data), [data]);
  const [picked, setPicked] = React.useState<string | null>(null);
  const current = list.find((a) => a.ticker === picked) ?? list[0] ?? null;
  // The desk's own pick is not in the address, so `?c=` alone names the stock to compare it with.
  const cmp = useCompare();

  const move = React.useCallback(
    (delta: number) => {
      if (list.length === 0) return;
      const i = Math.max(0, list.findIndex((a) => a.ticker === current?.ticker));
      setPicked(list[Math.min(list.length - 1, Math.max(0, i + delta))].ticker);
    },
    [list, current],
  );

  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const el = e.target as HTMLElement | null;
      if (el && (el.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName))) return;
      if (e.ctrlKey || e.metaKey || e.altKey) return;
      if (e.key === 'j') move(1);
      else if (e.key === 'k') move(-1);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [move]);

  return (
    <div className="grid min-h-[70vh] grid-cols-1 gap-px overflow-hidden rounded-sm border border-line bg-line lg:h-[calc(100vh-9rem)] lg:grid-cols-[28%_47%_25%]">
      <section aria-label="Stocks moving against their peers" className="flex min-h-0 flex-col bg-raised [contain:layout_style_paint]">
        <header className="flex items-baseline justify-between gap-2 border-b border-line px-2.5 py-1.5">
          <h2 className="text-xs font-semibold text-ink">Stocks moving against their peers</h2>
          <Updated source={KEY} pollMs={POLL.standard} />
        </header>
        <div className="min-h-0 flex-1 overflow-y-auto">
          {failedWithout(error, data) ? (
            <div className="p-2.5">
              <PanelError what="the situations" onRetry={() => void mutate()} />
            </div>
          ) : !data ? (
            <div className="p-2.5">
              <LoadingLines lines={5} label="Loading the situations" />
            </div>
          ) : list.length === 0 ? (
            <EmptyState kind="empty" title="No stock is moving against its peers right now" detail="An alert appears when a stock moves 2.5 or more usual-sized moves away from its peers. Checked every 10 minutes on weekdays." />
          ) : (
            <ul
              aria-label="Situations. Arrow keys move between rows."
              onKeyDown={(e) => {
                const rows = Array.from(e.currentTarget.querySelectorAll<HTMLButtonElement>('button'));
                const to = nextRowIndex(e.key, rows.indexOf(document.activeElement as HTMLButtonElement), rows.length);
                if (to === null) return;
                e.preventDefault();
                rows[to]?.focus();
                setPicked(list[to]?.ticker ?? '');
              }}
            >
              {list.map((a) => {
                const on = a.ticker === current?.ticker;
                return (
                  <li key={a.correlation_id}>
                    <Button
                      variant="ghost"
                      onClick={() => setPicked(a.ticker)}
                      pressed={on}
                      className={`grid! w-full grid-cols-[3.5rem_minmax(0,1fr)_auto] items-center gap-x-2 rounded-none! border-b border-line/60 px-2.5 py-1.5 text-left ${on ? 'bg-overlay shadow-[inset_2px_0_0_var(--accent)]' : 'hover:bg-overlay/50'}`}
                    >
                      <span className="font-mono text-sm font-semibold text-ink">{a.ticker}</span>
                      <span className="min-w-0">
                        {(() => {
                          const mv = peerMove(a.description);
                          return mv ? (
                            <span className="flex items-baseline gap-1.5">
                              <span className={`font-mono text-base font-semibold tabular-nums ${mv.pct < 0 ? 'text-negative' : 'text-positive'}`}>
                                {mv.pct < 0 ? '▼' : '▲'} {Math.abs(mv.pct).toFixed(1)}%
                              </span>
                              <span className="truncate text-micro text-ink-dim">{mv.span}</span>
                            </span>
                          ) : (
                            <span className="block truncate text-micro text-ink-dim">{(a.description ?? '').replace(/^\S+\s+/, '').split(/ while |: /)[0]}</span>
                          );
                        })()}
                        <span className="mt-0.5 flex items-center gap-2">
                          <ZWhisker z={a.situation?.last_z} />
                          <span className="font-mono text-micro tabular-nums text-ink-mute">{a.situation?.last_z != null ? `${a.situation.last_z > 0 ? '+' : ''}${a.situation.last_z.toFixed(2)}` : ''}</span>
                        </span>
                      </span>
                      <StageBadge situation={a.situation} horizon={a.horizon} />
                    </Button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </section>

      <section aria-label="The stock you picked" className="min-h-0 overflow-y-auto bg-raised [contain:layout_style_paint]">
        {current ? (
          <div className="space-y-3 p-2.5">
            <div className="flex items-center justify-between gap-2">
              <h2 className="text-xs font-semibold text-ink">{current.ticker}</h2>
              <StockLink ticker={current.ticker} className="text-micro text-accent hover:underline">
                Open the full stock page
              </StockLink>
            </div>
            <TickerHero symbol={current.ticker} />
            {cmp.other && cmp.other !== current.ticker && (
              <ComparePanel
                a={current.ticker}
                b={cmp.other}
                onSwap={() => {
                  setPicked(cmp.other);
                  cmp.pin(current.ticker);
                }}
                onClear={cmp.clear}
              />
            )}
            <DivergenceBrief id={`desk-${current.correlation_id}`} situation={current.situation} brief={current.brief} measures={current} />
            <WhatChanged ticker={current.ticker} />
            <RelatedStocks ticker={current.ticker} />
            <HeldBack ticker={current.ticker} />
            <SourceFacts ticker={current.ticker} />
          </div>
        ) : (
          <EmptyState kind="empty" title="Nothing picked" detail="Pick a stock on the left, or press j and k to move through them." />
        )}
      </section>

      <section aria-label="Bull and bear case" className="min-h-0 overflow-y-auto bg-raised [contain:layout_style_paint]">
        <div className="space-y-3 p-2.5">
          <h2 className="text-xs font-semibold text-ink">What could happen</h2>
          {current ? <ThesisPanel ticker={current.ticker} /> : <p className="text-xs text-ink-mute">The bull and bear case appears once a stock is picked.</p>}
        </div>
      </section>
    </div>
  );
}
