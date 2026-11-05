'use client';

import React from 'react';
import useSWR from 'swr';
import { failedWithout } from '../lib/dataHealth';
import { PanelError } from './ui/PanelError';
import { fetcher } from '../lib/api';
import { directionMark, formatNumber } from '../lib/format';
import { TAPE_KEY, tapeRows, type TapePoint, type TapeRow } from '../lib/tape';
import { POLL } from './ui/DataProvider';
import { flashClass, useFlash } from './ui/useFlash';

function Quote({ row }: { row: TapeRow }) {
  const flash = useFlash(row.price);
  const tone =
    row.changePct == null || row.changePct === 0 ? 'text-ink-mute' : row.changePct > 0 ? 'text-positive' : 'text-negative';
  return (
    <span className="flex shrink-0 items-baseline gap-1.5 font-mono text-xs tabular">
      <span className="text-ink-dim">{row.symbol}</span>
      <span className={`rounded px-1 text-ink ${flashClass(flash)}`}>
        {formatNumber(row.price, { decimals: row.price >= 1000 ? 0 : 2 })}
      </span>
      {row.changePct != null && (
        <span className={tone}>
          {directionMark(row.changePct)} {formatNumber(Math.abs(row.changePct), { decimals: 2 })}%
        </span>
      )}
    </span>
  );
}

/** A strip of live prices pinned above every Markets view. */
export function LiveTape() {
  const { data, error, mutate } = useSWR<{ series: Record<string, TapePoint[]> }>(TAPE_KEY, fetcher, { refreshInterval: POLL.live });
  const rows = tapeRows(data?.series);
  return (
    <div
      aria-label="Live prices"
      title="Latest price, and the change across the most recent readings"
      // shrink-0: the page is a flex column, and the tape shrank to its own
      // scrollbar (18px) with every price hidden.
      className="flex shrink-0 items-center gap-6 overflow-x-auto rounded-xl border border-line bg-inset px-4 py-2"
    >
      <span className="flex shrink-0 items-center gap-1.5 text-micro font-semibold uppercase tracking-[0.1em] text-positive">
        <span className="h-1.5 w-1.5 rounded-full bg-positive" aria-hidden />
        Live
      </span>
      {rows.length === 0 ? (
        failedWithout(error, data) ? (
          <PanelError inline what="prices" onRetry={() => void mutate()} />
        ) : (
          <span className="text-xs text-ink-mute">Waiting for prices…</span>
        )
      ) : (
        rows.map((r) => <Quote key={r.symbol} row={r} />)
      )}
    </div>
  );
}

export default LiveTape;
