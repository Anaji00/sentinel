'use client';

/**
 * One trade idea as a ruled row: the ticker large, one plain sentence, and the fine print (rank and the odds) in small
 * type, with the record of past ideas left to the card. Direction is carried by the word and an arrow; the expected
 * move is hidden when many ideas share the same value, since that says nothing about this stock.
 */

import React from 'react';
import { expectedMove, rankWords, rightWayOdds, type Idea } from '../lib/ideas';
import { formatNumber } from '../lib/format';
import { StockLink } from './StockDrawer';

export function ideaSentence(i: Idea, days: number, plateau: boolean, fallbackBps: number | null = null): string {
  const move = plateau ? null : expectedMove(i, fallbackBps);
  if (move == null) return 'No forecast for this stock: many top ideas get the same expected move.';
  const sign = move >= 0 ? '+' : '−';
  return `The model expects ${sign}${formatNumber(Math.abs(move), { decimals: 1 })}%, ${move >= 0 ? 'better' : 'worse'} than the S&P 500 over the next ${days === 1 ? 'day' : `${days} trading days`}.`;
}

export function ideaFine(i: Idea, plateau: boolean): string {
  const rank = i.industry_percentile != null ? `${rankWords(i.industry_percentile)} of its industry` : i.percentile != null ? `${rankWords(i.percentile)} of all stocks` : '';
  const odds = plateau ? null : rightWayOdds(i);
  return [rank, odds != null ? `chance it goes our way ${formatNumber(odds * 100, { decimals: 0 })}%` : ''].filter(Boolean).join(' · ');
}

export function IdeaRow({ idea, days, plateau, fallbackBps }: { idea: Idea; days: number; plateau: boolean; fallbackBps?: number | null }) {
  const long = idea.direction > 0;
  return (
    <li className="grid grid-cols-[6.5rem_minmax(0,1fr)] gap-x-4 border-t border-line py-3 first:border-t-0">
      <div>
        <StockLink ticker={idea.ticker} className="font-mono text-lead font-semibold leading-none text-ink hover:text-accent" />
        <div className={`mt-1 font-mono text-micro ${long ? 'text-positive' : 'text-negative'}`}>{long ? '▲ buy' : '▼ bet against'}</div>
      </div>
      <div className="min-w-0 space-y-0.5">
        <p className="text-sm leading-snug text-ink text-pretty">{ideaSentence(idea, days, plateau, fallbackBps ?? null)}</p>
        <p className="text-micro text-ink-mute">{ideaFine(idea, plateau)}</p>
      </div>
    </li>
  );
}

export default IdeaRow;
