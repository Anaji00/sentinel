'use client';

/**
 * Two figures at the top of every page, beside the market mood: the paper book against the S&P 500, and how many
 * stock-versus-peer situations are open. They read endpoints the pages already use, so nothing extra is fetched when
 * those pages are open, and a figure that cannot be read is simply not shown.
 */

import React from 'react';
import useSWR from 'swr';
import Link from 'next/link';
import { fetcher } from '../../lib/api';
import { bookFigure, openSituations } from '../../lib/ribbon';
import { accountChip, type AccountSummary } from '../../lib/liveFigures';
import { barLabel } from '../../lib/candles';
import { formatCurrency } from '../../lib/format';
import { POLL } from './DataProvider';

const TONE = { positive: 'text-positive', negative: 'text-negative', neutral: 'text-ink-dim' } as const;

export function DeskFigures() {
  const portfolio = useSWR<{ summary?: { return_pct?: number; spy_return_pct?: number; sessions?: number } }>('/predictions/model-portfolio', fetcher, { refreshInterval: POLL.slow });
  const bookSummary = portfolio.data?.summary;
  const alerts = useSWR<Array<{ correlation_id: string; description?: string; detected_at?: string; tags?: string[]; situation?: { stage?: string } }>>('/correlations?limit=200&min_tier=1', fetcher, {
    refreshInterval: POLL.slow,
  });
  const account = useSWR<AccountSummary>('/portfolio/summary', fetcher, { refreshInterval: POLL.slow, shouldRetryOnError: false });
  const chip = accountChip(account.data, (v) => formatCurrency(v, { decimals: 0 }), (iso) => barLabel(`${iso.slice(0, 10)}T00:00:00Z`, '1d'));
  const book = bookFigure(bookSummary);
  const open = alerts.data ? openSituations(alerts.data) : null;
  if (!book && open == null && !chip) return null;
  return (
    <span className="hidden items-center gap-4 whitespace-nowrap text-micro 2xl:flex">
      {book && (
        <Link href="/performance" title={book.title} className="flex items-center gap-1.5 hover:text-ink">
          <span className="text-ink-mute">Paper book</span>
          <span className={`font-mono tabular-nums ${TONE[book.tone]}`}>{book.text}</span>
        </Link>
      )}
      {chip && (
        <span title={chip.title} className="hidden items-center gap-1.5 min-[1700px]:flex">
          <span className="text-ink-mute">Paper account</span>
          <span className={`font-mono tabular-nums ${TONE[chip.tone]}`}>{chip.text}</span>
        </span>
      )}
      {open != null && (
        <Link href="/intel" title="Stocks currently moving away from what their peers did, not yet resolved" className="flex items-center gap-1.5 hover:text-ink">
          <span className="text-ink-mute">Open situations</span>
          <span className="font-mono tabular-nums text-ink">{open}</span>
        </Link>
      )}
    </span>
  );
}

export default DeskFigures;
