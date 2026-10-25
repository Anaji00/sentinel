'use client';

import Link from 'next/link';
import React, { useState } from 'react';
import useSWR from 'swr';
import type { NormalizedEvent } from '../lib/types';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { formatNumber } from '../lib/format';
import { IconFlow } from './ui/icons';
import { ClockTime } from './ui/ClockTime';
import { POLL } from './ui/DataProvider';

interface FlowItem {
  id: string;
  ticker: string;
  type: string; // "SWEEP", "BLOCK", "DARK_POOL"
  side: string; // "BUY", "SELL", "NEUTRAL"
  notional_usd: number;
  price: number;
  size_shares: number;
  occ_symbol?: string;
  /** Option sweeps only: "$400 C 2026-11-20". */
  contract?: string;
  occurred_at: string;
}

/** The financial fields a block or sweep event carries, as the collectors write them. */
interface RawFlowFinancials {
  ticker?: string;
  premium_usd?: number | string | null;
  notional_usd?: number | string | null;
  strike?: number | string | null;
  option_type?: string;
  close_price?: number | string | null;
  price?: number | string | null;
  volume?: number | string | null;
  side?: string;
  direction?: string;
  occ_symbol?: string;
  expiry?: string;
}

export default function DarkPoolFlowPanel() {
  const [filterType, setFilterType] = useState<string>('ALL');

  const { data } = useSWR<NormalizedEvent[]>('/api/v1/events/tradfi?limit=40&types=equity_block,options_flow', fetcher, {
    refreshInterval: POLL.live,
  });

  // The endpoint returns a bare JSON array, so `data?.events` was
  // undefined on every successful fetch and the `||` fired on the
  // normal path rather than on an outage -- permanently, on a refresh
  // timer. What it fell back to was not placeholder text but invented
  // records stamped `occurred_at: new Date()`, which is what turns a
  // fixture into a claim: a row dated now, with a source attribution
  // and an anomaly score, is indistinguishable from a live detection.
  //
  // An empty list is the honest answer when there is nothing to show.
  //
  // The endpoint serves NormalizedEvent rows, not FlowItem rows, so the shape
  // is mapped explicitly rather than assumed. The event types this panel is
  // about are the ones the platform actually emits: `options_flow` and
  // `equity_block`. It previously offered a DARK_POOL filter, and no event of
  // that type has ever been produced -- the enum declares it, the scorer
  // handles it, and nothing emits it -- so the filter could only ever have
  // matched the fabricated rows above it.
  // The same print can arrive twice (two feeds, one trade); show it once.
  const seen = new Set<string>();
  const flows: FlowItem[] = (Array.isArray(data) ? data : []).map((e) => {
    const fd = (e.financial_data || {}) as RawFlowFinancials;
    const isOption = e.type === 'options_flow';
    // For an option, notional_usd is exposure (strike x 100 x contracts) -- a
    // $120K sweep read as $10M. The premium is the money that changed hands.
    const notional = Number(isOption ? (fd.premium_usd ?? 0) : (fd.notional_usd ?? fd.premium_usd ?? 0)) || 0;
    const strike = Number(fd.strike);
    const right = String(fd.option_type || '').toUpperCase().startsWith('P') ? 'P' : 'C';
    const price = Number(fd.close_price ?? fd.price ?? 0) || 0;
    const volume = Number(fd.volume ?? 0) || 0;
    return {
      id: e.event_id,
      ticker: fd.ticker || e.primary_entity_name || e.primary_entity_id || '—',
      type: e.type === 'options_flow' ? 'SWEEP' : 'BLOCK',
      // No feed here reports the aggressor, so an unknown side says so.
      side: String(fd.side || fd.direction || '—').toUpperCase(),
      notional_usd: notional,
      price,
      size_shares: volume,
      occ_symbol: fd.occ_symbol || undefined,
      contract:
        isOption && Number.isFinite(strike) && strike > 0
          ? `$${strike} ${right}${fd.expiry ? ` ${fd.expiry}` : ''}`
          : undefined,
      occurred_at: e.occurred_at,
    };
  });

  const filteredFlows = flows.filter((f) => {
    const key = `${f.ticker}|${f.occurred_at}|${f.size_shares}|${f.notional_usd}`;
    if (seen.has(key)) return false;
    seen.add(key);
    if (filterType !== 'ALL' && f.type !== filterType) return false;
    return true;
  });

  return (
    <Card className="h-full flex flex-col overflow-hidden">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-4 border-b border-line shrink-0">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-lg bg-positive/10 border border-positive/30 flex items-center justify-center shadow-panel">
            <IconFlow className="text-positive" size={18} />
          </div>
          <div>
            <h2 className="text-sm font-semibold text-ink flex items-center gap-2">
              Big trades as they print
              <span className="px-2 py-0.5 rounded text-micro bg-positive/10 text-positive border border-positive/30">
                Live
              </span>
            </h2>
            <p className="text-micro text-ink-dim">
              Stock trades of $200K or more, well above the stock&apos;s usual size, and big options bets placed across
              several exchanges at once
            </p>
          </div>
        </div>

        {/* Filter Buttons */}
        <div className="flex items-center gap-1.5 text-micro">
          {/* DARK_POOL is not offered: the platform has never produced an
              event of that type, so the filter could only ever return
              nothing. */}
          {(
            [
              ['ALL', 'Everything'],
              ['SWEEP', 'Options bets'],
              ['BLOCK', 'Block trades'],
            ] as const
          ).map(([t, label]) => (
            <button
              key={t}
              onClick={() => setFilterType(t)}
              aria-pressed={filterType === t}
              className={`px-2.5 py-1 rounded-lg border transition-colors cursor-pointer ${
                filterType === t
                  ? 'bg-overlay text-ink font-semibold border-line-strong'
                  : 'text-ink-dim border-line hover:text-ink'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* Table List */}
      <div className="flex-1 overflow-y-auto p-4">
        <table className="w-full text-left text-xs border-collapse">
          <thead>
            <tr className="text-micro text-ink-mute border-b border-line">
              <th className="pb-2 font-normal">Stock</th>
              <th className="pb-2 font-normal">Kind</th>
              <th className="pb-2 font-normal">Side</th>
              <th className="pb-2 font-normal" title="Shares x price for blocks; premium paid for options bets">Size</th>
              <th className="pb-2 font-normal">Price</th>
              <th className="pb-2 font-normal">Details</th>
              <th className="pb-2 font-normal text-right">Time</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-line/60">
            {filteredFlows.map((item) => (
              <tr key={item.id} className="hover:bg-raised/60 transition-colors">
                <td className="py-2.5">
                  <Link href={`/ticker?symbol=${encodeURIComponent(item.ticker)}`} className="font-mono font-semibold text-ink hover:text-accent">
                    {item.ticker}
                  </Link>
                </td>
                <td>
                  <span
                    className={`px-2 py-0.5 rounded-full text-micro ${
                      item.type === 'SWEEP' ? 'bg-accent-dim text-accent' : 'bg-inset text-ink-dim border border-line'
                    }`}
                  >
                    {item.type === 'SWEEP' ? 'Options bet' : 'Block trade'}
                  </span>
                </td>
                <td>
                  <span
                    className={`px-2 py-0.5 rounded-full text-micro font-semibold ${
                      item.side === 'BUY'
                        ? 'bg-positive/10 text-positive border border-positive/30'
                        : item.side === 'SELL'
                          ? 'bg-negative/10 text-negative border border-negative/30'
                          : 'border border-line text-ink-mute'
                    }`}
                  >
                    {item.side}
                  </span>
                </td>
                <td className="font-mono font-semibold text-ink">
                  {item.notional_usd >= 1e6
                    ? `$${(item.notional_usd / 1e6).toFixed(2)}M`
                    : `$${Math.round(item.notional_usd / 1e3)}K`}
                </td>
                <td className="font-mono text-ink-dim">${item.price.toFixed(2)}</td>
                <td className="text-micro text-ink-dim">
                  {item.type === 'SWEEP'
                    ? `${formatNumber(item.size_shares, { decimals: 0 })} contracts${item.contract ? ` · ${item.contract}` : ''}`
                    : item.occ_symbol ||
                      `${formatNumber(item.size_shares, { decimals: 0 })} shares block`}
                </td>
                <td className="text-right text-micro text-ink-dim">
                  <ClockTime value={item.occurred_at} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}
