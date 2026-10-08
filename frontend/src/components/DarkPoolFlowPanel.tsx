'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
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

export default function DarkPoolFlowPanel() {
  const [filterType, setFilterType] = useState<string>('ALL');

  const { data } = useSWR<FlowItem[]>('/api/v1/events/tradfi?limit=40&types=equity_block,options_flow', fetcher, {
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
  const flows: FlowItem[] = (Array.isArray(data) ? data : []).map((e: any) => {
    const fd = e.financial_data || {};
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
    <Card className="h-full flex flex-col bg-[#06080e]/90 border border-cyan-500/20 shadow-2xl overflow-hidden font-mono">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-4 border-b border-cyan-500/20 bg-[#090d18]/80 shrink-0">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-lg bg-emerald-950/80 border border-emerald-500/60 flex items-center justify-center shadow-panel">
            <IconFlow className="text-emerald-400" size={18} />
          </div>
          <div>
            <h2 className="text-sm font-semibold text-white flex items-center gap-2">
              Block trades and options sweeps
              <span className="px-2 py-0.5 rounded text-micro bg-emerald-500/20 text-emerald-300 border border-emerald-500/40">
                Live
              </span>
            </h2>
            <p className="text-micro text-ink-dim">
              Prints large for their instrument (at least $200K and well above its usual size) and option sweeps
            </p>
          </div>
        </div>

        {/* Filter Buttons */}
        <div className="flex items-center gap-1.5 text-micro">
          {/* DARK_POOL is not offered: the platform has never produced an
              event of that type, so the filter could only ever return
              nothing. */}
          {['ALL', 'SWEEP', 'BLOCK'].map((t) => (
            <button
              key={t}
              onClick={() => setFilterType(t)}
              className={`px-2.5 py-1 rounded-lg font-bold border transition-all cursor-pointer ${
                filterType === t
                  ? 'bg-cyan-500/20 text-accent border-accent/60'
                  : 'bg-raised text-ink-dim border-line hover:text-white'
              }`}
            >
              {t.replace('_', ' ')}
            </button>
          ))}
        </div>
      </div>

      {/* Table List */}
      <div className="flex-1 overflow-y-auto p-4">
        <table className="w-full text-left text-xs border-collapse">
          <thead>
            <tr className="text-micro text-ink-dim uppercase border-b border-line pb-2">
              <th className="pb-2">TICKER</th>
              <th className="pb-2">TYPE</th>
              <th className="pb-2">SIDE</th>
              <th className="pb-2" title="Shares x price for blocks; premium paid for option sweeps">$ TRADED</th>
              <th className="pb-2">PRICE</th>
              <th className="pb-2">OCC CONTRACT / DETAILS</th>
              <th className="pb-2 text-right">TIME</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-line/60">
            {filteredFlows.map((item) => (
              <tr key={item.id} className="hover:bg-raised/60 transition-colors">
                <td className="py-3 font-extrabold text-amber-300">{item.ticker}</td>
                <td>
                  <span
                    className={`px-2 py-0.5 rounded text-micro font-bold ${
                      item.type === 'SWEEP'
                        ? 'bg-purple-500/20 text-purple-300 border border-purple-500/40'
                        : 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/40'
                    }`}
                  >
                    {item.type}
                  </span>
                </td>
                <td>
                  <span
                    className={`px-2 py-0.5 rounded text-micro font-black ${
                      item.side === 'BUY'
                        ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40'
                        : item.side === 'SELL'
                          ? 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                          : 'border border-line text-ink-mute'
                    }`}
                  >
                    {item.side}
                  </span>
                </td>
                <td className="font-extrabold text-ink">
                  {item.notional_usd >= 1e6
                    ? `$${(item.notional_usd / 1e6).toFixed(2)}M`
                    : `$${Math.round(item.notional_usd / 1e3)}K`}
                </td>
                <td className="text-ink-dim">${item.price.toFixed(2)}</td>
                <td className="text-micro text-cyan-300/90 font-mono">
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
