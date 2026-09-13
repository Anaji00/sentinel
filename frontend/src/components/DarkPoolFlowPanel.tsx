'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { formatNumber } from '../lib/format';

interface FlowItem {
  id: string;
  ticker: string;
  type: string; // "SWEEP", "BLOCK", "DARK_POOL"
  side: string; // "BUY", "SELL", "NEUTRAL"
  notional_usd: number;
  price: number;
  size_shares: number;
  occ_symbol?: string;
  occurred_at: string;
}

export default function DarkPoolFlowPanel() {
  const [filterType, setFilterType] = useState<string>('ALL');

  const { data } = useSWR<FlowItem[]>(
    '/api/v1/events/tradfi?limit=25',
    fetcher,
    { refreshInterval: 3000 }
  );

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
  const flows: FlowItem[] = (Array.isArray(data) ? data : []).map((e: any) => {
    const fd = e.financial_data || {};
    const notional = Number(fd.notional_usd ?? fd.premium_usd ?? 0) || 0;
    const price = Number(fd.close_price ?? fd.price ?? 0) || 0;
    const volume = Number(fd.volume ?? 0) || 0;
    return {
      id: e.event_id,
      ticker: fd.ticker || e.primary_entity_name || e.primary_entity_id || '—',
      type: e.type === 'options_flow' ? 'SWEEP' : 'BLOCK',
      side: String(fd.side || fd.direction || 'NEUTRAL').toUpperCase(),
      notional_usd: notional,
      price,
      size_shares: volume,
      occ_symbol: fd.occ_symbol || undefined,
      occurred_at: e.occurred_at,
    };
  });

  const filteredFlows = flows.filter((f) => {
    if (filterType !== 'ALL' && f.type !== filterType) return false;
    return true;
  });

  return (
    <Card className="h-full flex flex-col bg-[#06080e]/90 border border-cyan-500/20 shadow-2xl overflow-hidden font-mono">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-4 border-b border-cyan-500/20 bg-[#090d18]/80 shrink-0">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-lg bg-emerald-950/80 border border-emerald-500/60 flex items-center justify-center shadow-[0_0_12px_rgba(16,185,129,0.3)]">
            <span className="text-emerald-400 text-lg">🐋</span>
          </div>
          <div>
            <h2 className="text-sm font-black text-white tracking-widest uppercase flex items-center gap-2">
              DARK POOL & UNUSUAL OPTIONS FLOW
              <span className="px-2 py-0.5 rounded text-[9px] bg-emerald-500/20 text-emerald-300 border border-emerald-500/40">REAL-TIME SWEEPS</span>
            </h2>
            <p className="text-[10px] text-slate-400">INSTITUTIONAL BLOCK TRADES & OCC OPTION CONTRACT SWEEPS (&gt; $500K)</p>
          </div>
        </div>

        {/* Filter Buttons */}
        <div className="flex items-center gap-1.5 text-[10px]">
          {/* DARK_POOL is not offered: the platform has never produced an
              event of that type, so the filter could only ever return
              nothing. */}
          {['ALL', 'SWEEP', 'BLOCK'].map((t) => (
            <button
              key={t}
              onClick={() => setFilterType(t)}
              className={`px-2.5 py-1 rounded-lg font-bold border transition-all cursor-pointer ${
                filterType === t
                  ? 'bg-cyan-500/20 text-[#00f2fe] border-[#00f2fe]/60'
                  : 'bg-slate-900 text-slate-400 border-slate-800 hover:text-white'
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
            <tr className="text-[10px] text-slate-400 uppercase border-b border-slate-800 pb-2">
              <th className="pb-2">TICKER</th>
              <th className="pb-2">TYPE</th>
              <th className="pb-2">SIDE</th>
              <th className="pb-2">NOTIONAL ($)</th>
              <th className="pb-2">PRICE</th>
              <th className="pb-2">OCC CONTRACT / DETAILS</th>
              <th className="pb-2 text-right">TIME</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60">
            {filteredFlows.map((item) => (
              <tr key={item.id} className="hover:bg-slate-900/60 transition-colors">
                <td className="py-3 font-extrabold text-amber-300">{item.ticker}</td>
                <td>
                  <span className={`px-2 py-0.5 rounded text-[9px] font-bold ${
                    item.type === 'SWEEP' ? 'bg-purple-500/20 text-purple-300 border border-purple-500/40' : 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/40'
                  }`}>
                    {item.type}
                  </span>
                </td>
                <td>
                  <span className={`px-2 py-0.5 rounded text-[9px] font-black ${
                    item.side === 'BUY' ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40' : 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                  }`}>
                    {item.side}
                  </span>
                </td>
                <td className="font-extrabold text-slate-100">
                  ${(item.notional_usd / 1e6).toFixed(2)}M
                </td>
                <td className="text-slate-300">${item.price.toFixed(2)}</td>
                <td className="text-[10px] text-cyan-300/90 font-mono">
                  {item.occ_symbol || `${formatNumber(item.size_shares, { decimals: 0 })} shares block`}
                </td>
                <td className="text-right text-[10px] text-slate-400">
                  {new Date(item.occurred_at).toLocaleTimeString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}
