'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { NormalizedEvent } from '../lib/types';

export default function CryptoAnalytics() {
  const [activeChain, setActiveChain] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);

  const { data: cryptoEvents } = useSWR<NormalizedEvent[]>(
    '/events/crypto?limit=100',
    fetcher,
    { refreshInterval: 6000 }
  );

  const chainTabs = [
    { id: 'all', label: 'ALL CHAINS' },
    { id: 'btc', label: 'BITCOIN' },
    { id: 'eth', label: 'ETHEREUM' },
    { id: 'sol', label: 'SOLANA' },
    { id: 'dex', label: 'DEX SWAPS' },
  ];

  const filteredEvents = useMemo(() => {
    return (cryptoEvents || []).filter(e => {
      const entityStr = (e.primary_entity_name || e.entity_name || '').toLowerCase();
      const headlineStr = (e.headline || e.summary || '').toLowerCase();
      const sourceStr = (e.source || '').toLowerCase();
      const q = searchQuery.toLowerCase();

      const matchesSearch = !q || entityStr.includes(q) || headlineStr.includes(q) || sourceStr.includes(q);

      if (!matchesSearch) return false;
      if (activeChain === 'all') return true;

      const fullText = `${entityStr} ${headlineStr} ${sourceStr} ${(e.type || '')}`;
      if (activeChain === 'btc') return fullText.includes('btc') || fullText.includes('bitcoin');
      if (activeChain === 'eth') return fullText.includes('eth') || fullText.includes('ethereum') || fullText.includes('erc20');
      if (activeChain === 'sol') return fullText.includes('sol') || fullText.includes('solana');
      if (activeChain === 'dex') return fullText.includes('dex') || fullText.includes('swap') || fullText.includes('uniswap') || fullText.includes('liquidity');
      return true;
    });
  }, [cryptoEvents, activeChain, searchQuery]);

  const highSeverityCount = useMemo(() => {
    return (cryptoEvents || []).filter(e => e.anomaly_score >= 0.75).length;
  }, [cryptoEvents]);

  return (
    <Card
      title="DECENTRALIZED ASSET & DEX ANALYTICS"
      badge={<Badge variant="live" pulse>LIVE CHAIN SYNC</Badge>}
      headerAction={
        <Tabs
          tabs={chainTabs}
          activeTab={activeChain}
          onChange={setActiveChain}
        />
      }
      noPadding
    >
      <div className="p-3.5 space-y-3 font-mono">
        {/* Metric Summary HUD */}
        <div className="grid grid-cols-3 gap-3">
          <div className="p-2.5 bg-slate-950 border border-slate-800 rounded-lg">
            <div className="text-slate-400 text-[10px] uppercase font-bold">Total On-Chain Events</div>
            <div className="text-lg font-bold text-amber-400 mt-0.5">{cryptoEvents?.length || 0}</div>
          </div>
          <div className="p-2.5 bg-slate-950 border border-rose-500/30 rounded-lg">
            <div className="text-slate-400 text-[10px] uppercase font-bold">High Anomaly (Score &ge; 0.75)</div>
            <div className="text-lg font-bold text-rose-400 mt-0.5">{highSeverityCount}</div>
          </div>
          <div className="p-2.5 bg-slate-950 border border-cyan-500/30 rounded-lg">
            <div className="text-slate-400 text-[10px] uppercase font-bold">Whale Inflow Threshold</div>
            <div className="text-lg font-bold text-[#00f2fe] mt-0.5">&gt; $500,000 USD</div>
          </div>
        </div>

        {/* Search Bar */}
        <div className="relative">
          <input
            type="text"
            placeholder="Filter on-chain events by asset, hash, wallet, or exchange..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-[#080a10] border border-amber-500/20 rounded-lg px-3 py-1.5 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-amber-400/60 font-mono transition-colors"
          />
        </div>

        {/* On-Chain Events Stream */}
        <div className="space-y-2 max-h-[520px] overflow-y-auto pr-1">
          {filteredEvents.length > 0 ? (
            filteredEvents.map((e, i) => {
              const isHighSeverity = e.anomaly_score >= 0.75;
              return (
                <div
                  key={e.event_id || i}
                  onClick={() => setSelectedEvent(e)}
                  className={`p-3 bg-slate-900/70 rounded-lg border transition-all cursor-pointer hover:bg-slate-900/95 ${
                    isHighSeverity
                      ? 'border-rose-500/40 hover:border-rose-400 glow-crimson'
                      : 'border-amber-500/20 hover:border-amber-400/60'
                  }`}
                >
                  <div className="flex justify-between items-center mb-1.5">
                    <div className="flex items-center gap-2">
                      <span className="text-amber-300 font-bold text-xs uppercase">
                        ₿ {e.primary_entity_name || e.entity_name || 'Crypto Asset'}
                      </span>
                      <span className="text-[10px] text-slate-400 bg-slate-800 px-1.5 py-0.5 rounded border border-slate-700">
                        {e.source || 'CoinGecko On-Chain'}
                      </span>
                    </div>
                    <span className={`text-[10px] font-bold px-2 py-0.5 rounded border ${
                      isHighSeverity
                        ? 'bg-rose-500/20 text-rose-400 border-rose-500/40 glow-crimson'
                        : 'bg-amber-500/20 text-amber-300 border-amber-500/40'
                    }`}>
                      SCORE: {e.anomaly_score.toFixed(2)}
                    </span>
                  </div>

                  <p className="text-slate-200 text-xs font-sans font-semibold leading-snug line-clamp-2">
                    {e.headline || e.summary || `Large on-chain crypto movement for ${e.primary_entity_name || 'Asset'}`}
                  </p>

                  <div className="mt-2 text-[10px] text-slate-400 flex justify-between items-center pt-1 border-t border-slate-800/80">
                    <span>Region: <span className="text-slate-300">{e.region || 'Global DEX'}</span></span>
                    <span>{new Date(e.occurred_at).toLocaleTimeString()}</span>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="p-8 text-center border border-dashed border-amber-500/20 rounded-lg text-slate-400 text-xs">
              No matching on-chain crypto events found.
            </div>
          )}
        </div>
      </div>

      {/* On-Chain Event Inspector Modal */}
      {selectedEvent && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4 font-mono">
          <div className="bg-[#0b0e17] border border-amber-500/50 rounded-xl max-w-lg w-full p-5 space-y-4 shadow-[0_0_35px_rgba(245,158,11,0.25)] text-xs text-slate-200">
            <div className="flex items-center justify-between border-b border-amber-500/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-lg">₿</span>
                <span className="font-bold text-amber-300 uppercase tracking-wider">ON-CHAIN EVENT INSPECTOR</span>
              </div>
              <button
                onClick={() => setSelectedEvent(null)}
                className="text-slate-400 hover:text-white text-xs font-bold px-2 py-0.5 rounded bg-slate-800 cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            <div className="space-y-2.5">
              <div>
                <span className="text-slate-400 block mb-0.5">EVENT HEADLINE:</span>
                <p className="text-white font-bold font-sans text-sm">{selectedEvent.headline || selectedEvent.summary}</p>
              </div>

              <div className="grid grid-cols-2 gap-2 bg-slate-950 p-3 rounded-lg border border-slate-800">
                <div><span className="text-slate-400">EVENT ID:</span> <span className="text-amber-300 font-bold block truncate">{selectedEvent.event_id}</span></div>
                <div><span className="text-slate-400">SOURCE:</span> <span className="text-amber-300 font-bold block">{selectedEvent.source}</span></div>
                <div><span className="text-slate-400">ANOMALY SCORE:</span> <span className="text-rose-400 font-bold block">{selectedEvent.anomaly_score.toFixed(2)}</span></div>
                <div><span className="text-slate-400">TIMESTAMP:</span> <span className="text-slate-200 block">{new Date(selectedEvent.occurred_at).toUTCString()}</span></div>
              </div>

              <div>
                <span className="text-amber-400 font-bold block mb-1">ON-CHAIN PAYLOAD DATA:</span>
                <pre className="p-3 bg-slate-950 rounded border border-slate-800 text-[10px] text-amber-200 overflow-x-auto max-h-48">
                  {JSON.stringify(selectedEvent.domain_data || selectedEvent.raw_payload || selectedEvent, null, 2)}
                </pre>
              </div>
            </div>

            <button
              onClick={() => setSelectedEvent(null)}
              className="w-full py-2 bg-slate-900 text-amber-300 border border-amber-500/40 rounded-lg text-xs font-bold hover:bg-slate-800 transition-colors cursor-pointer"
            >
              DISMISS
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
