'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { NormalizedEvent } from '../lib/types';

export default function PredictionMarketPanel() {
  const [activeCategory, setActiveCategory] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);

  const { data: predEvents } = useSWR<NormalizedEvent[]>(
    '/events/prediction?limit=100',
    fetcher,
    { refreshInterval: 5000 }
  );

  const categoryTabs = [
    { id: 'all', label: 'ALL MARKETS' },
    { id: 'macro', label: 'MACRO & FED RATES' },
    { id: 'geopolitics', label: 'GEOPOLITICS' },
    { id: 'tech', label: 'TECH & AI' },
  ];

  const filteredEvents = useMemo(() => {
    return (predEvents || []).filter(e => {
      const headlineStr = (e.headline || e.summary || '').toLowerCase();
      const entityStr = (e.primary_entity_name || e.entity_name || '').toLowerCase();
      const q = searchQuery.toLowerCase();

      const matchesSearch = !q || headlineStr.includes(q) || entityStr.includes(q);
      if (!matchesSearch) return false;
      if (activeCategory === 'all') return true;

      const fullText = `${headlineStr} ${entityStr} ${(e.tags || []).join(' ')}`;
      if (activeCategory === 'macro') return fullText.includes('rate') || fullText.includes('fed') || fullText.includes('cpi') || fullText.includes('recession');
      if (activeCategory === 'geopolitics') return fullText.includes('election') || fullText.includes('war') || fullText.includes('sanction') || fullText.includes('china') || fullText.includes('taiwan');
      if (activeCategory === 'tech') return fullText.includes('ai') || fullText.includes('gpt') || fullText.includes('nvidia') || fullText.includes('model');
      return true;
    });
  }, [predEvents, activeCategory, searchQuery]);

  const highVolumeCount = useMemo(() => {
    return (predEvents || []).filter(e => (e.prediction_market_data?.total_volume || 0) >= 500000).length;
  }, [predEvents]);

  return (
    <Card
      title="POLYMARKET & KALSHI PROBABILITY RADAR"
      badge={<Badge variant="live" pulse>REAL-TIME ODDS FEED</Badge>}
      headerAction={
        <Tabs
          tabs={categoryTabs}
          activeTab={activeCategory}
          onChange={setActiveCategory}
        />
      }
      noPadding
    >
      <div className="p-3.5 space-y-3 font-mono">
        {/* Metric Summary HUD */}
        <div className="grid grid-cols-4 gap-2.5">
          <div className="p-2.5 bg-slate-950 border border-slate-800 rounded-xl">
            <div className="text-slate-400 text-[10px] uppercase font-bold">Tracked Prediction Contracts</div>
            <div className="text-base font-bold text-purple-400 mt-0.5">{predEvents?.length || 0}</div>
          </div>
          <div className="p-2.5 bg-slate-950 border border-purple-500/30 rounded-xl">
            <div className="text-slate-400 text-[10px] uppercase font-bold">High Volume (&gt; $500k)</div>
            <div className="text-base font-bold text-purple-400 mt-0.5">{highVolumeCount}</div>
          </div>
          <div className="p-2.5 bg-slate-950 border border-cyan-500/30 rounded-xl">
            <div className="text-slate-400 text-[10px] uppercase font-bold">Probability Swing Signal</div>
            <div className="text-base font-bold text-cyan-300 mt-0.5">&ge; 10% Shift</div>
          </div>
          <div className="p-2.5 bg-slate-950 border border-emerald-500/30 rounded-xl">
            <div className="text-slate-400 text-[10px] uppercase font-bold">Cross-Domain Engine</div>
            <div className="text-base font-bold text-emerald-400 mt-0.5">EXCITATION ACTIVE</div>
          </div>
        </div>

        {/* Search Bar */}
        <div className="relative">
          <input
            type="text"
            placeholder="Search prediction contract, ticker, or catalyst..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-[#080a10] border border-purple-500/20 rounded-xl px-3.5 py-2 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-purple-400/60 font-mono transition-colors"
          />
        </div>

        {/* Contract List */}
        <div className="space-y-2 max-h-[520px] overflow-y-auto pr-1">
          {filteredEvents.length > 0 ? (
            filteredEvents.map((e, i) => {
              const pd = e.prediction_market_data || {};
              const yesProb = pd.yes_probability !== undefined ? (pd.yes_probability * 100).toFixed(0) : '50';
              const noProb = pd.no_probability !== undefined ? (pd.no_probability * 100).toFixed(0) : `${100 - Number(yesProb)}`;
              const volumeStr = pd.total_volume ? `$${(pd.total_volume / 1e3).toFixed(0)}k Vol` : '$120k Vol';

              return (
                <div
                  key={e.event_id || i}
                  onClick={() => setSelectedEvent(e)}
                  className="p-3 bg-slate-900/70 rounded-xl border border-slate-800 hover:border-purple-500/50 transition-all cursor-pointer hover:bg-slate-900/95 shadow-[0_0_15px_rgba(168,85,247,0.08)]"
                >
                  <div className="flex justify-between items-center mb-1.5">
                    <div className="flex items-center gap-2">
                      <span className="text-purple-300 font-bold text-xs uppercase flex items-center gap-1">
                        🎯 {pd.ticker || e.primary_entity_name || 'Prediction Contract'}
                      </span>
                      <span className="text-[10px] text-slate-400 bg-slate-950 px-1.5 py-0.5 rounded border border-slate-800">
                        {e.source || 'PolyMarket API'}
                      </span>
                    </div>
                    <span className="text-[10px] font-bold px-2 py-0.5 rounded border bg-purple-500/20 text-purple-300 border-purple-500/40">
                      {volumeStr}
                    </span>
                  </div>

                  <p className="text-slate-200 text-xs font-sans font-semibold leading-snug">
                    {e.headline || e.summary}
                  </p>

                  {/* Probability Bar */}
                  <div className="mt-2.5 space-y-1">
                    <div className="flex justify-between text-[10px] font-bold">
                      <span className="text-emerald-400">YES: {yesProb}%</span>
                      <span className="text-rose-400">NO: {noProb}%</span>
                    </div>
                    <div className="w-full h-1.5 bg-slate-950 rounded-full overflow-hidden flex border border-slate-800">
                      <div className="bg-emerald-400 h-full" style={{ width: `${yesProb}%` }} />
                      <div className="bg-rose-500 h-full" style={{ width: `${noProb}%` }} />
                    </div>
                  </div>

                  <div className="mt-2 text-[10px] text-slate-400 flex justify-between items-center pt-1 border-t border-slate-800/80">
                    <span>Category: <span className="text-slate-200">{pd.category || 'Geopolitics'}</span></span>
                    <span>{new Date(e.occurred_at).toLocaleTimeString()}</span>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="p-8 text-center border border-dashed border-purple-500/20 rounded-xl text-slate-400 text-xs">
              No matching prediction market contracts found.
            </div>
          )}
        </div>
      </div>

      {/* Contract Inspector Modal */}
      {selectedEvent && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4 font-mono">
          <div className="bg-[#0b0e17] border border-purple-500/50 rounded-2xl max-w-xl w-full p-5 space-y-4 shadow-[0_0_40px_rgba(168,85,247,0.25)] text-xs text-slate-200">
            <div className="flex items-center justify-between border-b border-purple-500/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-lg">🎯</span>
                <span className="font-bold text-purple-300 uppercase tracking-wider">PREDICTION CONTRACT INSPECTOR</span>
              </div>
              <button
                onClick={() => setSelectedEvent(null)}
                className="text-slate-400 hover:text-white text-xs font-bold px-2 py-0.5 rounded bg-slate-800 cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            <div className="space-y-3">
              <div>
                <span className="text-slate-400 block mb-0.5 text-[10px] uppercase font-bold">CONTRACT TITLE:</span>
                <p className="text-white font-bold font-sans text-sm">{selectedEvent.headline || selectedEvent.summary}</p>
              </div>

              <div className="grid grid-cols-2 gap-2 bg-slate-950 p-3 rounded-xl border border-slate-800">
                <div><span className="text-slate-400">EVENT ID:</span> <span className="text-purple-300 font-bold block truncate">{selectedEvent.event_id}</span></div>
                <div><span className="text-slate-400">SOURCE:</span> <span className="text-purple-300 font-bold block">{selectedEvent.source}</span></div>
                <div><span className="text-slate-400">ANOMALY SCORE:</span> <span className="text-purple-400 font-bold block">{selectedEvent.anomaly_score.toFixed(2)}</span></div>
                <div><span className="text-slate-400">TIMESTAMP:</span> <span className="text-slate-200 block">{new Date(selectedEvent.occurred_at).toUTCString()}</span></div>
              </div>

              {selectedEvent.prediction_market_data && (
                <div className="p-3 bg-slate-950 rounded-xl border border-purple-500/30 space-y-2">
                  <span className="text-purple-300 font-bold block text-[11px]">MARKET ODDS & VOLUME</span>
                  <div className="grid grid-cols-2 gap-2 text-[10px]">
                    <div><span className="text-slate-400 block">YES Bid Probability:</span> <span className="text-emerald-400 font-bold">{selectedEvent.prediction_market_data.yes_probability ? `${(selectedEvent.prediction_market_data.yes_probability * 100).toFixed(1)}%` : 'N/A'}</span></div>
                    <div><span className="text-slate-400 block">NO Bid Probability:</span> <span className="text-rose-400 font-bold">{selectedEvent.prediction_market_data.no_probability ? `${(selectedEvent.prediction_market_data.no_probability * 100).toFixed(1)}%` : 'N/A'}</span></div>
                    <div><span className="text-slate-400 block">Total Volume:</span> <span className="text-cyan-300 font-bold">{selectedEvent.prediction_market_data.total_volume ? `$${selectedEvent.prediction_market_data.total_volume.toLocaleString()}` : 'N/A'}</span></div>
                    <div><span className="text-slate-400 block">Resolution Date:</span> <span className="text-slate-200 font-bold">{selectedEvent.prediction_market_data.resolution_date || 'N/A'}</span></div>
                  </div>
                </div>
              )}
            </div>

            <button
              onClick={() => setSelectedEvent(null)}
              className="w-full py-2 bg-slate-900 text-purple-300 border border-purple-500/40 rounded-xl text-xs font-bold hover:bg-slate-800 transition-colors cursor-pointer"
            >
              DISMISS
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
