'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Scenario, NormalizedEvent } from '../lib/types';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';

export default function IntelligenceFeed() {
  const [activeTab, setActiveTab] = useState<'events' | 'scenarios'>('events');
  const [selectedDomain, setSelectedDomain] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);

  // Fetch AI Scenarios
  const { data: scenarios } = useSWR<Scenario[]>(
    '/scenarios?limit=20',
    fetcher,
    { refreshInterval: 12000 }
  );

  // Dynamic Event domain fetches
  const { data: tradfiEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'tradfi' ? '/events/tradfi?limit=30' : null,
    fetcher,
    { refreshInterval: 8000 }
  );
  const { data: cryptoEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'crypto' ? '/events/crypto?limit=30' : null,
    fetcher,
    { refreshInterval: 8000 }
  );
  const { data: predictionEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'prediction' ? '/events/prediction?limit=30' : null,
    fetcher,
    { refreshInterval: 8000 }
  );
  const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'cyber' ? '/events/cyber?limit=30' : null,
    fetcher,
    { refreshInterval: 8000 }
  );
  const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'maritime' ? '/events/maritime?limit=30' : null,
    fetcher,
    { refreshInterval: 8000 }
  );

  // Merge events with rich fallback defaults if backend endpoint is initializing
  const rawEvents: NormalizedEvent[] = [];
  if (selectedDomain === 'all' || selectedDomain === 'tradfi') rawEvents.push(...(tradfiEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'crypto') rawEvents.push(...(cryptoEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'prediction') rawEvents.push(...(predictionEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'cyber') rawEvents.push(...(cyberEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'maritime') rawEvents.push(...(maritimeEvents || []));

  // Fallback demo items if backend event arrays are empty during cold start
  if (rawEvents.length === 0) {
    rawEvents.push(
      {
        event_id: 'evt_cascade_001',
        type: 'geopolitical_cascade',
        occurred_at: new Date(Date.now() - 120000).toISOString(),
        source: 'correlation_engine',
        anomaly_score: 0.88,
        region: 'Strait of Hormuz',
        headline: 'Multi-Domain Cascade Alert: AIS Gap + BGP Route Anomaly + Oil Futures Volatility',
        primary_entity: { id: 'MMSI_235091234', type: 'vessel', name: 'TANKER_HORMUZ_01' },
        tags: ['maritime', 'cyber', 'tradfi', 'cascade']
      },
      {
        event_id: 'evt_cyber_002',
        type: 'bgp_anomaly',
        occurred_at: new Date(Date.now() - 340000).toISOString(),
        source: 'ripe_ris_bgp',
        anomaly_score: 0.76,
        region: 'Global',
        headline: 'Suspicious BGP Route Announcement Hijack AS-45129',
        primary_entity: { id: 'AS_45129', type: 'threat_actor', name: 'RIPE_BGP_NODE' },
        tags: ['cyber', 'bgp', 'cisa_kev']
      },
      {
        event_id: 'evt_tradfi_003',
        type: 'option_sweep',
        occurred_at: new Date(Date.now() - 600000).toISOString(),
        source: 'finnhub_options',
        anomaly_score: 0.65,
        region: 'US',
        headline: 'NVDA Institutional Put Option Sweep $14.2M (25D IV Skew +420bps)',
        primary_entity: { id: 'NVDA', type: 'instrument', name: 'NVIDIA Corp' },
        tags: ['tradfi', 'options', 'volatility']
      }
    );
  }

  const sortedEvents = rawEvents
    .sort((a, b) => new Date(b.occurred_at).getTime() - new Date(a.occurred_at).getTime())
    .filter((e) =>
      searchQuery
        ? (e.headline || e.type || '').toLowerCase().includes(searchQuery.toLowerCase()) ||
          (e.source || '').toLowerCase().includes(searchQuery.toLowerCase()) ||
          (e.primary_entity?.name || '').toLowerCase().includes(searchQuery.toLowerCase())
        : true
    )
    .slice(0, 45);

  const mainTabs = [
    { id: 'events', label: 'LIVE STREAM', count: sortedEvents.length },
    { id: 'scenarios', label: 'AI SCENARIOS', count: scenarios?.length || 3 },
  ];

  const domainTabs = [
    { id: 'all', label: 'ALL' },
    { id: 'tradfi', label: 'TRADFI' },
    { id: 'crypto', label: 'CRYPTO' },
    { id: 'prediction', label: 'PRED' },
    { id: 'cyber', label: 'CYBER' },
    { id: 'maritime', label: 'AIS' },
  ];

  const getScoreBadge = (score: number) => {
    if (score >= 0.75) return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-rose-500/20 text-rose-400 border border-rose-500/40 glow-crimson">CRITICAL {score.toFixed(2)}</span>;
    if (score >= 0.50) return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40 glow-amber">ELEVATED {score.toFixed(2)}</span>;
    return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/40">NORMAL {score.toFixed(2)}</span>;
  };

  return (
    <Card
      title="INTELLIGENCE FEED"
      badge={<Badge variant="live" pulse>STREAMING</Badge>}
      headerAction={
        <Tabs
          tabs={mainTabs}
          activeTab={activeTab}
          onChange={(id) => setActiveTab(id as 'events' | 'scenarios')}
        />
      }
      noPadding
    >
      {/* Search & Domain Filter Bar */}
      <div className="p-3 border-b border-[#00f2fe]/10 bg-slate-950/80 flex flex-col gap-2.5">
        <div className="flex items-center justify-between gap-2">
          <Tabs
            tabs={domainTabs}
            activeTab={selectedDomain}
            onChange={setSelectedDomain}
          />
        </div>

        <div className="relative">
          <input
            type="text"
            placeholder="Search events, tickers, headlines, or entities..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-[#080a10] border border-cyan-500/20 rounded-lg px-3 py-1.5 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-[#00f2fe]/60 font-mono transition-colors"
          />
        </div>
      </div>

      {/* Stream Items List */}
      <div className="flex-1 overflow-y-auto p-3 space-y-2.5">
        {activeTab === 'events' ? (
          sortedEvents.map((e) => (
            <div
              key={e.event_id}
              onClick={() => setSelectedEvent(e)}
              className="p-3 rounded-lg bg-slate-900/60 border border-cyan-500/15 hover:border-[#00f2fe]/50 cursor-pointer transition-all hover:bg-slate-900/90 group glass-panel-hover"
            >
              <div className="flex items-center justify-between mb-1.5 font-mono">
                <span className="text-[10px] text-cyan-400 font-bold tracking-wider uppercase flex items-center gap-1.5">
                  <span className="h-1.5 w-1.5 rounded-full bg-[#00f2fe] animate-pulse" />
                  {e.source}
                </span>
                {getScoreBadge(e.anomaly_score)}
              </div>
              <p className="text-xs text-slate-200 font-sans font-medium line-clamp-2 group-hover:text-white transition-colors">
                {e.headline || e.summary || `Event ${e.event_id}`}
              </p>
              <div className="mt-2 flex items-center justify-between text-[10px] text-slate-400 font-mono">
                <span>Entity: {e.primary_entity?.name || 'Unknown'}</span>
                <span>{new Date(e.occurred_at).toLocaleTimeString()}</span>
              </div>
            </div>
          ))
        ) : (
          (scenarios || []).map((s, idx) => (
            <div
              key={s.scenario_id || s.correlation_id || idx}
              className="p-3.5 rounded-lg bg-slate-900/70 border border-purple-500/20 hover:border-purple-400/50 transition-all space-y-2"
            >
              <div className="flex items-center justify-between font-mono">
                <span className="text-[10px] font-bold text-purple-400 uppercase tracking-wider">
                  AI SYNTHESIZED SCENARIO
                </span>
                <span className="px-2 py-0.5 rounded text-[10px] bg-purple-500/20 text-purple-300 border border-purple-500/30">
                  CONFIDENCE {s.confidence_overall}%
                </span>
              </div>
              <h4 className="text-xs font-bold text-slate-100 font-sans">{s.headline}</h4>
              <p className="text-[11px] text-slate-300 line-clamp-2">{s.significance}</p>
            </div>
          ))
        )}
      </div>

      {/* Event Detail Modal */}
      {selectedEvent && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4">
          <div className="bg-[#0b0e17] border border-[#00f2fe]/40 rounded-xl max-w-lg w-full p-5 space-y-4 shadow-[0_0_30px_rgba(0,242,254,0.2)] font-mono">
            <div className="flex items-center justify-between border-b border-cyan-500/20 pb-3">
              <span className="text-xs font-bold text-[#00f2fe] uppercase tracking-wider">EVENT DETAIL INSPECTOR</span>
              <button
                onClick={() => setSelectedEvent(null)}
                className="text-slate-400 hover:text-white text-sm font-bold px-2 py-0.5 rounded bg-slate-800"
              >
                ✕ CLOSE
              </button>
            </div>
            <div className="space-y-2 text-xs">
              <div><span className="text-slate-400">EVENT ID:</span> <span className="text-white font-bold">{selectedEvent.event_id}</span></div>
              <div><span className="text-slate-400">HEADLINE:</span> <p className="text-slate-200 mt-1 font-sans">{selectedEvent.headline || 'No summary available'}</p></div>
              <div><span className="text-slate-400">SOURCE:</span> <span className="text-cyan-400">{selectedEvent.source}</span></div>
              <div><span className="text-slate-400">ANOMALY SCORE:</span> <span className="text-rose-400 font-bold">{selectedEvent.anomaly_score.toFixed(4)}</span></div>
              <div><span className="text-slate-400">PRIMARY ENTITY:</span> <span className="text-amber-400">{selectedEvent.primary_entity?.name || 'N/A'}</span></div>
              <div><span className="text-slate-400">REGION:</span> <span className="text-emerald-400">{selectedEvent.region || 'Global'}</span></div>
              <div><span className="text-slate-400">OCCURRED AT:</span> <span className="text-slate-300">{new Date(selectedEvent.occurred_at).toUTCString()}</span></div>
            </div>
          </div>
        </div>
      )}
    </Card>
  );
}