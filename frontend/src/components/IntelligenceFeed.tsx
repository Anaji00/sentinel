'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { useLiveEvents } from '../lib/useLiveEvents';
import { NormalizedEvent, Scenario } from '../lib/types';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';

// Helper to derive clean domain tag + icon
function getDomainMeta(type: string): { label: string; icon: string; badgeStyle: string } {
    const t = (type || '').toLowerCase();
    if (t.includes('vessel') || t.includes('maritime') || t.includes('ais')) {
        return { label: 'MARITIME', icon: '🚢', badgeStyle: 'text-cyan-400 border-cyan-500/40 bg-cyan-500/10' };
    }
    if (t.includes('cyber') || t.includes('bgp') || t.includes('breach')) {
        return { label: 'CYBER', icon: '🔐', badgeStyle: 'text-rose-400 border-rose-500/40 bg-rose-500/10' };
    }
    if (t.includes('market') || t.includes('tradfi') || t.includes('stock') || t.includes('equity') || t.includes('option')) {
        return { label: 'TRADFI', icon: '📈', badgeStyle: 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10' };
    }
    if (t.includes('crypto') || t.includes('token') || t.includes('wallet')) {
        return { label: 'CRYPTO', icon: '₿', badgeStyle: 'text-amber-400 border-amber-500/40 bg-amber-500/10' };
    }
    if (t.includes('pred') || t.includes('poly') || t.includes('kalshi')) {
        return { label: 'PREDICTION', icon: '🎯', badgeStyle: 'text-purple-400 border-purple-500/40 bg-purple-500/10' };
    }
    return { label: 'NEWS', icon: '📰', badgeStyle: 'text-slate-300 border-slate-700 bg-slate-800' };
}

// Helper to derive clean source name (ALWAYS included)
function getCleanSource(e: NormalizedEvent): string {
    if (e.source && e.source !== 'unknown' && !e.source.startsWith('Event ')) {
        return e.source;
    }
    const domain = getDomainMeta(e.type).label;
    switch (domain) {
        case 'MARITIME': return 'AISStream Telemetry';
        case 'CYBER': return 'BGP Monitoring Network';
        case 'TRADFI': return 'AlphaVantage Feed';
        case 'CRYPTO': return 'CoinGecko On-Chain';
        case 'PREDICTION': return 'PolyMarket API';
        default: return 'Sentinel Intelligence Collector';
    }
}

// Helper to format clean English titles for events
function formatEnglishHeadline(e: NormalizedEvent): string {
    const entityName = e.primary_entity_name || e.entity_name || e.primary_entity?.name || '';
    
    // Check if headline is valid and not a raw Event UUID / fallback
    const isRawId = (str?: string) => !str || str.startsWith('Event ') || Boolean(str.match(/^[0-9a-f]{8}-[0-9a-f]{4}/i));
    
    if (!isRawId(e.headline)) {
        return e.headline;
    }
    if (!isRawId(e.summary)) {
        return e.summary!;
    }
    
    // Fallback: construct natural English title based on event type & entity
    const t = (e.type || '').toLowerCase();
    const regionStr = e.region ? ` in ${e.region}` : '';

    if (t.includes('vessel_position')) {
        return `Vessel position report for ${entityName || 'Maritime Vessel'}${regionStr}`;
    }
    if (t.includes('vessel_dark')) {
        return `AIS Dark gap anomaly detected on ${entityName || 'Vessel'}${regionStr}`;
    }
    if (t.includes('vessel_static')) {
        return `Vessel static registry report: ${entityName || 'Vessel'}`;
    }
    if (t.includes('cyber') || t.includes('bgp')) {
        return `BGP routing hijack anomaly on ${entityName || 'Network Target'}`;
    }
    if (t.includes('tradfi') || t.includes('market')) {
        return `Equity market volume anomaly on ${entityName || 'Ticker'}`;
    }
    if (t.includes('crypto')) {
        return `Large cryptocurrency transaction for ${entityName || 'Asset'}`;
    }
    if (t.includes('pred')) {
        return `Prediction market probability move for ${entityName || 'Contract'}`;
    }
    
    return `${getDomainMeta(e.type).label} Intelligence Event: ${entityName || 'Asset Target'}${regionStr}`;
}

export default function IntelligenceFeed() {
  const [activeTab, setActiveTab] = useState<'events' | 'scenarios'>('events');
  const [selectedDomain, setSelectedDomain] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);

  // Real-time WebSocket Live Feed connection
  const wsLiveEvents = useLiveEvents(selectedDomain);

  // Fetch AI Scenarios
  const { data: scenarios } = useSWR<Scenario[]>(
    '/scenarios?limit=20',
    fetcher,
    { refreshInterval: 6000 }
  );

  // Dynamic Event domain fetches with rapid 4-second polling
  const { data: tradfiEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'tradfi' ? '/events/tradfi?limit=30' : null,
    fetcher,
    { refreshInterval: 4000 }
  );
  const { data: cryptoEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'crypto' ? '/events/crypto?limit=30' : null,
    fetcher,
    { refreshInterval: 4000 }
  );
  const { data: predictionEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'prediction' ? '/events/prediction?limit=30' : null,
    fetcher,
    { refreshInterval: 4000 }
  );
  const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'cyber' ? '/events/cyber?limit=30' : null,
    fetcher,
    { refreshInterval: 4000 }
  );
  const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'maritime' ? '/events/maritime?limit=30' : null,
    fetcher,
    { refreshInterval: 4000 }
  );

  // Merge events with zero-latency WebSocket stream
  const rawEvents: NormalizedEvent[] = [...wsLiveEvents];
  if (selectedDomain === 'all' || selectedDomain === 'tradfi') rawEvents.push(...(tradfiEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'crypto') rawEvents.push(...(cryptoEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'prediction') rawEvents.push(...(predictionEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'cyber') rawEvents.push(...(cyberEvents || []));
  if (selectedDomain === 'all' || selectedDomain === 'maritime') rawEvents.push(...(maritimeEvents || []));

  const sortedEvents = rawEvents
    .sort((a, b) => new Date(b.occurred_at).getTime() - new Date(a.occurred_at).getTime())
    .filter((e) =>
      searchQuery
        ? (e.headline || e.type || formatEnglishHeadline(e)).toLowerCase().includes(searchQuery.toLowerCase()) ||
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

  const [selectedScenario, setSelectedScenario] = useState<Scenario | null>(null);

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
          sortedEvents.length > 0 ? (
            sortedEvents.map((e, idx) => {
              const domainMeta = getDomainMeta(e.type);
              const sourceName = getCleanSource(e);
              const title = formatEnglishHeadline(e);
              const entityName = e.primary_entity_name || e.entity_name || e.primary_entity?.name || 'Unknown Entity';

              return (
                <div
                  key={e.event_id || idx}
                  onClick={() => setSelectedEvent(e)}
                  className="p-3 rounded-lg bg-slate-900/60 border border-cyan-500/15 hover:border-[#00f2fe]/50 cursor-pointer transition-all hover:bg-slate-900/90 group glass-panel-hover"
                >
                  {/* Header: Domain Badge + Source + Anomaly Score */}
                  <div className="flex items-center justify-between mb-1.5 font-mono text-[10px]">
                    <div className="flex items-center gap-1.5">
                      <span className={`px-1.5 py-0.5 rounded border font-bold uppercase ${domainMeta.badgeStyle}`}>
                        {domainMeta.icon} {domainMeta.label}
                      </span>
                      <span className="text-slate-400 font-medium">
                        via <span className="text-cyan-400 font-bold">{sourceName}</span>
                      </span>
                    </div>
                    {getScoreBadge(e.anomaly_score)}
                  </div>

                  {/* Main English Headline */}
                  <p className="text-xs text-slate-100 font-sans font-semibold line-clamp-2 group-hover:text-white transition-colors leading-snug">
                    {title}
                  </p>

                  {/* Footer: Primary Entity + Timestamp (NO raw Event IDs in list) */}
                  <div className="mt-2 flex items-center justify-between text-[10px] text-slate-400 font-mono pt-1 border-t border-slate-800/60">
                    <span>Entity: <span className="text-amber-300 font-bold">{entityName}</span></span>
                    <span>{new Date(e.occurred_at).toLocaleTimeString()}</span>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="flex flex-col items-center justify-center p-8 text-center space-y-2 border border-dashed border-cyan-500/20 bg-slate-950/40 rounded-lg font-mono">
              <span className="text-2xl animate-pulse">📡</span>
              <p className="text-xs text-slate-300 font-bold">NO MATCHING TELEMETRY EVENTS</p>
              <span className="text-[10px] text-slate-500">Listening on active WebSocket feed & REST polling...</span>
            </div>
          )
        ) : (
          (scenarios || []).length > 0 ? (
            (scenarios || []).map((s, idx) => (
              <div
                key={s.scenario_id || s.correlation_id || idx}
                onClick={() => setSelectedScenario(s)}
                className="p-3.5 rounded-lg bg-slate-900/70 border border-purple-500/20 hover:border-purple-400/60 hover:bg-slate-900/90 cursor-pointer transition-all space-y-2"
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
          ) : (
            <div className="flex flex-col items-center justify-center p-8 text-center space-y-2 border border-dashed border-purple-500/20 bg-slate-950/40 rounded-lg font-mono">
              <span className="text-2xl animate-pulse">🧠</span>
              <p className="text-xs text-purple-300 font-bold">NO ACTIVE AI SCENARIOS</p>
              <span className="text-[10px] text-slate-500">LLM Reasoning Engine running correlation synthesis...</span>
            </div>
          )
        )}
      </div>

      {/* Event Detail Modal (Raw Event IDs & Full Technical Details Drop Down Here Only) */}
      {selectedEvent && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4">
          <div className="bg-[#0b0e17] border border-[#00f2fe]/40 rounded-xl max-w-lg w-full p-5 space-y-4 shadow-[0_0_30px_rgba(0,242,254,0.2)] font-mono">
            <div className="flex items-center justify-between border-b border-cyan-500/20 pb-3">
              <div className="flex items-center gap-2">
                <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${getDomainMeta(selectedEvent.type).badgeStyle}`}>
                  {getDomainMeta(selectedEvent.type).icon} {getDomainMeta(selectedEvent.type).label}
                </span>
                <span className="text-xs font-bold text-[#00f2fe] uppercase tracking-wider">EVENT DETAIL INSPECTOR</span>
              </div>
              <button
                onClick={() => setSelectedEvent(null)}
                className="text-slate-400 hover:text-white text-sm font-bold px-2 py-0.5 rounded bg-slate-800 cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            <div className="space-y-2.5 text-xs">
              <div><span className="text-slate-400 block mb-0.5">EVENT TITLE:</span> <p className="text-white font-bold font-sans text-sm">{formatEnglishHeadline(selectedEvent)}</p></div>
              <div><span className="text-slate-400">SOURCE:</span> <span className="text-cyan-400 font-bold">{getCleanSource(selectedEvent)}</span></div>
              <div><span className="text-slate-400">EVENT ID:</span> <span className="text-slate-300 font-mono select-all bg-slate-900 px-2 py-0.5 rounded border border-slate-800">{selectedEvent.event_id}</span></div>
              <div><span className="text-slate-400">ANOMALY SCORE:</span> <span className="text-rose-400 font-bold">{selectedEvent.anomaly_score.toFixed(4)}</span></div>
              <div><span className="text-slate-400">PRIMARY ENTITY:</span> <span className="text-amber-400 font-bold">{selectedEvent.primary_entity_name || selectedEvent.entity_name || selectedEvent.primary_entity?.name || 'N/A'}</span></div>
              <div><span className="text-slate-400">REGION / THEATER:</span> <span className="text-emerald-400">{selectedEvent.region || 'Global'}</span></div>
              <div><span className="text-slate-400">TIMESTAMP (UTC):</span> <span className="text-slate-300">{new Date(selectedEvent.occurred_at).toUTCString()}</span></div>
            </div>
          </div>
        </div>
      )}

      {/* Scenario Detail Inspector Modal */}
      {selectedScenario && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4">
          <div className="bg-[#0b0e17] border border-purple-500/50 rounded-xl max-w-xl w-full p-6 space-y-4 shadow-[0_0_30px_rgba(168,85,247,0.3)] font-mono max-h-[85vh] overflow-y-auto">
            <div className="flex items-center justify-between border-b border-purple-500/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-xs font-bold text-purple-400 uppercase tracking-wider">AI SCENARIO INSPECTOR</span>
                <span className="px-2 py-0.5 rounded text-[10px] bg-purple-500/20 text-purple-300 border border-purple-500/40">
                  {selectedScenario.confidence_overall}% CONFIDENCE
                </span>
              </div>
              <button
                onClick={() => setSelectedScenario(null)}
                className="text-slate-400 hover:text-white text-sm font-bold px-2 py-0.5 rounded bg-slate-800 cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            <div className="space-y-3">
              <h3 className="text-sm font-bold text-white font-sans">{selectedScenario.headline}</h3>
              <p className="text-xs text-slate-300 font-sans leading-relaxed bg-slate-900/60 p-3 rounded border border-purple-500/20">{selectedScenario.significance}</p>
              
              {selectedScenario.confidence_rationale && (
                <div>
                  <span className="text-[11px] font-bold text-purple-400 uppercase block mb-1.5">Confidence Rationale</span>
                  <p className="text-xs text-slate-400 font-sans">{selectedScenario.confidence_rationale}</p>
                </div>
              )}

              {selectedScenario.hypotheses && selectedScenario.hypotheses.length > 0 && (
                <div className="space-y-2">
                  <span className="text-[11px] font-bold text-cyan-400 uppercase block">Hypotheses Breakdown</span>
                  {selectedScenario.hypotheses.map((h, i) => (
                    <div key={i} className="p-2.5 rounded bg-slate-950 border border-slate-800 space-y-1">
                      <div className="flex items-center justify-between text-xs font-bold text-slate-200">
                        <span>{h.label || `Hypothesis ${i+1}`}</span>
                        <span className="text-[#00f2fe]">{h.probability}%</span>
                      </div>
                      <p className="text-[11px] text-slate-400 font-sans">{h.mechanism}</p>
                    </div>
                  ))}
                </div>
              )}

              {selectedScenario.recommended_monitoring && (
                <div>
                  <span className="text-[11px] font-bold text-amber-400 uppercase block mb-1">Recommended Monitoring</span>
                  <ul className="list-disc list-inside text-xs text-slate-300 space-y-1">
                    {selectedScenario.recommended_monitoring.map((m, i) => (
                      <li key={i}>{m}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>

            <div className="pt-2 border-t border-purple-500/20 flex justify-end gap-2">
              <button
                onClick={() => {
                  alert(`Dispatched scenario '${selectedScenario.headline}' to Adversarial Wargame Engine.`);
                  setSelectedScenario(null);
                }}
                className="px-3 py-1.5 rounded bg-purple-600 hover:bg-purple-500 text-white font-bold text-xs cursor-pointer transition-colors"
              >
                ⚔️ RUN ADVERSARIAL WARGAME
              </button>
            </div>
          </div>
        </div>
      )}
    </Card>
  );
}