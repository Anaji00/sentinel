'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { apiClient, fetcher } from '../lib/api';
import { useLiveEvents } from '../lib/useLiveEvents';
import { NormalizedEvent, Scenario } from '../lib/types';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';

interface EvidenceContributor {
  agent_name: string;
  direction?: string;
  conviction?: number;
  score?: number;
  weight?: number;
}

interface CorrelationCluster {
  correlation_id: string;
  rule_name: string;
  alert_tier: number;
  detected_at: string;
  description: string;
  tags?: string[];
  evidence_trail?: EvidenceContributor[];
}

// Helper to derive clean domain tag + icon
function getDomainMeta(type: string): { label: string; icon: string; badgeStyle: string } {
    const t = (type || '').toLowerCase();
    if (t.includes('pred') || t.includes('poly') || t.includes('kalshi')) {
        return { label: 'PREDICTION', icon: '🎯', badgeStyle: 'text-purple-400 border-purple-500/40 bg-purple-500/10' };
    }
    if (t.includes('earnings')) {
        return { label: 'EARNINGS', icon: '📅', badgeStyle: 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10' };
    }
    if (t.includes('funding')) {
        return { label: 'PERP FUNDING', icon: '⚡', badgeStyle: 'text-purple-400 border-purple-500/40 bg-purple-500/10' };
    }
    if (t.includes('vessel') || t.includes('maritime') || t.includes('ais')) {
        return { label: 'MARITIME', icon: '🚢', badgeStyle: 'text-cyan-400 border-cyan-500/40 bg-cyan-500/10' };
    }
    if (t.includes('flight') || t.includes('aviation') || t.includes('adsb')) {
        return { label: 'AVIATION', icon: '✈️', badgeStyle: 'text-blue-400 border-blue-500/40 bg-blue-500/10' };
    }
    if (t.includes('cyber') || t.includes('bgp') || t.includes('breach')) {
        return { label: 'CYBER', icon: '🔐', badgeStyle: 'text-rose-400 border-rose-500/40 bg-rose-500/10' };
    }
    if (t.includes('crypto') || t.includes('token') || t.includes('wallet') || t.includes('blockchain') || t.includes('perp')) {
        return { label: 'CRYPTO', icon: '₿', badgeStyle: 'text-amber-400 border-amber-500/40 bg-amber-500/10' };
    }
    if (t.includes('tradfi') || t.includes('stock') || t.includes('equity') || t.includes('option') || t.includes('market')) {
        return { label: 'TRADFI', icon: '📈', badgeStyle: 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10' };
    }
    return { label: 'NEWS', icon: '📰', badgeStyle: 'text-slate-300 border-slate-700 bg-slate-800' };
}

// Helper to derive clean source name
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
    const rawHeadline = e.headline || '';
    
    // Sanitize any raw string split artifacts like 'tradfi | prediction_market' or 'tradfi - prediction market'
    if (rawHeadline.includes('tradfi | prediction') || rawHeadline.includes('tradfi - prediction')) {
        const cleaned = rawHeadline.replace(/tradfi\s*[|\-]\s*prediction_market/gi, 'Prediction Market');
        return cleaned;
    }
    
    if (e.prediction_market_data?.question) {
        const pm = e.prediction_market_data;
        const sideStr = pm.outcome ? ` (${pm.outcome})` : '';
        const qUpper = (pm.question || '').toUpperCase();
        return `🎯 PREDICTION MARKET: ${qUpper}${sideStr} — Notional: $${(pm.notional_usd || 0).toLocaleString()}`;
    }

    const entityName = e.primary_entity_name || e.entity_name || e.primary_entity?.name || '';
    const isRawId = (str?: string) => !str || str.startsWith('Event ') || Boolean(str.match(/^[0-9a-f]{8}-[0-9a-f]{4}/i));
    
    if (!isRawId(e.headline)) {
        return e.headline;
    }
    if (!isRawId(e.summary)) {
        return e.summary!;
    }
    
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
    if (t.includes('pred')) {
        return `Prediction market probability move for ${entityName || 'Contract'}`;
    }
    if (t.includes('crypto')) {
        return `Cryptocurrency market anomaly on ${entityName || 'Asset'}`;
    }
    if (t.includes('tradfi') || t.includes('market')) {
        return `Equity market volume anomaly on ${entityName || 'Ticker'}`;
    }
    
    return `${getDomainMeta(e.type).label} Intelligence Event: ${entityName || 'Asset Target'}${regionStr}`;
}

const getScoreBadge = (score: number) => {
  if (score >= 0.75) return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-rose-500/20 text-rose-400 border border-rose-500/40 glow-crimson">CRITICAL {score.toFixed(2)}</span>;
  if (score >= 0.50) return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40 glow-amber">ELEVATED {score.toFixed(2)}</span>;
  return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/40">NORMAL {score.toFixed(2)}</span>;
};

const EventRow = React.memo(({ e, onClick }: { e: NormalizedEvent; onClick: (e: NormalizedEvent) => void }) => {
  const domainMeta = getDomainMeta(e.type);
  const sourceName = getCleanSource(e);
  const title = formatEnglishHeadline(e);
  const entityName = e.primary_entity_name || e.entity_name || e.primary_entity?.name || 'Unknown Entity';

  const fd = e.financial_data || {};
  const cd = e.crypto_data || {};

  return (
    <div
      onClick={() => onClick(e)}
      className="p-3 rounded-lg bg-slate-900/60 border border-cyan-500/15 hover:border-[#00f2fe]/50 cursor-pointer transition-all hover:bg-slate-900/90 group glass-panel-hover"
    >
      <div className="flex items-center justify-between mb-1.5 font-mono text-[10px]">
        <div className="flex items-center gap-1.5 flex-wrap">
          <span className={`px-1.5 py-0.5 rounded border font-bold uppercase ${domainMeta.badgeStyle}`}>
            {domainMeta.icon} {domainMeta.label}
          </span>
          {fd.option_type && (
            <span className={`px-1.5 py-0.5 rounded border font-extrabold text-[9px] ${
              fd.option_type === 'CALL' ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40' : 'bg-rose-500/20 text-rose-300 border-rose-500/40'
            }`}>
              {fd.option_type}
            </span>
          )}
          {fd.eps_surprise_pct !== undefined && fd.eps_surprise_pct !== null && (
            <span className={`px-1.5 py-0.5 rounded border font-extrabold text-[9px] ${
              fd.eps_surprise_pct >= 0 ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40' : 'bg-rose-500/20 text-rose-300 border-rose-500/40'
            }`}>
              EPS {fd.eps_surprise_pct >= 0 ? '+' : ''}{fd.eps_surprise_pct.toFixed(1)}%
            </span>
          )}
          {cd.funding_rate !== undefined && (
            <span className="px-1.5 py-0.5 rounded border border-purple-500/40 bg-purple-500/20 text-purple-300 font-extrabold text-[9px]">
              {(cd.funding_rate * 100).toFixed(4)}% RATE
            </span>
          )}
          <span className="text-slate-400 font-medium">
            via <span className="text-cyan-400 font-bold">{sourceName}</span>
          </span>
        </div>
        {getScoreBadge(e.anomaly_score)}
      </div>

      <p className="text-xs text-slate-100 font-sans font-semibold line-clamp-2 group-hover:text-white transition-colors leading-snug">
        {title}
      </p>

      <div className="mt-2 flex items-center justify-between text-[10px] text-slate-400 font-mono pt-1 border-t border-slate-800/60">
        <span>Entity: <span className="text-amber-300 font-bold">{entityName}</span></span>
        <span>{new Date(e.occurred_at).toLocaleTimeString()}</span>
      </div>
    </div>
  );
});
EventRow.displayName = 'EventRow';

const CorrelationCard = React.memo(({ c }: { c: CorrelationCluster }) => {
  const [showEvidence, setShowEvidence] = useState(false);
  const trail = c.evidence_trail || [];

  return (
    <div className="p-3 rounded-lg bg-slate-900/80 border border-amber-500/20 hover:border-amber-400/60 transition-all space-y-1.5 font-mono">
      <div className="flex items-center justify-between">
        <span className="font-bold text-amber-400 uppercase text-xs flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-amber-400 animate-ping" />
          RULE: {c.rule_name}
        </span>
        <span className="px-2 py-0.5 rounded text-[9px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40">
          TIER {c.alert_tier}
        </span>
      </div>
      <p className="text-xs text-slate-200 font-sans leading-snug">{c.description}</p>

      {trail.length > 0 && (
        <div className="pt-1">
          <button
            onClick={() => setShowEvidence(!showEvidence)}
            className="text-[10px] text-cyan-400 hover:text-cyan-300 font-bold flex items-center gap-1 cursor-pointer"
          >
            <span>{showEvidence ? '▼ HIDE EVIDENCE TRAIL' : '▶ SHOW EVIDENCE TRAIL'}</span>
            <span className="text-slate-400 font-normal">({trail.length} signals)</span>
          </button>
          {showEvidence && (
            <div className="mt-1.5 p-2 bg-slate-950/80 rounded border border-cyan-500/20 space-y-1 text-[10px]">
              <span className="text-cyan-300 font-bold block mb-1">EVIDENCE TRAIL (SWARM FUSION):</span>
              {trail.map((item, idx) => (
                <div key={idx} className="flex items-center justify-between border-b border-slate-800/60 pb-0.5">
                  <span className="text-slate-200 font-bold">{item.agent_name}</span>
                  <span className="text-slate-400">
                    Dir: <span className="text-amber-300 font-bold">{item.direction || 'neutral'}</span> | Score: <span className="text-emerald-400 font-bold">{(item.conviction ?? item.score ?? 0).toFixed(2)}</span> | Weight: <span className="text-cyan-400">{(item.weight ?? 1.0).toFixed(2)}</span>
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      <div className="flex items-center justify-between text-[10px] text-slate-400 border-t border-slate-800/80 pt-1">
        <span>TAGS: {c.tags?.join(', ') || 'MULTI-DOMAIN'}</span>
        <span>{new Date(c.detected_at).toLocaleTimeString()}</span>
      </div>
    </div>
  );
});
CorrelationCard.displayName = 'CorrelationCard';

export default function IntelligenceFeed() {
  const [activeTab, setActiveTab] = useState<'events' | 'scenarios' | 'correlations'>('events');
  const [selectedDomain, setSelectedDomain] = useState<string>('all');
  const [scenarioStatus, setScenarioStatus] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);
  const [fullEventDetail, setFullEventDetail] = useState<any | null>(null);
  const [isLoadingDetail, setIsLoadingDetail] = useState<boolean>(false);

  // Real-time WebSocket Live Feed connection
  const wsLiveEvents = useLiveEvents(selectedDomain);

  // Fetch AI Scenarios with dynamic status filter
  const scenarioUrl = scenarioStatus === 'all' 
    ? '/scenarios?limit=20' 
    : `/scenarios?limit=20&status=${encodeURIComponent(scenarioStatus)}`;

  const { data: scenarios } = useSWR<Scenario[]>(
    scenarioUrl,
    fetcher,
    { refreshInterval: 6000 }
  );

  // Fetch Raw Correlation Clusters
  const { data: correlations } = useSWR<CorrelationCluster[]>(
    '/correlations?limit=30&min_tier=1',
    fetcher,
    { refreshInterval: 6000 }
  );

  // Dynamic Event domain fetches
  const { data: tradfiEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'tradfi' ? '/events/tradfi?limit=30' : null,
    fetcher,
    { refreshInterval: 4000 }
  );
  const { data: cryptoEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'crypto' ? '/events/crypto?limit=30' : null,
    fetcher,
    { refreshInterval: 6000 }
  );
  const { data: predictionEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'prediction' ? '/events/prediction?limit=30' : null,
    fetcher,
    { refreshInterval: 8000 }
  );
  const { data: cyberEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'cyber' ? '/events/cyber?limit=30' : null,
    fetcher,
    { refreshInterval: 10000 }
  );
  const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'maritime' ? '/events/maritime?limit=30' : null,
    fetcher,
    { refreshInterval: 8000 }
  );

  // Merge events with zero-latency WebSocket stream, deduplicating by event_id
  const sortedEvents = useMemo(() => {
    const rawEvents: NormalizedEvent[] = [...wsLiveEvents];
    if (selectedDomain === 'all' || selectedDomain === 'tradfi') rawEvents.push(...(tradfiEvents || []));
    if (selectedDomain === 'all' || selectedDomain === 'crypto') rawEvents.push(...(cryptoEvents || []));
    if (selectedDomain === 'all' || selectedDomain === 'prediction') rawEvents.push(...(predictionEvents || []));
    if (selectedDomain === 'all' || selectedDomain === 'cyber') rawEvents.push(...(cyberEvents || []));
    if (selectedDomain === 'all' || selectedDomain === 'maritime') rawEvents.push(...(maritimeEvents || []));

    const deduped = Array.from(
      new Map(rawEvents.map(e => [e.event_id, e])).values()
    );

    return deduped
      .sort((a, b) => new Date(b.occurred_at).getTime() - new Date(a.occurred_at).getTime())
      .filter((e) =>
        searchQuery
          ? (e.headline || e.type || formatEnglishHeadline(e)).toLowerCase().includes(searchQuery.toLowerCase()) ||
            (e.source || '').toLowerCase().includes(searchQuery.toLowerCase()) ||
            (e.primary_entity?.name || '').toLowerCase().includes(searchQuery.toLowerCase())
          : true
      )
      .slice(0, 45);
  }, [wsLiveEvents, tradfiEvents, cryptoEvents, predictionEvents, cyberEvents, maritimeEvents, selectedDomain, searchQuery]);

  const mainTabs = [
    { id: 'events', label: 'LIVE STREAM', count: sortedEvents.length },
    { id: 'scenarios', label: 'AI SCENARIOS', count: scenarios?.length || 0 },
    { id: 'correlations', label: 'CORRELATIONS', count: correlations?.length || 0 },
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

  // Handle Event Click to fetch deep detail payload from backend
  const handleEventClick = async (event: NormalizedEvent) => {
    setSelectedEvent(event);
    setFullEventDetail(null);
    setIsLoadingDetail(true);
    try {
      const res = await apiClient.get(`/events/detail/${event.event_id}`);
      setFullEventDetail(res.data);
    } catch (err) {
      console.warn("Could not fetch deep event details:", err);
    } finally {
      setIsLoadingDetail(false);
    }
  };

  return (
    <Card
      title="INTELLIGENCE FEED"
      badge={<Badge variant="live" pulse>STREAMING</Badge>}
      headerAction={
        <Tabs
          tabs={mainTabs}
          activeTab={activeTab}
          onChange={(id) => setActiveTab(id as 'events' | 'scenarios' | 'correlations')}
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
            id="intelligence-search-input"
            name="intelligence_search"
            autoComplete="off"
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
            sortedEvents.map((e, idx) => (
              <EventRow key={e.event_id || idx} e={e} onClick={handleEventClick} />
            ))
          ) : (
            <div className="flex flex-col items-center justify-center p-8 text-center space-y-2 border border-dashed border-cyan-500/20 bg-slate-950/40 rounded-lg font-mono">
              <span className="text-2xl animate-pulse">📡</span>
              <p className="text-xs text-slate-300 font-bold">NO MATCHING TELEMETRY EVENTS</p>
              <span className="text-[10px] text-slate-500">Listening on active WebSocket feed & REST polling...</span>
            </div>
          )
        ) : activeTab === 'scenarios' ? (
          <>
            <div className="flex items-center gap-1.5 pb-2 mb-1 border-b border-purple-500/20 font-mono text-[10px]">
              <span className="text-slate-400 font-bold uppercase mr-1">STATUS FILTER:</span>
              {[
                { id: 'all', label: 'ALL' },
                { id: 'confirmed', label: 'CONFIRMED' },
                { id: 'hypothesis', label: 'HYPOTHESIS' },
                { id: 'under_revise', label: 'UNDER REVISION' },
              ].map(st => (
                <button
                  key={st.id}
                  onClick={() => setScenarioStatus(st.id)}
                  className={`px-2 py-0.5 rounded font-bold transition-all cursor-pointer ${
                    scenarioStatus === st.id
                      ? 'bg-purple-500/30 text-purple-300 border border-purple-400/50 glow-purple'
                      : 'bg-slate-950 text-slate-400 hover:text-white border border-slate-800'
                  }`}
                >
                  {st.label}
                </button>
              ))}
            </div>

            {(scenarios || []).length > 0 ? (
              (scenarios || []).map((s, idx) => (
                <div
                  key={s.scenario_id || s.correlation_id || idx}
                  onClick={() => setSelectedScenario(s)}
                  className="p-3.5 rounded-lg bg-slate-900/70 border border-purple-500/20 hover:border-purple-400/60 hover:bg-slate-900/90 cursor-pointer transition-all space-y-2"
                >
                  <div className="flex items-center justify-between font-mono">
                    <span className="text-[10px] font-bold text-purple-400 uppercase tracking-wider flex items-center gap-1.5">
                      <span className="h-1.5 w-1.5 rounded-full bg-purple-400 animate-ping" />
                      {s.status ? s.status.toUpperCase() : 'AI SYNTHESIZED SCENARIO'}
                    </span>
                    <span className="px-2 py-0.5 rounded text-[10px] bg-purple-500/20 text-purple-300 border border-purple-500/30 font-bold">
                      CONFIDENCE {s.confidence_overall}%
                    </span>
                  </div>
                  <h4 className="text-xs font-bold text-slate-100 font-sans">{s.headline || s.primary_entity_name}</h4>
                  <p className="text-[11px] text-slate-300 line-clamp-2">{s.significance}</p>
                </div>
              ))
            ) : (
              <div className="flex flex-col items-center justify-center p-8 text-center space-y-2 border border-dashed border-purple-500/20 bg-slate-950/40 rounded-lg font-mono">
                <span className="text-2xl animate-pulse">🧠</span>
                <p className="text-xs text-purple-300 font-bold">NO SCENARIOS MATCHING STATUS '{scenarioStatus.toUpperCase()}'</p>
                <span className="text-[10px] text-slate-500">LLM Reasoning Engine evaluating Bayesian recalibration loop...</span>
              </div>
            )}
          </>
        ) : (
          (correlations || []).length > 0 ? (
            (correlations || []).map((c, idx) => (
              <CorrelationCard key={c.correlation_id || idx} c={c} />
            ))
          ) : (
            <div className="flex flex-col items-center justify-center p-8 text-center space-y-2 border border-dashed border-amber-500/20 bg-slate-950/40 rounded-lg font-mono">
              <span className="text-2xl animate-pulse">⚡</span>
              <p className="text-xs text-amber-300 font-bold">NO RAW CORRELATION CLUSTERS</p>
              <span className="text-[10px] text-slate-500">Rule correlation engine evaluating multi-domain triggers...</span>
            </div>
          )
        )}
      </div>

      {/* Deep Event Detail Modal */}
      {selectedEvent && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4">
          <div className="bg-[#0b0e17] border border-[#00f2fe]/40 rounded-xl max-w-2xl w-full p-5 space-y-4 shadow-[0_0_30px_rgba(0,242,254,0.2)] font-mono max-h-[85vh] overflow-y-auto">
            <div className="flex items-center justify-between border-b border-cyan-500/20 pb-3">
              <div className="flex items-center gap-2">
                <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${getDomainMeta(selectedEvent.type).badgeStyle}`}>
                  {getDomainMeta(selectedEvent.type).icon} {getDomainMeta(selectedEvent.type).label}
                </span>
                <span className="text-xs font-bold text-[#00f2fe] uppercase tracking-wider">EVENT FORENSIC DETAIL INSPECTOR</span>
              </div>
              <button
                onClick={() => { setSelectedEvent(null); setFullEventDetail(null); }}
                className="text-slate-400 hover:text-white text-sm font-bold px-2 py-0.5 rounded bg-slate-800 cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            <div className="space-y-3 text-xs">
              <div>
                <span className="text-slate-400 block mb-0.5">EVENT HEADLINE:</span>
                <p className="text-white font-bold font-sans text-sm">{formatEnglishHeadline(selectedEvent)}</p>
              </div>

              <div className="grid grid-cols-2 gap-2 bg-slate-950 p-3 rounded-lg border border-slate-800">
                <div><span className="text-slate-400">EVENT ID:</span> <span className="text-cyan-400 font-bold block truncate">{selectedEvent.event_id}</span></div>
                <div><span className="text-slate-400">SOURCE:</span> <span className="text-cyan-400 font-bold block">{getCleanSource(selectedEvent)}</span></div>
                <div><span className="text-slate-400">ANOMALY SCORE:</span> <span className="text-amber-400 font-bold block">{selectedEvent.anomaly_score.toFixed(2)}</span></div>
                <div><span className="text-slate-400">TIMESTAMP:</span> <span className="text-slate-200 block">{new Date(selectedEvent.occurred_at).toUTCString()}</span></div>
                <div><span className="text-slate-400">PRIMARY ENTITY:</span> <span className="text-emerald-400 font-bold block">{selectedEvent.primary_entity_name || selectedEvent.entity_name || selectedEvent.primary_entity?.name || 'N/A'}</span></div>
                <div><span className="text-slate-400">REGION:</span> <span className="text-purple-400 font-bold block">{selectedEvent.region || 'GLOBAL'}</span></div>
              </div>

              {/* Full JSON Payload from TimescaleDB */}
              <div>
                <span className="text-cyan-400 font-bold block mb-1">DEEP DATABASE JSON PAYLOAD ({isLoadingDetail ? 'FETCHING...' : 'LIVE'}):</span>
                {isLoadingDetail ? (
                  <div className="p-4 bg-slate-950 rounded border border-cyan-500/20 text-slate-400 text-center animate-pulse">
                    Querying Hypertable event payload...
                  </div>
                ) : (
                  <pre className="p-3 bg-slate-950 rounded border border-slate-800 text-[10px] text-emerald-300 overflow-x-auto max-h-52 font-mono">
                    {JSON.stringify(fullEventDetail || selectedEvent, null, 2)}
                  </pre>
                )}
              </div>
            </div>

            <div className="pt-2">
              <button
                onClick={() => { setSelectedEvent(null); setFullEventDetail(null); }}
                className="w-full py-2 bg-slate-900 text-[#00f2fe] border border-cyan-500/30 rounded-lg text-xs font-bold hover:bg-slate-800 transition-colors cursor-pointer"
              >
                DISMISS INSPECTOR
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Scenario Detail Modal */}
      {selectedScenario && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4">
          <div className="bg-[#0c0914] border border-purple-500/50 rounded-xl max-w-2xl w-full p-5 space-y-4 shadow-[0_0_40px_rgba(168,85,247,0.25)] font-mono max-h-[85vh] overflow-y-auto">
            <div className="flex items-center justify-between border-b border-purple-500/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-xl">🧠</span>
                <span className="text-xs font-bold text-purple-300 uppercase tracking-wider">AI STRATEGIC SCENARIO REVIEW</span>
              </div>
              <button
                onClick={() => setSelectedScenario(null)}
                className="text-slate-400 hover:text-white text-sm font-bold px-2 py-0.5 rounded bg-slate-800 cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            <div className="space-y-3 text-xs">
              <div className="flex items-center justify-between bg-purple-950/40 p-3 rounded-lg border border-purple-500/30">
                <div>
                  <span className="text-slate-400 block text-[10px]">SCENARIO STATUS:</span>
                  <span className="text-purple-300 font-bold text-sm uppercase">
                    {selectedScenario.status || 'HYPOTHESIS'}
                  </span>
                </div>
                <div className="text-right">
                  <span className="text-slate-400 block text-[10px]">CONFIDENCE SCORE:</span>
                  <span className="text-emerald-400 font-extrabold text-sm">
                    {selectedScenario.confidence_overall}%
                  </span>
                </div>
              </div>

              <div>
                <span className="text-purple-400 font-bold block mb-1">HEADLINE:</span>
                <h3 className="text-sm font-bold text-white font-sans">{selectedScenario.headline}</h3>
              </div>

              {selectedScenario.primary_entity_name && (
                <div>
                  <span className="text-slate-400 block mb-0.5">PRIMARY ENTITY:</span>
                  <span className="text-amber-300 font-bold">{selectedScenario.primary_entity_name}</span>
                </div>
              )}

              <div>
                <span className="text-slate-400 block mb-1">STRATEGIC SIGNIFICANCE:</span>
                <p className="text-slate-200 bg-slate-950 p-3 rounded border border-purple-500/20 leading-relaxed font-sans">{selectedScenario.significance}</p>
              </div>

              {selectedScenario.confidence_rationale && (
                <div>
                  <span className="text-slate-400 block mb-1">CONFIDENCE RATIONALE:</span>
                  <p className="text-slate-300 bg-slate-950 p-3 rounded border border-slate-800 text-[11px] leading-relaxed">{selectedScenario.confidence_rationale}</p>
                </div>
              )}

              {selectedScenario.evidence_trail && selectedScenario.evidence_trail.length > 0 && (
                <div>
                  <span className="text-cyan-400 font-bold block mb-1">EVIDENCE TRAIL (SWARM FUSION):</span>
                  <div className="p-2.5 bg-slate-950 rounded border border-cyan-500/20 space-y-1 text-[10px]">
                    {selectedScenario.evidence_trail.map((item, idx) => (
                      <div key={idx} className="flex items-center justify-between border-b border-slate-800 pb-1">
                        <span className="text-slate-200 font-bold">{item.agent_name}</span>
                        <span className="text-slate-400">
                          Dir: <span className="text-amber-300 font-bold">{item.direction || 'neutral'}</span> | Conviction: <span className="text-emerald-400 font-bold">{(item.conviction ?? item.score ?? 0).toFixed(2)}</span>
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>

            <div className="pt-2">
              <button
                onClick={() => setSelectedScenario(null)}
                className="w-full py-2 bg-purple-950/60 text-purple-300 border border-purple-500/40 rounded-lg text-xs font-bold hover:bg-purple-900/60 transition-colors cursor-pointer"
              >
                DISMISS REVIEW
              </button>
            </div>
          </div>
        </div>
      )}
    </Card>
  );
}