'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { apiClient, fetcher } from '../lib/api';
import { useLiveEvents } from '../lib/useLiveEvents';
import { NormalizedEvent, Scenario } from '../lib/types';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { DataGrid } from './ui/DataGrid';
import { formatPercent } from '../lib/format';

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
/** The domain an event belongs to.
 *
 *  The gateway decides this now, from which payload column the row actually
 *  carries, and sends it as `domain`. It has to: deriving it here meant
 *  substring-matching the event type, and "market_anomaly" contains "market",
 *  so every Coinbase candle anomaly was labelled TRADFI -- BCHUSDT, DOTUSDT and
 *  ADAUSDT rendered as stock-market events.
 *
 *  The payload checks and the type heuristic remain as fallbacks for rows served
 *  by an older gateway.
 */
export function domainMetaFor(e: NormalizedEvent): { label: string; icon: string; badgeStyle: string } {
    const declared = (e as { domain?: string }).domain;
    const exemplar: Record<string, string> = {
        crypto: 'crypto_trade',
        prediction: 'prediction_market_trade',
        maritime: 'vessel_position',
        aviation: 'flight_position',
        cyber: 'bgp_anomaly',
        tradfi: 'equity_block',
        news: 'headline',
    };
    if (declared && exemplar[declared]) return getDomainMeta(exemplar[declared]);
    if (e.crypto_data) return getDomainMeta('crypto_trade');
    if (e.prediction_market_data) return getDomainMeta('prediction_market_trade');
    if (e.vessel_data) return getDomainMeta('vessel_position');
    if (e.flight_data) return getDomainMeta('flight_position');
    if (e.financial_data) return getDomainMeta('equity_block');
    return getDomainMeta(e.type);
}

export function getDomainMeta(type: string): { label: string; icon: string; badgeStyle: string } {
    const t = (type || '').toLowerCase();
    if (t.includes('pred') || t.includes('poly') || t.includes('kalshi')) {
        return { label: 'PREDICTION', icon: '🎯', badgeStyle: 'text-purple-400 border-purple-500/40 bg-purple-500/10' };
    }
    if (t.includes('crypto') || t.includes('coinbase') || t.includes('binance') || t.includes('token') || t.includes('wallet') || t.includes('blockchain') || t.includes('perp') || t.includes('btc') || t.includes('eth') || t.includes('sol')) {
        return { label: 'CRYPTO', icon: '₿', badgeStyle: 'text-amber-400 border-amber-500/40 bg-amber-500/10' };
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
    if (t.includes('tradfi') || t.includes('stock') || t.includes('equity') || t.includes('option') || t.includes('market')) {
        return { label: 'TRADFI', icon: '📈', badgeStyle: 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10' };
    }
    return { label: 'NEWS', icon: '📰', badgeStyle: 'text-slate-300 border-slate-700 bg-slate-800' };
}

// Helper to derive clean source name
export function getCleanSource(e: NormalizedEvent): string {
    if (e.source && e.source !== 'unknown' && !e.source.startsWith('Event ')) {
        return e.source;
    }
    // Nothing, rather than a guess. This used to name a vendor per domain --
    // "AlphaVantage Feed" for TRADFI, "CoinGecko On-Chain" for CRYPTO,
    // "AISStream Telemetry" for MARITIME. This deployment uses none of them
    // (equities come from Alpaca and Finnhub, crypto from Coinbase and OKX), so
    // the interface credited data to companies with no part in producing it --
    // and did so most confidently on rows whose domain it had already guessed
    // wrong. An unattributed row now shows no attribution.
    return '';
}

// Helper to format clean English titles for events
function formatEnglishHeadline(e: NormalizedEvent): string {
    const rawHeadline = e.headline || '';
    
    // Priority 1: If backend provided an enriched headline, use it directly!
    if (rawHeadline && !rawHeadline.startsWith('Event ') && !rawHeadline.match(/^[0-9a-f]{8}-[0-9a-f]{4}/i)) {
        return rawHeadline.replace(/tradfi\s*[|\-]\s*prediction_market/gi, 'Prediction Market');
    }
    
    // Priority 2: If summary is present and rich, use it
    if (e.summary && e.summary.length > 15 && !e.summary.startsWith('Event ')) {
        return e.summary;
    }

    if (e.prediction_market_data?.question) {
        const pm = e.prediction_market_data;
        const sideStr = pm.outcome ? ` (${pm.outcome})` : '';
        const qUpper = (pm.question || '').toUpperCase();
        return `🎯 PREDICTION MARKET: ${qUpper}${sideStr}`;
    }

    const entityName = e.primary_entity_name || e.entity_name || e.primary_entity?.name || '';
    const t = (e.type || '').toLowerCase();
    const regionStr = e.region ? ` in ${e.region}` : '';

    return `${getDomainMeta(e.type).label} Intelligence Event: ${entityName || 'Target'}${regionStr}`;
}

const getScoreBadge = (score: number) => {
  if (score >= 0.75) return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-rose-500/20 text-rose-400 border border-rose-500/40 glow-crimson">CRITICAL {score.toFixed(2)}</span>;
  if (score >= 0.50) return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40 glow-amber">ELEVATED {score.toFixed(2)}</span>;
  return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/40">NORMAL {score.toFixed(2)}</span>;
};

/** How well a claim is independently supported.
 *
 *  Only meaningful for events that can be corroborated at all -- news and
 *  OSINT. A market tick has no second source, and marking it "single-sourced"
 *  would be noise, so an absent assessment renders nothing.
 *
 *  Single-sourced is the state worth an analyst's attention: still a lead, but
 *  it must not read as confirmed. Syndication is called out separately, because
 *  four outlets running one wire story looks like consensus and is not.
 */
function CorroborationBadge({ e }: { e: NormalizedEvent }) {
    const c = e.corroboration;
    if (!c) return null;

    if (c.is_single_sourced) {
        return (
            <span
                className="badge tone-caution"
                title="Only one source reports this so far. It is a lead, not a confirmed fact."
            >
                single source
            </span>
        );
    }

    const timing =
        c.minutes_to_corroboration !== null
            ? ` — second source ${Math.round(c.minutes_to_corroboration)} min later`
            : '';
    const syndicated = c.is_syndicated
        ? ' Some reports share wording, which suggests syndication rather than independent confirmation.'
        : '';

    return (
        <span
            className={`badge ${c.is_syndicated ? 'tone-info' : 'tone-positive'}`}
            title={`${c.contributing_sources.slice(0, 6).join(', ')}${timing}.${syndicated}`}
        >
            {c.independent_sources} sources
        </span>
    );
}

const EventRow = React.memo(({ e, onClick }: { e: NormalizedEvent; onClick: (e: NormalizedEvent) => void }) => {
  const domainMeta = domainMetaFor(e);
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
          <CorroborationBadge e={e} />
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
              EPS {formatPercent(fd.eps_surprise_pct, { decimals: 1, signed: true })}
            </span>
          )}
          {cd.funding_rate !== undefined && (
            <span className="px-1.5 py-0.5 rounded border border-purple-500/40 bg-purple-500/20 text-purple-300 font-extrabold text-[9px]">
              {formatPercent(cd.funding_rate, { from: 'ratio', decimals: 4 })} RATE
            </span>
          )}
          {sourceName && (
            <span className="text-slate-400 font-medium">
              via <span className="text-cyan-400 font-bold">{sourceName}</span>
            </span>
          )}
        </div>
        {getScoreBadge(e.anomaly_score)}
      </div>

      <p className="text-xs text-slate-100 font-sans font-semibold line-clamp-2 group-hover:text-white transition-colors leading-snug">
        {title}
      </p>

      {e.summary && (
        <p className="text-[11px] text-slate-300 font-sans mt-1 line-clamp-2 leading-tight">
          {e.summary}
        </p>
      )}

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
      // /events/detail/{id}, not /events/{id}.
      //
      // The latter matches the /events/{domain} route, and an unrecognised
      // "domain" falls through to the all-events branch -- so this returned a
      // list of fifty unrelated events instead of the one requested. DataGrid
      // flattens objects and returns nothing for an array, which is why the
      // inspector reported "No structured detail available" for every event.
      const response = await apiClient.get(
        `/events/detail/${encodeURIComponent(event.event_id)}`,
      );
      setFullEventDetail(response.data);
    } catch (err) {
      console.warn("Could not fetch deep event detail from hypertable, using live payload fallback:", err);
      setFullEventDetail(event);
    } finally {
      setIsLoadingDetail(false);
    }
  };

  return (
    <div className="space-y-4 max-w-[1600px] mx-auto font-sans">
      {/* Header Bar */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 p-4 bg-[#090d16] border border-cyan-500/20 rounded-xl shadow-[0_0_20px_rgba(0,242,254,0.05)]">
        <div>
          <h1 className="text-lg font-mono font-bold text-white tracking-wider flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-cyan-400 animate-pulse shadow-[0_0_10px_#00f2fe]" />
            MULTI-DOMAIN INTELLIGENCE FEED
          </h1>
          <p className="text-xs text-slate-400 font-mono mt-0.5">
            Real-time cross-domain event stream, AI strategic scenarios & macro correlation clusters.
          </p>
        </div>

        {/* Search & Filter Inputs */}
        <div className="flex items-center gap-2">
          <input
            type="text"
            placeholder="Search events, tickers, entities..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="px-3 py-1.5 rounded-lg bg-slate-950 border border-slate-800 text-xs text-slate-100 focus:outline-none focus:border-cyan-500/60 font-mono w-48 md:w-64"
          />
        </div>
      </div>

      {/* Main Mode Tabs */}
      <div className="flex items-center justify-between gap-2 border-b border-slate-800 pb-2">
        <div className="flex items-center gap-2">
          {mainTabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as any)}
              className={`px-3 py-1.5 rounded-lg font-mono text-xs font-bold transition-all cursor-pointer flex items-center gap-2 ${
                activeTab === tab.id
                  ? 'bg-cyan-500/20 text-[#00f2fe] border border-cyan-500/40 shadow-[0_0_12px_rgba(0,242,254,0.25)]'
                  : 'bg-slate-900/60 text-slate-400 border border-slate-800 hover:text-slate-200'
              }`}
            >
              {tab.label}
              <span className="px-1.5 py-0.2 rounded bg-slate-950 text-[10px] font-mono text-cyan-300 font-bold border border-cyan-500/20">
                {tab.count}
              </span>
            </button>
          ))}
        </div>

        {/* Sub-domain filters (Only visible in Live Stream tab) */}
        {activeTab === 'events' && (
          <div className="flex items-center gap-1 overflow-x-auto py-1 font-mono text-[11px]">
            {domainTabs.map((d) => (
              <button
                key={d.id}
                onClick={() => setSelectedDomain(d.id)}
                className={`px-2.5 py-1 rounded transition-colors cursor-pointer font-bold ${
                  selectedDomain === d.id
                    ? 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/40'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-900'
                }`}
              >
                {d.label}
              </button>
            ))}
          </div>
        )}

        {/* Scenario Status filter (Only visible in AI Scenarios tab) */}
        {activeTab === 'scenarios' && (
          <div className="flex items-center gap-1 font-mono text-[11px]">
            {['all', 'HYPOTHESIS', 'CONFIRMED', 'MONITORING'].map((status) => (
              <button
                key={status}
                onClick={() => setScenarioStatus(status)}
                className={`px-2 py-0.5 rounded uppercase font-bold cursor-pointer ${
                  scenarioStatus === status
                    ? 'bg-purple-500/20 text-purple-300 border border-purple-500/40'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {status}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* TAB CONTENT: Live Stream */}
      {activeTab === 'events' && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {sortedEvents.length === 0 ? (
            <div className="col-span-full p-8 text-center bg-slate-950/60 rounded-xl border border-slate-800/80 font-mono text-slate-400 text-xs">
              <span className="h-2 w-2 rounded-full bg-cyan-400 inline-block animate-ping mr-2" />
              AWAITING LIVE DATA STREAM...
            </div>
          ) : (
            sortedEvents.map((e) => (
              <EventRow key={e.event_id} e={e} onClick={handleEventClick} />
            ))
          )}
        </div>
      )}

      {/* TAB CONTENT: AI Scenarios */}
      {activeTab === 'scenarios' && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {(!scenarios || scenarios.length === 0) ? (
            <div className="col-span-full p-8 text-center bg-slate-950/60 rounded-xl border border-slate-800/80 font-mono text-slate-400 text-xs">
              NO ACTIVE SCENARIOS FOUND.
            </div>
          ) : (
            scenarios.map((s) => (
              <div
                key={s.scenario_id}
                onClick={() => setSelectedScenario(s)}
                className="p-4 rounded-xl bg-[#0d0a18] border border-purple-500/20 hover:border-purple-500/60 transition-all cursor-pointer space-y-2 group glass-panel-hover font-mono"
              >
                <div className="flex items-center justify-between text-[11px]">
                  <span className="px-2 py-0.5 rounded bg-purple-500/20 text-purple-300 font-bold uppercase border border-purple-500/40">
                    {s.status || 'HYPOTHESIS'}
                  </span>
                  <span className="text-emerald-400 font-bold">
                    CONFIDENCE: {s.confidence_overall}%
                  </span>
                </div>
                <h3 className="text-xs font-bold text-slate-100 group-hover:text-purple-300 transition-colors font-sans line-clamp-2 leading-snug">
                  {s.headline}
                </h3>
                <p className="text-[11px] text-slate-400 font-sans line-clamp-2 leading-tight">
                  {s.narrative || s.description}
                </p>
                <div className="pt-2 flex items-center justify-between text-[10px] text-slate-400 border-t border-purple-500/10">
                  <span>Entity: <span className="text-amber-300 font-bold">{s.primary_entity_name || 'Multi-Entity'}</span></span>
                  <span>{new Date(s.updated_at || s.created_at).toLocaleTimeString()}</span>
                </div>
              </div>
            ))
          )}
        </div>
      )}

      {/* TAB CONTENT: Raw Correlations */}
      {activeTab === 'correlations' && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {(!correlations || correlations.length === 0) ? (
            <div className="col-span-full p-8 text-center bg-slate-950/60 rounded-xl border border-slate-800/80 font-mono text-slate-400 text-xs">
              NO CORRELATION CLUSTERS DETECTED YET.
            </div>
          ) : (
            correlations.map((c) => (
              <CorrelationCard key={c.correlation_id} c={c} />
            ))
          )}
        </div>
      )}

      {/* Event Detail Forensic Inspector Modal */}
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

              {selectedEvent.summary && (
                <div>
                  <span className="text-slate-400 block mb-0.5">EXECUTIVE SUMMARY & AGENTIC CONTEXT:</span>
                  <p className="text-slate-200 font-sans text-xs bg-slate-950 p-3 rounded-lg border border-cyan-500/20 leading-relaxed">
                    {selectedEvent.summary}
                  </p>
                </div>
              )}

              <div className="grid grid-cols-2 gap-2 bg-slate-950 p-3 rounded-lg border border-slate-800">
                <div><span className="text-slate-400">EVENT ID:</span> <span className="text-cyan-400 font-bold block truncate">{selectedEvent.event_id}</span></div>
                <div><span className="text-slate-400">SOURCE:</span> <span className="text-cyan-400 font-bold block">{getCleanSource(selectedEvent) || 'unattributed'}</span></div>
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
                  <div className="max-h-64 overflow-y-auto">
                  <DataGrid data={fullEventDetail || selectedEvent} omit={['raw_payload']} />
                </div>
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
    </div>
  );
}