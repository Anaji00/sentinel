'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { useLiveEvents } from '../lib/useLiveEvents';
import { NormalizedEvent, Scenario } from '../lib/types';
import { resolveEventDomain } from '../lib/domain';
import { recordInteraction } from '../lib/interactions';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { DataGrid } from './ui/DataGrid';
import { formatPercent } from '../lib/format';
import { IconChevronDown, IconModel } from '@/components/ui/icons';
import { ClockTime } from './ui/ClockTime';
import { POLL } from './ui/DataProvider';
import { useDialog } from './ui/useDialog';
import { SimilarEvents } from './SimilarEvents';
import { RuleVerdict } from './RuleVerdict';

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
export function domainMetaFor(e: NormalizedEvent): {
  label: string;
  icon: string;
  badgeStyle: string;
} {
  // One resolver, shared with the live-feed tab filter. These were two
  // separate answers to the same question -- this one authoritative, the
  // hook's a substring guess -- and they disagreed on every Polymarket row.
  const exemplar: Record<string, string> = {
    crypto: 'crypto_trade',
    prediction: 'prediction_market_trade',
    maritime: 'vessel_position',
    aviation: 'flight_position',
    cyber: 'bgp_anomaly',
    tradfi: 'equity_block',
    news: 'headline',
  };
  const resolved = resolveEventDomain(e);
  if (resolved) return getDomainMeta(exemplar[resolved]);
  // Nothing declared and no payload: the type is all there is, which is the
  // case the server change exists to prevent.
  return getDomainMeta(e.type);
}

export function getDomainMeta(type: string): { label: string; icon: string; badgeStyle: string } {
  const t = (type || '').toLowerCase();
  if (t.includes('pred') || t.includes('poly') || t.includes('kalshi')) {
    return {
      label: 'PREDICTION',
      icon: '',
      badgeStyle: 'text-purple-400 border-purple-500/40 bg-purple-500/10',
    };
  }
  if (
    t.includes('crypto') ||
    t.includes('coinbase') ||
    t.includes('binance') ||
    t.includes('token') ||
    t.includes('wallet') ||
    t.includes('blockchain') ||
    t.includes('perp') ||
    t.includes('btc') ||
    t.includes('eth') ||
    t.includes('sol')
  ) {
    return {
      label: 'CRYPTO',
      icon: '₿',
      badgeStyle: 'text-amber-400 border-amber-500/40 bg-amber-500/10',
    };
  }
  if (t.includes('earnings')) {
    return {
      label: 'EARNINGS',
      icon: '',
      badgeStyle: 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10',
    };
  }
  if (t.includes('funding')) {
    return {
      label: 'PERP FUNDING',
      icon: '',
      badgeStyle: 'text-purple-400 border-purple-500/40 bg-purple-500/10',
    };
  }
  if (t.includes('vessel') || t.includes('maritime') || t.includes('ais')) {
    return {
      label: 'MARITIME',
      icon: '',
      badgeStyle: 'text-cyan-400 border-cyan-500/40 bg-cyan-500/10',
    };
  }
  if (t.includes('flight') || t.includes('aviation') || t.includes('adsb')) {
    return {
      label: 'AVIATION',
      icon: '',
      badgeStyle: 'text-blue-400 border-blue-500/40 bg-blue-500/10',
    };
  }
  if (t.includes('cyber') || t.includes('bgp') || t.includes('breach')) {
    return {
      label: 'CYBER',
      icon: '',
      badgeStyle: 'text-rose-400 border-rose-500/40 bg-rose-500/10',
    };
  }
  if (
    t.includes('tradfi') ||
    t.includes('stock') ||
    t.includes('equity') ||
    t.includes('option') ||
    t.includes('market')
  ) {
    return {
      label: 'TRADFI',
      icon: '',
      badgeStyle: 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10',
    };
  }
  return { label: 'NEWS', icon: '', badgeStyle: 'text-ink-dim border-line-strong bg-overlay' };
}

// Helper to derive clean source name
export function getCleanSource(e: NormalizedEvent): string {
  if (e.source && e.source !== 'unknown' && !e.source.startsWith('Event')) {
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
  if (
    rawHeadline &&
    !rawHeadline.startsWith('Event') &&
    !rawHeadline.match(/^[0-9a-f]{8}-[0-9a-f]{4}/i)
  ) {
    return rawHeadline.replace(/tradfi\s*[|\-]\s*prediction_market/gi, 'Prediction Market');
  }

  // Priority 2: If summary is present and rich, use it
  if (e.summary && e.summary.length > 15 && !e.summary.startsWith('Event')) {
    return e.summary;
  }

  if (e.prediction_market_data?.question) {
    const pm = e.prediction_market_data;
    const sideStr = pm.outcome ? `(${pm.outcome})` : '';
    const qUpper = (pm.question || '').toUpperCase();
    return `PREDICTION MARKET: ${qUpper}${sideStr}`;
  }

  const entityName = e.primary_entity_name || e.entity_name || e.primary_entity?.name || '';
  const t = (e.type || '').toLowerCase();
  const regionStr = e.region ? `in ${e.region}` : '';

  return `${getDomainMeta(e.type).label} Intelligence Event: ${entityName || 'Target'}${regionStr}`;
}

const getScoreBadge = (score: number) => {
  if (score >= 0.75)
    return (
      <span className="px-2 py-0.5 rounded text-micro font-mono font-bold bg-rose-500/20 text-rose-400 border border-rose-500/40 glow-crimson">
        CRITICAL {score.toFixed(2)}
      </span>
    );
  if (score >= 0.5)
    return (
      <span className="px-2 py-0.5 rounded text-micro font-mono font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40 glow-amber">
        ELEVATED {score.toFixed(2)}
      </span>
    );
  return (
    <span className="px-2 py-0.5 rounded text-micro font-mono font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/40">
      NORMAL {score.toFixed(2)}
    </span>
  );
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
      ? `— second source ${Math.round(c.minutes_to_corroboration)} min later`
      : '';
  const syndicated = c.is_syndicated
    ? 'Some reports share wording, which suggests syndication rather than independent confirmation.'
    : '';

  return (
    <span
      className={`badge ${c.is_syndicated ? 'tone-info' : 'tone-positive'}`}
      title={`${c.contributing_sources.slice(0, 6).join(',')}${timing}.${syndicated}`}
    >
      {c.independent_sources} sources
    </span>
  );
}

const EventRow = React.memo(
  ({ e, onClick }: { e: NormalizedEvent; onClick: (e: NormalizedEvent) => void }) => {
    const domainMeta = domainMetaFor(e);
    const sourceName = getCleanSource(e);
    const title = formatEnglishHeadline(e);
    const entityName =
      e.primary_entity_name || e.entity_name || e.primary_entity?.name || 'Unknown Entity';

    const fd = e.financial_data || {};
    const cd = e.crypto_data || {};

    // The denominator. "The 0.9 band was opened 40 times" is meaningless until
    // something records how many times it was shown, and nothing did.
    // De-duplicated per alert inside recordInteraction, so a re-render or an SWR
    // revalidation does not turn this into a count of React renders.
    React.useEffect(() => {
      recordInteraction('surfaced', e.anomaly_score, { correlationId: e.event_id });
    }, [e.event_id, e.anomaly_score]);

    return (
      <div
        onClick={() => onClick(e)}
        className="p-3 rounded-lg bg-raised/60 border border-cyan-500/15 hover:border-accent/50 cursor-pointer transition-all hover:bg-raised/90 group glass-panel-hover"
      >
        <div className="flex items-center justify-between mb-1.5 text-micro">
          <div className="flex items-center gap-1.5 flex-wrap">
            <span
              className={`px-1.5 py-0.5 rounded border font-bold uppercase ${domainMeta.badgeStyle}`}
            >
              {domainMeta.icon} {domainMeta.label}
            </span>
            <CorroborationBadge e={e} />
            {fd.option_type && (
              <span
                className={`px-1.5 py-0.5 rounded border font-extrabold text-micro ${
                  fd.option_type === 'CALL'
                    ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'
                    : 'bg-rose-500/20 text-rose-300 border-rose-500/40'
                }`}
              >
                {fd.option_type}
              </span>
            )}
            {fd.eps_surprise_pct !== undefined && fd.eps_surprise_pct !== null && (
              <span
                className={`px-1.5 py-0.5 rounded border font-extrabold text-micro ${
                  fd.eps_surprise_pct >= 0
                    ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'
                    : 'bg-rose-500/20 text-rose-300 border-rose-500/40'
                }`}
              >
                EPS {formatPercent(fd.eps_surprise_pct, { decimals: 1, signed: true })}
              </span>
            )}
            {cd.funding_rate !== undefined && (
              <span className="px-1.5 py-0.5 rounded border border-purple-500/40 bg-purple-500/20 text-purple-300 font-extrabold text-micro">
                {formatPercent(cd.funding_rate, { from: 'ratio', decimals: 4 })} RATE
              </span>
            )}
            {sourceName && (
              <span className="text-ink-dim font-medium">
                via <span className="text-cyan-400 font-bold">{sourceName}</span>
              </span>
            )}
          </div>
          {getScoreBadge(e.anomaly_score)}
        </div>

        <p className="text-xs text-ink font-sans font-semibold line-clamp-2 group-hover:text-white transition-colors leading-snug">
          {title}
        </p>

        {e.summary && (
          <p className="text-micro text-ink-dim font-sans mt-1 line-clamp-2 leading-tight">
            {e.summary}
          </p>
        )}

        <div className="mt-2 flex items-center justify-between text-micro text-ink-dim pt-1 border-t border-line/60">
          <span>
            Entity: <span className="text-amber-300 font-bold">{entityName}</span>
          </span>
          <span>
            <ClockTime value={e.occurred_at} />
          </span>
        </div>
      </div>
    );
  },
);
EventRow.displayName = 'EventRow';

const CorrelationCard = React.memo(({ c }: { c: CorrelationCluster }) => {
  const [showEvidence, setShowEvidence] = useState(false);
  const trail = c.evidence_trail || [];

  return (
    <div className="p-3 rounded-lg bg-raised/80 border border-amber-500/20 hover:border-amber-400/60 transition-all space-y-1.5">
      <div className="flex items-center justify-between">
        <span className="font-bold text-amber-400 uppercase text-xs flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-amber-400 animate-ping" />
          RULE: {c.rule_name}
        </span>
        <span className="px-2 py-0.5 rounded text-micro font-bold bg-amber-500/20 text-amber-300 border border-amber-500/40">
          TIER {c.alert_tier}
        </span>
      </div>
      <p className="text-xs text-ink font-sans leading-snug">{c.description}</p>

      {/* The first step of a loop that had every other step built: the
          scorecard, the aggregate, the console panel and the retirement agent
          all existed, and nothing in the product ever recorded a verdict. */}
      <RuleVerdict correlationId={c.correlation_id} ruleName={c.rule_name} />

      {trail.length > 0 && (
        <div className="pt-1">
          <button
            onClick={() => setShowEvidence(!showEvidence)}
            className="text-micro text-cyan-400 hover:text-cyan-300 font-bold flex items-center gap-1 cursor-pointer"
          >
            <IconChevronDown
              className={`transition-transform ${showEvidence ? '' : '-rotate-90'}`}
            />
            <span>{showEvidence ? 'Hide evidence trail' : 'Show evidence trail'}</span>
            <span className="text-ink-dim font-normal">({trail.length} signals)</span>
          </button>
          {showEvidence && (
            <div className="mt-1.5 p-2 bg-page/80 rounded border border-cyan-500/20 space-y-1 text-micro">
              <span className="text-cyan-300 font-bold block mb-1">
                EVIDENCE TRAIL (SWARM FUSION):
              </span>
              {trail.map((item, idx) => (
                <div
                  key={idx}
                  className="flex items-center justify-between border-b border-line/60 pb-0.5"
                >
                  <span className="text-ink font-bold">{item.agent_name}</span>
                  <span className="text-ink-dim">
                    Dir:{' '}
                    <span className="text-amber-300 font-bold">{item.direction || 'neutral'}</span>{' '}
                    | Score:{' '}
                    <span className="text-emerald-400 font-bold">
                      {(item.conviction ?? item.score ?? 0).toFixed(2)}
                    </span>{' '}
                    | Weight:{' '}
                    <span className="text-cyan-400">{(item.weight ?? 1.0).toFixed(2)}</span>
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      <div className="flex items-center justify-between text-micro text-ink-dim border-t border-line/80 pt-1">
        <span>TAGS: {c.tags?.join(',') || 'MULTI-DOMAIN'}</span>
        <span>
          <ClockTime value={c.detected_at} />
        </span>
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
  const scenarioUrl =
    scenarioStatus === 'all'
      ? '/scenarios?limit=20'
      : `/scenarios?limit=20&status=${encodeURIComponent(scenarioStatus)}`;

  const { data: scenarios, error: scenariosError } = useSWR<Scenario[]>(scenarioUrl, fetcher, {
    refreshInterval: POLL.live,
  });

  // Fetch Raw Correlation Clusters
  const { data: correlations, error: correlationsError } = useSWR<CorrelationCluster[]>(
    '/correlations?limit=30&min_tier=1',
    fetcher,
    { refreshInterval: POLL.live },
  );

  // Dynamic Event domain fetches
  const { data: tradfiEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'tradfi' ? '/events/tradfi?limit=30' : null,
    fetcher,
    { refreshInterval: POLL.live },
  );
  const { data: cryptoEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'crypto' ? '/events/crypto?limit=30' : null,
    fetcher,
    { refreshInterval: POLL.live },
  );
  const { data: predictionEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'prediction'
      ? '/events/prediction?limit=30'
      : null,
    fetcher,
    { refreshInterval: POLL.standard },
  );
  // No cyber fetch. The domain is retired -- `RETIRED_DOMAINS` names it and the
  // collector is behind a compose profile that does not start -- so this was a
  // request every eight seconds, on the default view, whose only possible
  // answer was an empty array. Removing the filter tab alone would have left
  // the polling behind it.
  const { data: maritimeEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'maritime' ? '/events/maritime?limit=30' : null,
    fetcher,
    { refreshInterval: POLL.standard },
  );

  // Merge events with zero-latency WebSocket stream, deduplicating by event_id
  const sortedEvents = useMemo(() => {
    const rawEvents: NormalizedEvent[] = [...wsLiveEvents];
    if (selectedDomain === 'all' || selectedDomain === 'tradfi')
      rawEvents.push(...(tradfiEvents || []));
    if (selectedDomain === 'all' || selectedDomain === 'crypto')
      rawEvents.push(...(cryptoEvents || []));
    if (selectedDomain === 'all' || selectedDomain === 'prediction')
      rawEvents.push(...(predictionEvents || []));
    if (selectedDomain === 'all' || selectedDomain === 'maritime')
      rawEvents.push(...(maritimeEvents || []));

    const deduped = Array.from(new Map(rawEvents.map((e) => [e.event_id, e])).values());

    return deduped
      .sort((a, b) => new Date(b.occurred_at).getTime() - new Date(a.occurred_at).getTime())
      .filter((e) =>
        searchQuery
          ? (e.headline || e.type || formatEnglishHeadline(e))
              .toLowerCase()
              .includes(searchQuery.toLowerCase()) ||
            (e.source || '').toLowerCase().includes(searchQuery.toLowerCase()) ||
            (e.primary_entity?.name || '').toLowerCase().includes(searchQuery.toLowerCase())
          : true,
      )
      .slice(0, 45);
  }, [
    wsLiveEvents,
    tradfiEvents,
    cryptoEvents,
    predictionEvents,
    maritimeEvents,
    selectedDomain,
    searchQuery,
  ]);

  const mainTabs = [
    { id: 'events', label: 'Events', count: sortedEvents.length },
    { id: 'scenarios', label: 'Scenarios', count: scenarios?.length || 0 },
    { id: 'correlations', label: 'Correlations', count: correlations?.length || 0 },
  ];

  // Cyber is gone from here because it is gone from the platform.
  //
  // `RETIRED_DOMAINS` names it, the collector is behind a compose profile that
  // does not start by default, and no cyber event has been produced since. The
  // filter stayed, so a user could select a domain whose only possible answer
  // was zero events -- which reads as "nothing happening in cyber" rather than
  // "there is no cyber". A retirement that reaches the model and the collector
  // and not the UI is still visible to the person using it.
  const domainTabs = [
    { id: 'all', label: 'All' },
    { id: 'tradfi', label: 'TradFi' },
    { id: 'crypto', label: 'Crypto' },
    { id: 'prediction', label: 'Prediction' },
    { id: 'maritime', label: 'AIS' },
  ];

  const [selectedScenario, setSelectedScenario] = useState<Scenario | null>(null);

  // Two overlays, one component. Both were `fixed inset-0` with no role,
  // no focus trap and no Escape -- opening an event detail put a keyboard
  // user inside a surface they could tab out of but not close.
  const eventDialog = useDialog(
    Boolean(selectedEvent),
    () => {
      setSelectedEvent(null);
      setFullEventDetail(null);
    },
    'Event detail',
  );
  const scenarioDialog = useDialog(
    Boolean(selectedScenario),
    () => setSelectedScenario(null),
    'Scenario review',
  );

  // The evidence trail belongs to the correlation, not to the scenario.
  //
  // The modal read `selectedScenario.evidence_trail`, and /scenarios returns
  // rows of the `scenarios` table, which has no such column and never has --
  // so "EVIDENCE TRAIL (SWARM FUSION)" has never appeared for any scenario.
  // The trail it wanted is real and already on this page: /correlations builds
  // it per cluster, and a scenario names the cluster it was written from.
  const selectedScenarioTrail = useMemo<EvidenceContributor[]>(() => {
    if (!selectedScenario) return [];
    return (
      correlations?.find((c) => c.correlation_id === selectedScenario.correlation_id)
        ?.evidence_trail || []
    );
  }, [selectedScenario, correlations]);

  // Handle Event Click to fetch deep detail payload from backend
  const handleEventClick = async (event: NormalizedEvent) => {
    // Opening an alert is the ordinary signal the calibration loop never had.
    // `surfaced` is recorded by the row itself; this is the other half.
    recordInteraction('opened', event.anomaly_score, { correlationId: event.event_id });
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
      const response = await apiClient.get(`/events/detail/${encodeURIComponent(event.event_id)}`);
      setFullEventDetail(response.data);
    } catch (err) {
      console.warn(
        'Could not fetch deep event detail from hypertable, using live payload fallback:',
        err,
      );
      setFullEventDetail(event);
    } finally {
      setIsLoadingDetail(false);
    }
  };

  return (
    <div className="space-y-4 max-w-[1600px] mx-auto font-sans">
      {/* Header Bar */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 p-4 bg-raised border border-cyan-500/20 rounded-xl shadow-panel">
        <div>
          {/* The page already carries the title. This was a second <h1> with a
              louder version of the same words directly beneath the first -- two
              headings, one page, and an accessibility tree with two h1s. */}
          <p className="text-xs text-ink-dim flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-cyan-400 animate-pulse shadow-panel" />
            Events, scenarios and correlation clusters, as they arrive.
          </p>
        </div>

        {/* Search & Filter Inputs */}
        <div className="flex items-center gap-2">
          <input
            type="text"
            placeholder="Search events, tickers, entities..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="px-3 py-1.5 rounded-lg bg-page border border-line text-xs text-ink focus:outline-none focus:border-cyan-500/60 font-mono w-48 md:w-64"
          />
        </div>
      </div>

      {/* Main Mode Tabs */}
      <div className="flex items-center justify-between gap-2 border-b border-line pb-2">
        <div className="flex items-center gap-2">
          {mainTabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as any)}
              className={`px-3 py-1.5 rounded-lg font-mono text-xs font-bold transition-all cursor-pointer flex items-center gap-2 ${
                activeTab === tab.id
                  ? 'bg-cyan-500/20 text-accent border border-cyan-500/40 shadow-panel'
                  : 'bg-raised/60 text-ink-dim border border-line hover:text-ink'
              }`}
            >
              {tab.label}
              <span className="px-1.5 py-0.2 rounded bg-page text-micro font-mono text-cyan-300 font-bold border border-cyan-500/20">
                {tab.count}
              </span>
            </button>
          ))}
        </div>

        {/* Sub-domain filters (Only visible in Live Stream tab) */}
        {activeTab === 'events' && (
          <div className="flex items-center gap-1 overflow-x-auto py-1 text-micro">
            {domainTabs.map((d) => (
              <button
                key={d.id}
                onClick={() => setSelectedDomain(d.id)}
                className={`px-2.5 py-1 rounded transition-colors cursor-pointer font-bold ${
                  selectedDomain === d.id
                    ? 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/40'
                    : 'text-ink-dim hover:text-ink hover:bg-raised'
                }`}
              >
                {d.label}
              </button>
            ))}
          </div>
        )}

        {/* Scenario Status filter (Only visible in AI Scenarios tab) */}
        {activeTab === 'scenarios' && (
          <div className="flex items-center gap-1 text-micro">
            {['all', 'HYPOTHESIS', 'CONFIRMED', 'MONITORING'].map((status) => (
              <button
                key={status}
                onClick={() => setScenarioStatus(status)}
                className={`px-2 py-0.5 rounded uppercase font-bold cursor-pointer ${
                  scenarioStatus === status
                    ? 'bg-purple-500/20 text-purple-300 border border-purple-500/40'
                    : 'text-ink-dim hover:text-ink'
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
            <div className="col-span-full p-8 text-center bg-page/60 rounded-xl border border-line/80 text-ink-dim text-xs">
              <span className="h-2 w-2 rounded-full bg-cyan-400 inline-block animate-ping mr-2" />
              Waiting for the first event. The stream is connected and quiet, which before the open
              is the correct answer.
            </div>
          ) : (
            sortedEvents.map((e) => <EventRow key={e.event_id} e={e} onClick={handleEventClick} />)
          )}
        </div>
      )}

      {/* TAB CONTENT: AI Scenarios */}
      {activeTab === 'scenarios' && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {!scenarios || scenarios.length === 0 ? (
            <div className="col-span-full p-8 text-center bg-page/60 rounded-xl border border-line/80 text-ink-dim text-xs">
              {/* Not the same statement as "the request failed", and this line
                  was making it for both. The status filter returned a 500 on
                  every request for as long as it existed -- the SQL called
                  Cypher's toLower() -- and each of the four status tabs
                  reported that the platform had nothing to say. */}
              {describeApiError(scenariosError) ?? 'No active scenarios.'}
            </div>
          ) : (
            scenarios.map((s) => (
              <div
                key={s.scenario_id}
                onClick={() => setSelectedScenario(s)}
                className="p-4 rounded-xl bg-[#0d0a18] border border-purple-500/20 hover:border-purple-500/60 transition-all cursor-pointer space-y-2 group glass-panel-hover font-mono"
              >
                <div className="flex items-center justify-between text-micro">
                  <span className="px-2 py-0.5 rounded bg-purple-500/20 text-purple-300 font-bold uppercase border border-purple-500/40">
                    {s.status || 'HYPOTHESIS'}
                  </span>
                  <span className="text-emerald-400 font-bold">
                    CONFIDENCE: {s.confidence_overall}%
                  </span>
                </div>
                <h3 className="text-xs font-bold text-ink group-hover:text-purple-300 transition-colors font-sans line-clamp-2 leading-snug">
                  {s.headline}
                </h3>
                <p className="text-micro text-ink-dim font-sans line-clamp-2 leading-tight">
                  {s.narrative_summary}
                </p>
                <div className="pt-2 flex items-center justify-between text-micro text-ink-dim border-t border-purple-500/10">
                  <span>
                    Entity:{' '}
                    <span className="text-amber-300 font-bold">
                      {s.primary_entity_name || 'Multi-Entity'}
                    </span>
                  </span>
                  <span>
                    <ClockTime value={s.updated_at || s.created_at} />
                  </span>
                </div>
              </div>
            ))
          )}
        </div>
      )}

      {/* TAB CONTENT: Raw Correlations */}
      {activeTab === 'correlations' && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {!correlations || correlations.length === 0 ? (
            <div className="col-span-full p-8 text-center bg-page/60 rounded-xl border border-line/80 text-ink-dim text-xs">
              {describeApiError(correlationsError) ?? 'No correlation clusters yet.'}
            </div>
          ) : (
            correlations.map((c) => <CorrelationCard key={c.correlation_id} c={c} />)
          )}
        </div>
      )}

      {/* Event Detail Forensic Inspector Modal */}
      {selectedEvent && (
        <div
          className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4"
          {...eventDialog.overlayProps}
        >
          <div
            className="bg-raised border border-accent/40 rounded-xl max-w-2xl w-full p-5 space-y-4 shadow-panel max-h-[85vh] overflow-y-auto"
            {...eventDialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-cyan-500/20 pb-3">
              <div className="flex items-center gap-2">
                <span
                  className={`px-2 py-0.5 rounded text-micro font-bold border ${getDomainMeta(selectedEvent.type).badgeStyle}`}
                >
                  {getDomainMeta(selectedEvent.type).icon} {getDomainMeta(selectedEvent.type).label}
                </span>
                <span className="text-xs font-semibold text-accent">Event detail</span>
              </div>
              <button
                onClick={() => {
                  setSelectedEvent(null);
                  setFullEventDetail(null);
                }}
                className="text-ink-dim hover:text-white text-sm font-bold px-2 py-0.5 rounded bg-overlay cursor-pointer"
              >
                CLOSE
              </button>
            </div>

            <div className="space-y-3 text-xs">
              <div>
                <span className="text-ink-dim block mb-0.5">EVENT HEADLINE:</span>
                <p className="text-white font-bold font-sans text-sm">
                  {formatEnglishHeadline(selectedEvent)}
                </p>
              </div>

              {selectedEvent.summary && (
                <div>
                  <span className="text-ink-dim block mb-0.5">Summary</span>
                  <p className="text-ink font-sans text-xs bg-page p-3 rounded-lg border border-cyan-500/20 leading-relaxed">
                    {selectedEvent.summary}
                  </p>
                </div>
              )}

              {/* The corpus has held an embedding for every enriched event
                  since the platform was built, and until now nothing could ask
                  it anything. This is the question the index exists for. */}
              <div className="rounded-lg border border-line bg-page p-3">
                <SimilarEvents
                  eventId={selectedEvent.event_id}
                  domain={resolveEventDomain(selectedEvent)}
                />
              </div>

              <div className="grid grid-cols-2 gap-2 bg-page p-3 rounded-lg border border-line">
                <div>
                  <span className="text-ink-dim">EVENT ID:</span>{' '}
                  <span className="text-cyan-400 font-bold block truncate">
                    {selectedEvent.event_id}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">SOURCE:</span>{' '}
                  <span className="text-cyan-400 font-bold block">
                    {getCleanSource(selectedEvent) || 'unattributed'}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">ANOMALY SCORE:</span>{' '}
                  <span className="text-amber-400 font-bold block">
                    {selectedEvent.anomaly_score.toFixed(2)}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">TIMESTAMP:</span>{' '}
                  <span className="text-ink block">
                    {new Date(selectedEvent.occurred_at).toUTCString()}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">PRIMARY ENTITY:</span>{' '}
                  <span className="text-emerald-400 font-bold block">
                    {selectedEvent.primary_entity_name ||
                      selectedEvent.entity_name ||
                      selectedEvent.primary_entity?.name ||
                      'N/A'}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">REGION:</span>{' '}
                  <span className="text-purple-400 font-bold block">
                    {selectedEvent.region || 'GLOBAL'}
                  </span>
                </div>
              </div>

              {/* Full JSON Payload from TimescaleDB */}
              <div>
                <span className="text-cyan-400 font-bold block mb-1">
                  DEEP DATABASE JSON PAYLOAD ({isLoadingDetail ? 'FETCHING...' : 'LIVE'}):
                </span>
                {isLoadingDetail ? (
                  <div className="p-4 bg-page rounded border border-cyan-500/20 text-ink-dim text-center animate-pulse">
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
                onClick={() => {
                  setSelectedEvent(null);
                  setFullEventDetail(null);
                }}
                className="w-full py-2 bg-raised text-accent border border-cyan-500/30 rounded-lg text-xs font-bold hover:bg-overlay transition-colors cursor-pointer"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Scenario Detail Modal */}
      {selectedScenario && (
        <div
          className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4"
          {...scenarioDialog.overlayProps}
        >
          <div
            className="bg-[#0c0914] border border-purple-500/50 rounded-xl max-w-2xl w-full p-5 space-y-4 shadow-panel max-h-[85vh] overflow-y-auto"
            {...scenarioDialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-purple-500/30 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-xl">
                  <IconModel className="inline-block shrink-0" />
                </span>
                <span className="text-xs font-bold text-purple-300 uppercase tracking-wider">
                  Scenario review
                </span>
              </div>
              <button
                onClick={() => setSelectedScenario(null)}
                className="text-ink-dim hover:text-white text-sm font-bold px-2 py-0.5 rounded bg-overlay cursor-pointer"
              >
                CLOSE
              </button>
            </div>

            <div className="space-y-3 text-xs">
              <div className="flex items-center justify-between bg-purple-950/40 p-3 rounded-lg border border-purple-500/30">
                <div>
                  <span className="text-ink-dim block text-micro">SCENARIO STATUS:</span>
                  <span className="text-purple-300 font-bold text-sm">
                    {selectedScenario.status || 'HYPOTHESIS'}
                  </span>
                </div>
                <div className="text-right">
                  <span className="text-ink-dim block text-micro">CONFIDENCE SCORE:</span>
                  <span className="text-emerald-400 font-extrabold text-sm">
                    {selectedScenario.confidence_overall}%
                  </span>
                </div>
              </div>

              <div>
                <span className="text-purple-400 font-bold block mb-1">HEADLINE:</span>
                <h3 className="text-sm font-bold text-white font-sans">
                  {selectedScenario.headline}
                </h3>
              </div>

              {selectedScenario.primary_entity_name && (
                <div>
                  <span className="text-ink-dim block mb-0.5">PRIMARY ENTITY:</span>
                  <span className="text-amber-300 font-bold">
                    {selectedScenario.primary_entity_name}
                  </span>
                </div>
              )}

              <div>
                <span className="text-ink-dim block mb-1">STRATEGIC SIGNIFICANCE:</span>
                <p className="text-ink bg-page p-3 rounded border border-purple-500/20 leading-relaxed font-sans">
                  {selectedScenario.significance}
                </p>
              </div>

              {selectedScenario.confidence_rationale && (
                <div>
                  <span className="text-ink-dim block mb-1">CONFIDENCE RATIONALE:</span>
                  <p className="text-ink-dim bg-page p-3 rounded border border-line text-micro leading-relaxed">
                    {selectedScenario.confidence_rationale}
                  </p>
                </div>
              )}

              {selectedScenarioTrail.length > 0 && (
                <div>
                  <span className="text-cyan-400 font-bold block mb-1">
                    EVIDENCE TRAIL (SWARM FUSION):
                  </span>
                  <div className="p-2.5 bg-page rounded border border-cyan-500/20 space-y-1 text-micro">
                    {selectedScenarioTrail.map((item, idx) => (
                      <div
                        key={idx}
                        className="flex items-center justify-between border-b border-line pb-1"
                      >
                        <span className="text-ink font-bold">{item.agent_name}</span>
                        <span className="text-ink-dim">
                          Dir:{' '}
                          <span className="text-amber-300 font-bold">
                            {item.direction || 'neutral'}
                          </span>{' '}
                          | Conviction:{' '}
                          <span className="text-emerald-400 font-bold">
                            {(item.conviction ?? item.score ?? 0).toFixed(2)}
                          </span>
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
