'use client';

import React, { useMemo, useState } from 'react';
import { Input } from './ui/Input';
import useSWR from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { useLiveEvents } from '../lib/useLiveEvents';
import { NormalizedEvent, Scenario } from '../lib/types';
import { resolveEventDomain } from '../lib/domain';
import { companyNewsTone, entityLabel, type NewsTone } from '../lib/eventBadge';
import { recordInteraction } from '../lib/interactions';
import { DataGrid } from './ui/DataGrid';
import { decodeEntities, formatPercent } from '../lib/format';
import { IconChevronDown, IconModel } from '@/components/ui/icons';
import { ClockTime } from './ui/ClockTime';
import { POLL } from './ui/DataProvider';
import { useDialog } from './ui/useDialog';
import { SimilarEvents } from './SimilarEvents';
import { RuleVerdict } from './RuleVerdict';
import { withinHours } from '../lib/timeWindow';

// Scores arrive null on some rows; a null here crashed the whole view.
function fixed(value: number | null | undefined, digits: number): string {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '—';
}


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
      badgeStyle: 'text-accent border-line-accent bg-accent-dim',
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
      icon: '',
      badgeStyle: 'text-caution border-caution/30 bg-caution/10',
    };
  }
  if (t.includes('earnings')) {
    return {
      label: 'EARNINGS',
      icon: '',
      badgeStyle: 'text-positive border-positive/30 bg-positive/10',
    };
  }
  if (t.includes('funding')) {
    return {
      label: 'FUNDING RATE',
      icon: '',
      badgeStyle: 'text-accent border-line-accent bg-accent-dim',
    };
  }
  if (t.includes('vessel') || t.includes('maritime') || t.includes('ais')) {
    return {
      label: 'SHIPS',
      icon: '',
      badgeStyle: 'text-accent border-line-accent bg-accent-dim',
    };
  }
  if (t.includes('flight') || t.includes('aviation') || t.includes('adsb')) {
    return {
      label: 'FLIGHTS',
      icon: '',
      badgeStyle: 'text-accent border-accent/40 bg-accent/10',
    };
  }
  if (t.includes('cyber') || t.includes('bgp') || t.includes('breach')) {
    return {
      label: 'CYBER',
      icon: '',
      badgeStyle: 'text-negative border-negative/30 bg-negative/10',
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
      label: 'MARKETS',
      icon: '',
      badgeStyle: 'text-positive border-positive/30 bg-positive/10',
    };
  }
  return { label: 'NEWS', icon: '', badgeStyle: 'text-ink-dim border-line-strong bg-overlay' };
}

// Helper to derive clean source name
export function getCleanSource(e: NormalizedEvent): string {
  if (e.source && e.source !== 'unknown' && !e.source.startsWith('Event')) {
    return e.source.replace(/_/g, ' ');
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
    return decodeEntities(rawHeadline.replace(/tradfi\s*[|\-]\s*prediction_market/gi, 'Prediction Market'));
  }

  // Priority 2: If summary is present and rich, use it
  if (e.summary && e.summary.length > 15 && !e.summary.startsWith('Event')) {
    return decodeEntities(e.summary);
  }

  if (e.prediction_market_data?.question) {
    const pm = e.prediction_market_data;
    const sideStr = pm.outcome ? `(${pm.outcome})` : '';
    return `Prediction market: ${pm.question} ${sideStr}`.trim();
  }

  const entityName = e.primary_entity_name || e.entity_name || e.primary_entity?.name || '';
  const regionStr = e.region ? ` in ${e.region}` : '';

  return `${entityName || `${getDomainMeta(e.type).label.toLowerCase()} event`}${regionStr}`;
}

// Red is kept for the rare event: at 0.75 nearly every ship report in the feed
// read CRITICAL, and a colour on every card tells the reader nothing. Below the
// elevated line a score is routine, shown in grey -- green already means "good"
// or "bullish" everywhere else on the platform.
const SCORE_CRITICAL = 0.9;
const SCORE_ELEVATED = 0.7;

const getScoreBadge = (score: number) => {
  if (score >= SCORE_CRITICAL)
    return (
      <span className="px-2 py-0.5 rounded text-micro font-mono font-bold bg-negative/15 text-negative border border-negative/40">
        Very unusual {score.toFixed(2)}
      </span>
    );
  if (score >= SCORE_ELEVATED)
    return (
      <span className="px-2 py-0.5 rounded text-micro font-mono font-bold bg-caution/10 text-caution border border-caution/30">
        Unusual {score.toFixed(2)}
      </span>
    );
  return (
    <span className="px-2 py-0.5 rounded text-micro font-mono text-ink-mute border border-line">
      Routine {score.toFixed(2)}
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

const NewsToneBadge = ({ reading }: { reading: NewsTone }) => (
  <span
    title="How positive or negative the wording is, as read by an AI trained on financial news. It does not say how much the story matters."
    className={`px-2 py-0.5 rounded text-micro font-mono border border-line ${
      reading.label === 'Positive tone'
        ? 'text-positive'
        : reading.label === 'Negative tone'
          ? 'text-negative'
          : 'text-ink-mute'
    }`}
  >
    {reading.label}
  </span>
);

const EventRow = React.memo(
  ({ e, onClick }: { e: NormalizedEvent; onClick: (e: NormalizedEvent) => void }) => {
    const domainMeta = domainMetaFor(e);
    const sourceName = getCleanSource(e);
    const title = formatEnglishHeadline(e);
    const entityName = entityLabel(e);
    const newsTone = companyNewsTone(e);

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
        className="p-3 rounded-lg bg-raised/60 border border-line hover:border-accent/50 cursor-pointer transition-all hover:bg-raised/90 group glass-panel-hover"
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
                    ? 'bg-positive/10 text-positive border-positive/30'
                    : 'bg-negative/10 text-negative border-negative/30'
                }`}
              >
                {fd.option_type}
              </span>
            )}
            {fd.eps_surprise_pct !== undefined && fd.eps_surprise_pct !== null && (
              <span
                className={`px-1.5 py-0.5 rounded border font-extrabold text-micro ${
                  fd.eps_surprise_pct >= 0
                    ? 'bg-positive/10 text-positive border-positive/30'
                    : 'bg-negative/10 text-negative border-negative/30'
                }`}
              >
                EPS {formatPercent(fd.eps_surprise_pct, { decimals: 1, signed: true })}
              </span>
            )}
            {cd.funding_rate !== undefined && (
              <span className="px-1.5 py-0.5 rounded border border-line-accent bg-accent-dim text-accent font-extrabold text-micro">
                {formatPercent(cd.funding_rate, { from: 'ratio', decimals: 4 })} RATE
              </span>
            )}
            {sourceName && (
              <span className="text-ink-dim font-medium">
                via <span className="text-accent font-bold">{sourceName}</span>
              </span>
            )}
          </div>
          {newsTone ? <NewsToneBadge reading={newsTone} /> : getScoreBadge(e.anomaly_score)}
        </div>

        <p className="text-xs text-ink font-sans font-semibold line-clamp-2 group-hover:text-ink transition-colors leading-snug">
          {title}
        </p>

        {e.summary && (
          <p className="text-micro text-ink-dim font-sans mt-1 line-clamp-2 leading-tight">
            {e.summary}
          </p>
        )}

        <div className="mt-2 flex items-center justify-between text-micro text-ink-dim pt-1 border-t border-line/60">
          <span>
            {entityName && (
              <>
                About: <span className="text-caution font-bold">{entityName}</span>
              </>
            )}
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
    <div className="p-3 rounded-lg bg-raised/80 border border-caution/30 hover:border-caution/30 transition-all space-y-1.5">
      <div className="flex items-center justify-between">
        <span className="font-semibold text-caution text-xs flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-caution animate-ping" />
          Rule: {c.rule_name}
        </span>
        <span className="px-2 py-0.5 rounded text-micro font-bold bg-caution/10 text-caution border border-caution/30">
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
            className="text-micro text-accent hover:text-accent font-bold flex items-center gap-1 cursor-pointer"
          >
            <IconChevronDown
              className={`transition-transform ${showEvidence ? '' : '-rotate-90'}`}
            />
            <span>{showEvidence ? 'Hide evidence trail' : 'Show evidence trail'}</span>
            <span className="text-ink-dim font-normal">({trail.length} signals)</span>
          </button>
          {showEvidence && (
            <div className="mt-1.5 p-2 bg-page/80 rounded border border-line space-y-1 text-micro">
              <span className="text-accent font-bold block mb-1">
                Evidence from the AI analysts:
              </span>
              {trail.map((item, idx) => (
                <div
                  key={idx}
                  className="flex items-center justify-between border-b border-line/60 pb-0.5"
                >
                  <span className="text-ink font-bold">{item.agent_name}</span>
                  <span className="text-ink-dim">
                    Dir:{' '}
                    <span className="text-caution font-bold">{item.direction || 'neutral'}</span>{' '}
                    | Score:{' '}
                    <span className="text-positive font-bold">
                      {(item.conviction ?? item.score ?? 0).toFixed(2)}
                    </span>{' '}
                    | Weight:{' '}
                    <span className="text-accent">{(item.weight ?? 1.0).toFixed(2)}</span>
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

/** `windowHours` narrows the event list to recent reports (the Intel console's slider). */
export default function IntelligenceFeed({ windowHours }: { windowHours?: number } = {}) {
  const [activeTab, setActiveTab] = useState<'events' | 'scenarios' | 'correlations'>('events');
  const [selectedDomain, setSelectedDomain] = useState<string>('all');
  const [scenarioStatus, setScenarioStatus] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedEvent, setSelectedEvent] = useState<NormalizedEvent | null>(null);
  const [fullEventDetail, setFullEventDetail] = useState<unknown>(null);
  const [isLoadingDetail, setIsLoadingDetail] = useState<boolean>(false);

  // Real-time WebSocket Live Feed connection
  const wsLiveEvents = useLiveEvents(selectedDomain);

  // Every 15 s, not 5: the shared live stream pushes new events as they
  // happen, so this is a backfill. At 5 s the Intel page polled 116 times a
  // minute (30 October).
  // Fetch AI Scenarios with dynamic status filter
  const scenarioUrl =
    scenarioStatus === 'all'
      ? '/scenarios?limit=20'
      : `/scenarios?limit=20&status=${encodeURIComponent(scenarioStatus)}`;

  const { data: scenarios, error: scenariosError } = useSWR<Scenario[]>(scenarioUrl, fetcher, {
    refreshInterval: POLL.standard,
  });

  // Fetch Raw Correlation Clusters
  const { data: correlations, error: correlationsError } = useSWR<CorrelationCluster[]>(
    '/correlations?limit=30&min_tier=1',
    fetcher,
    { refreshInterval: POLL.standard },
  );

  // Dynamic Event domain fetches
  const { data: tradfiEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'tradfi' ? '/events/tradfi?limit=30' : null,
    fetcher,
    { refreshInterval: POLL.standard },
  );
  const { data: cryptoEvents } = useSWR<NormalizedEvent[]>(
    selectedDomain === 'all' || selectedDomain === 'crypto' ? '/events/crypto?limit=30' : null,
    fetcher,
    { refreshInterval: POLL.standard },
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

    const deduped = withinHours(Array.from(new Map(rawEvents.map((e) => [e.event_id, e])).values()), windowHours);

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
    windowHours,
  ]);

  const mainTabs: { id: typeof activeTab; label: string; count: number }[] = [
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
    { id: 'tradfi', label: 'Markets' },
    { id: 'crypto', label: 'Crypto' },
    { id: 'prediction', label: 'Prediction' },
    { id: 'maritime', label: 'Ships' },
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
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 p-4 bg-raised border border-line rounded-xl shadow-panel">
        <div>
          {/* The page already carries the title. This was a second <h1> with a
              louder version of the same words directly beneath the first -- two
              headings, one page, and an accessibility tree with two h1s. */}
          <p className="text-xs text-ink-dim flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-accent animate-pulse shadow-panel" />
            Events, scenarios and groups of related events, as they arrive.
          </p>
        </div>

        {/* Search & Filter Inputs */}
        <div className="flex items-center gap-2">
          <Input
            label="Search events, tickers and entities"
            hideLabel
            size="sm"
            containerClassName="w-48"
            className="font-mono"
            type="text"
            placeholder="Search events, tickers, entities..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>
      </div>

      {/* Main Mode Tabs */}
      <div className="flex items-center justify-between gap-2 border-b border-line pb-2">
        <div className="flex items-center gap-2">
          {mainTabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              aria-pressed={activeTab === tab.id}
              className={`px-3 py-1.5 rounded-lg font-mono text-xs font-bold transition-all cursor-pointer flex items-center gap-2 ${
                activeTab === tab.id
                  ? 'bg-accent-dim text-accent border border-line-accent shadow-panel'
                  : 'bg-raised/60 text-ink-dim border border-line hover:text-ink'
              }`}
            >
              {tab.label}
              <span className="px-1.5 py-0.2 rounded bg-page text-micro font-mono text-accent font-bold border border-line">
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
                aria-pressed={selectedDomain === d.id}
                className={`px-2.5 py-1 rounded transition-colors cursor-pointer font-bold ${
                  selectedDomain === d.id
                    ? 'bg-accent-dim text-accent border border-line-accent'
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
                aria-pressed={scenarioStatus === status}
                className={`px-2 py-0.5 rounded uppercase font-bold cursor-pointer ${
                  scenarioStatus === status
                    ? 'bg-accent-dim text-accent border border-line-accent'
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
              <span className="h-2 w-2 rounded-full bg-accent inline-block animate-ping mr-2" />
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
                className="p-4 rounded-xl bg-inset border border-line hover:border-line-accent transition-all cursor-pointer space-y-2 group glass-panel-hover font-mono"
              >
                <div className="flex items-center justify-between text-micro">
                  <span className="px-2 py-0.5 rounded bg-accent-dim text-accent font-bold uppercase border border-line-accent">
                    {s.status || 'HYPOTHESIS'}
                  </span>
                  <span className="text-positive font-bold">
                    CONFIDENCE: {s.confidence_overall}%
                  </span>
                </div>
                <h3 className="text-xs font-bold text-ink group-hover:text-accent transition-colors font-sans line-clamp-2 leading-snug">
                  {s.headline}
                </h3>
                <p className="text-micro text-ink-dim font-sans line-clamp-2 leading-tight">
                  {s.narrative_summary}
                </p>
                <div className="pt-2 flex items-center justify-between text-micro text-ink-dim border-t border-line">
                  <span>
                    Entity:{' '}
                    <span className="text-caution font-bold">
                      {s.primary_entity_name || 'Several subjects'}
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
            <div className="flex items-center justify-between border-b border-line pb-3">
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
                className="text-ink-dim hover:text-ink text-sm font-bold px-2 py-0.5 rounded bg-overlay cursor-pointer"
              >
                CLOSE
              </button>
            </div>

            <div className="space-y-3 text-xs">
              <div>
                <span className="text-ink-dim block mb-0.5">Event headline:</span>
                <p className="text-ink font-bold font-sans text-sm">
                  {formatEnglishHeadline(selectedEvent)}
                </p>
              </div>

              {selectedEvent.summary && (
                <div>
                  <span className="text-ink-dim block mb-0.5">Summary</span>
                  <p className="text-ink font-sans text-xs bg-page p-3 rounded-lg border border-line leading-relaxed">
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
                  <span className="text-ink-dim">Event ID:</span>{' '}
                  <span className="text-accent font-bold block truncate">
                    {selectedEvent.event_id}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">Source:</span>{' '}
                  <span className="text-accent font-bold block">
                    {getCleanSource(selectedEvent) || 'unattributed'}
                  </span>
                </div>
                {companyNewsTone(selectedEvent) ? (
                  <div>
                    <span className="text-ink-dim">Tone of the wording:</span>{' '}
                    <span className="text-ink font-bold block">
                      {companyNewsTone(selectedEvent)?.label}
                    </span>
                  </div>
                ) : (
                  <div>
                    <span className="text-ink-dim">Anomaly score:</span>{' '}
                    <span className="text-caution font-bold block">
                      {fixed(selectedEvent.anomaly_score, 2)}
                    </span>
                  </div>
                )}
                <div>
                  <span className="text-ink-dim">Time:</span>{' '}
                  <span className="text-ink block">
                    {new Date(selectedEvent.occurred_at).toUTCString()}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">About:</span>{' '}
                  <span className="text-positive font-bold block">
                    {entityLabel(selectedEvent) || 'N/A'}
                  </span>
                </div>
                <div>
                  <span className="text-ink-dim">Region:</span>{' '}
                  <span className="text-accent font-bold block">
                    {selectedEvent.region || 'Global'}
                  </span>
                </div>
              </div>

              {/* Full JSON Payload from TimescaleDB */}
              <div>
                <span className="text-accent font-bold block mb-1">
                  DEEP DATABASE JSON PAYLOAD ({isLoadingDetail ? 'FETCHING...' : 'LIVE'}):
                </span>
                {isLoadingDetail ? (
                  <div className="p-4 bg-page rounded border border-line text-ink-dim text-center animate-pulse">
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
                className="w-full py-2 bg-raised text-accent border border-line rounded-lg text-xs font-bold hover:bg-overlay transition-colors cursor-pointer"
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
            className="bg-inset border border-line-accent rounded-xl max-w-2xl w-full p-5 space-y-4 shadow-panel max-h-[85vh] overflow-y-auto"
            {...scenarioDialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-line pb-3">
              <div className="flex items-center gap-2">
                <span className="text-xl">
                  <IconModel className="inline-block shrink-0" />
                </span>
                <span className="text-xs font-semibold text-accent">
                  Scenario review
                </span>
              </div>
              <button
                onClick={() => setSelectedScenario(null)}
                className="text-ink-dim hover:text-ink text-sm font-bold px-2 py-0.5 rounded bg-overlay cursor-pointer"
              >
                CLOSE
              </button>
            </div>

            <div className="space-y-3 text-xs">
              <div className="flex items-center justify-between bg-accent-dim p-3 rounded-lg border border-line">
                <div>
                  <span className="text-ink-dim block text-micro">Scenario status:</span>
                  <span className="text-accent font-bold text-sm">
                    {selectedScenario.status || 'HYPOTHESIS'}
                  </span>
                </div>
                <div className="text-right">
                  <span className="text-ink-dim block text-micro">Confidence score:</span>
                  <span className="text-positive font-extrabold text-sm">
                    {selectedScenario.confidence_overall}%
                  </span>
                </div>
              </div>

              <div>
                <span className="text-accent font-bold block mb-1">Headline:</span>
                <h3 className="text-sm font-bold text-ink font-sans">
                  {selectedScenario.headline}
                </h3>
              </div>

              {selectedScenario.primary_entity_name && (
                <div>
                  <span className="text-ink-dim block mb-0.5">About:</span>
                  <span className="text-caution font-bold">
                    {selectedScenario.primary_entity_name}
                  </span>
                </div>
              )}

              <div>
                <span className="text-ink-dim block mb-1">Strategic significance:</span>
                <p className="text-ink bg-page p-3 rounded border border-line leading-relaxed font-sans">
                  {selectedScenario.significance}
                </p>
              </div>

              {selectedScenario.confidence_rationale && (
                <div>
                  <span className="text-ink-dim block mb-1">Confidence rationale:</span>
                  <p className="text-ink-dim bg-page p-3 rounded border border-line text-micro leading-relaxed">
                    {selectedScenario.confidence_rationale}
                  </p>
                </div>
              )}

              {selectedScenarioTrail.length > 0 && (
                <div>
                  <span className="text-accent font-bold block mb-1">
                    Evidence from the AI analysts:
                  </span>
                  <div className="p-2.5 bg-page rounded border border-line space-y-1 text-micro">
                    {selectedScenarioTrail.map((item, idx) => (
                      <div
                        key={idx}
                        className="flex items-center justify-between border-b border-line pb-1"
                      >
                        <span className="text-ink font-bold">{item.agent_name}</span>
                        <span className="text-ink-dim">
                          Dir:{' '}
                          <span className="text-caution font-bold">
                            {item.direction || 'neutral'}
                          </span>{' '}
                          | Conviction:{' '}
                          <span className="text-positive font-bold">
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
                className="w-full py-2 bg-accent-dim text-accent border border-line-accent rounded-lg text-xs font-bold hover:bg-accent-dim transition-colors cursor-pointer"
              >
                Dismiss review
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
