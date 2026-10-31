'use client';

import React, { useMemo, useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';
import { ClockTime } from './ui/ClockTime';
import { formatNumber } from '../lib/format';
import { useDialog } from './ui/useDialog';
import { POLL } from './ui/DataProvider';

// Scores arrive null on some rows; a null here crashed the whole view.
function fixed(value: number | null | undefined, digits: number): string {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '—';
}


// The server sends `z_score`, computed as the inverse of the curve the
// enricher used to build `anomaly_score`: `-5 * ln(1 - score)`.
//
// The fallback here was `anomaly_score * 4.5` -- a fourth copy of a constant
// that was wrong in two ways. It is linear where the forward map is
// logarithmic, and 4.5 is not the 5.0 the enricher compresses with, so the
// displayed sigma was bounded above by 4.5: five, ten and twenty sigma all
// showed between 2.8 and 4.5 and converged, in the one field whose job is to
// tell them apart.
const Z_SCORE_SCALE = 5.0;
const MAX_REPORTABLE_Z = 25.0;

function zOf(a: { z_score?: number | null; anomaly_score?: number | null }): number {
  if (typeof a.z_score === 'number' && Number.isFinite(a.z_score)) return a.z_score;
  const score = typeof a.anomaly_score === 'number' ? a.anomaly_score : 0;
  if (!(score > 0)) return 0;
  if (score >= 1) return MAX_REPORTABLE_Z;
  return Math.min(-Z_SCORE_SCALE * Math.log(1 - score), MAX_REPORTABLE_Z);
}

interface RadarAnomaly {
  event_id: string;
  ticker: string;
  entity_name: string;
  anomaly_score: number;
  z_score: number;
  occurred_at: string;
  region: string;
  details?: Record<string, unknown>;
}

/** `/radar/sweeps`: how much of the universe the sweep engine is covering. */
interface SweepStatus {
  total_universe_scanned?: number;
  tracked_baselines?: number;
}

interface RadarResponse {
  service: string;
  anomalies_count: number;
  anomalies: RadarAnomaly[];
  watchlist_count: number;
  watchlist: Array<{ ticker: string; added_timestamp: number }>;
}

interface AgentProcess {
  name: string;
  group_id: string;
  model: string;
  status: string;
  input_topic: string;
  output_topic: string;
  last_action: string;
}

interface AgentDecision {
  agent: string;
  timestamp: string;
  action: string;
  rationale: string;
  tickers_evicted?: string[];
  cluster_id?: string;
}

interface AgentResponse {
  active_agents_count: number;
  agents: AgentProcess[];
  recent_decisions_count: number;
  recent_decisions: AgentDecision[];
}

/** The z-score thresholds as words: how far above a stock's own normal. */
const LEVEL_WORD: Record<number, string> = { 3.0: 'Unusual', 4.5: 'Very unusual', 5.0: 'Extreme' };

export default function QuantRadarPanel() {
  const [subTab, setSubTab] = useState<'radar' | 'agents'>('radar');
  const [selectedAnomaly, setSelectedAnomaly] = useState<RadarAnomaly | null>(null);

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(
    Boolean(selectedAnomaly),
    () => setSelectedAnomaly(null),
    'Anomaly detail',
  );
  const [minZScore, setMinZScore] = useState<number>(3.0);
  const [searchQuery, setSearchQuery] = useState<string>('');

  const { data: radarData } = useSWR<RadarResponse>('/radar/anomalies', fetcher, {
    refreshInterval: POLL.live,
  });

  const { data: sweepData } = useSWR<SweepStatus>('/radar/sweeps', fetcher, {
    refreshInterval: POLL.live,
  });

  const { data: agentData } = useSWR<AgentResponse>('/agents/processes', fetcher, {
    refreshInterval: POLL.live,
  });

  const rawAnomalies = useMemo(() => radarData?.anomalies || [], [radarData]);
  const agents = agentData?.agents || [];
  const decisions = agentData?.recent_decisions || [];

  const filteredAnomalies = useMemo(() => {
    return rawAnomalies.filter((a) => {
      const zPass = zOf(a) >= minZScore;
      const q = searchQuery.toLowerCase();
      const searchPass =
        !q || a.ticker.toLowerCase().includes(q) || a.entity_name.toLowerCase().includes(q);
      return zPass && searchPass;
    });
  }, [rawAnomalies, minZScore, searchQuery]);

  return (
    <Card
      title="Radar"
      badge={
        // The swept count, or nothing. This read "SWEEPING 4,500+ TICKERS" as a
        // literal, beside two figures that fell back to '4,500+' and '1,800+'
        // when the real ones were missing -- so the panel asserted a universe
        // size whether or not a sweep had happened.
        <Badge variant="live" pulse>
          {sweepData?.total_universe_scanned
            ? `Sweeping ${formatNumber(sweepData.total_universe_scanned, { decimals: 0 })} tickers`
            : 'Awaiting first sweep'}
        </Badge>
      }
      headerAction={
        <Tabs
          tabs={[
            { id: 'radar', label: 'Sweeps', count: filteredAnomalies.length },
            { id: 'agents', label: 'Agent output', count: agents.length },
          ]}
          activeTab={subTab}
          onChange={(id) => setSubTab(id as 'radar' | 'agents')}
        />
      }
      noPadding
    >
      <div className="p-3.5 space-y-3 flex-1 overflow-y-auto text-xs">
        {subTab === 'radar' ? (
          <>
            {/* Radar Baseline Status HUD */}
            <div className="p-2.5 rounded-lg bg-page border border-line space-y-1.5">
              <div className="flex items-center justify-between text-accent font-bold">
                <span>Unusual trading volume</span>
                <div className="flex items-center gap-1">
                  <span className="text-micro text-ink-dim">Show</span>
                  {[3.0, 4.5, 5.0].map((z) => (
                    <button
                      key={z}
                      onClick={() => setMinZScore(z)}
                      aria-pressed={minZScore === z}
                      className={`px-1.5 py-0.5 rounded text-micro font-bold transition-all cursor-pointer ${
                        minZScore === z
                          ? 'bg-accent text-inset border border-ink'
                          : 'bg-raised text-ink-dim border border-line-strong'
                      }`}
                    >
                      {LEVEL_WORD[z] ?? `${z.toFixed(1)}+`}
                    </button>
                  ))}
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2 text-micro text-ink-dim pt-1 border-t border-line/80">
                {/* Em dash where there is no measurement. Not a rounded
                    guess in the same weight as a measured figure. */}
                <div>
                  Scanned:{' '}
                  <span className="text-ink font-bold">
                    {sweepData?.total_universe_scanned
                      ? `${formatNumber(sweepData.total_universe_scanned, { decimals: 0 })} equities`
                      : '—'}
                  </span>
                </div>
                <div>
                  Known normals:{' '}
                  <span className="text-positive font-bold">
                    {sweepData?.tracked_baselines
                      ? `${formatNumber(sweepData.tracked_baselines, { decimals: 0 })} stocks with a known normal`
                      : '—'}
                  </span>
                </div>
              </div>
            </div>

            {/* Search Input */}
            <div className="relative">
              <input
                type="text"
                placeholder="Find a stock by ticker or company name…"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full bg-inset border border-line rounded-lg px-3 py-1.5 text-xs text-ink placeholder:text-ink-mute focus:outline-none focus:border-accent/60 font-mono transition-colors"
              />
            </div>

            {/* Radar Volume Anomalies List */}
            <div className="space-y-2">
              <span className="text-micro text-ink-dim uppercase font-bold tracking-wider">
                TRADING FAR ABOVE ITS NORMAL ({(LEVEL_WORD[minZScore] ?? '').toUpperCase()} AND UP)
              </span>
              {filteredAnomalies.length > 0 ? (
                filteredAnomalies.map((a, idx) => {
                  const zVal = zOf(a);
                  return (
                    <div
                      key={a.event_id || idx}
                      onClick={() => setSelectedAnomaly(a)}
                      className="p-2.5 rounded-lg bg-raised/80 border border-line hover:border-accent/50 cursor-pointer transition-all space-y-1 hover:bg-raised/95"
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-bold text-ink text-xs">
                          {a.ticker} ({a.entity_name})
                        </span>
                        <span className="px-2 py-0.5 rounded text-micro font-bold bg-negative/10 text-negative border border-negative/30">
                          {zVal >= 5 ? 'EXTREME' : zVal >= 4.5 ? 'VERY UNUSUAL' : 'UNUSUAL'} · {zVal.toFixed(1)}
                        </span>
                      </div>
                      <div className="flex items-center justify-between text-micro text-ink-dim pt-0.5">
                        <span>
                          Region: <span className="text-ink-dim">{a.region}</span>
                        </span>
                        <span>
                          <ClockTime value={a.occurred_at} />
                        </span>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="p-6 text-center border border-dashed border-line rounded-lg text-ink-dim text-xs">
                  Nothing is trading this far above its normal right now.
                </div>
              )}
            </div>
          </>
        ) : (
          <>
            {/* Agentic Processes List */}
            <div className="space-y-2">
              <span className="text-micro text-ink-dim uppercase font-bold tracking-wider">
                Agents ({agents.length})
              </span>
              {agents.map((ag, idx) => (
                <div
                  key={idx}
                  className="p-2.5 rounded-lg bg-raised/80 border border-line hover:border-line-accent transition-all space-y-1.5"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-bold text-accent uppercase text-micro flex items-center gap-1.5">
                      <span className="h-1.5 w-1.5 rounded-full bg-info animate-pulse" />
                      {ag.name}
                    </span>
                    <span className="px-1.5 py-0.5 rounded text-micro font-bold bg-accent-dim text-accent border border-line-accent">
                      {ag.model}
                    </span>
                  </div>
                  <p className="text-micro text-ink-dim font-sans leading-snug">
                    <span className="text-ink-mute font-mono">ACTION:</span> {ag.last_action}
                  </p>
                  <div className="text-micro text-ink-dim flex items-center justify-between border-t border-line pt-1">
                    <span>IN: {ag.input_topic}</span>
                    <span>OUT: {ag.output_topic}</span>
                  </div>
                </div>
              ))}
            </div>

            {/* Agent Decisions & Telemetry */}
            <div className="space-y-2 pt-1">
              <span className="text-micro text-ink-dim font-semibold">Agent reasoning</span>
              {decisions.map((dec, idx) => (
                <div
                  key={idx}
                  className="p-2.5 rounded-lg bg-page border border-line text-micro space-y-1"
                >
                  <div className="flex items-center justify-between text-accent font-bold">
                    <span>
                      [{dec.agent}] {dec.action}
                    </span>
                    <span className="text-ink-mute">
                      <ClockTime value={dec.timestamp} />
                    </span>
                  </div>
                  <p className="text-ink-dim font-sans leading-relaxed">{dec.rationale}</p>
                </div>
              ))}
            </div>
          </>
        )}
      </div>

      {/* Radar Anomaly Inspector Modal */}
      {selectedAnomaly && (
        <div
          className="fixed inset-0 z-50 bg-black/80 flex items-center justify-center p-4"
          {...dialog.overlayProps}
        >
          <div
            className="bg-raised border border-accent/50 rounded-xl max-w-md w-full p-5 space-y-3 text-xs"
            {...dialog.panelProps}
          >
            <div className="flex items-center justify-between border-b border-line pb-2.5">
              <span className="text-xs font-bold text-ink">
                {selectedAnomaly.ticker} VOLUME SPIKE INSPECTOR
              </span>
              <button
                onClick={() => setSelectedAnomaly(null)}
                className="text-ink-dim hover:text-ink font-bold text-xs bg-overlay px-2 py-0.5 rounded cursor-pointer"
              >
                CLOSE
              </button>
            </div>
            <div className="space-y-2">
              <div>
                <span className="text-ink-dim">ENTITY:</span>{' '}
                <span className="text-ink font-bold">{selectedAnomaly.entity_name}</span>
              </div>
              <div>
                <span className="text-ink-dim">Z-score spike:</span>{' '}
                <span className="text-negative font-bold">
                  +{zOf(selectedAnomaly).toFixed(2)}&sigma;
                </span>
              </div>
              <div>
                <span className="text-ink-dim">Anomaly score:</span>{' '}
                <span className="text-caution font-bold">
                  {fixed(selectedAnomaly.anomaly_score, 3)}
                </span>
              </div>
              <div>
                <span className="text-ink-dim">REGION:</span>{' '}
                <span className="text-positive">{selectedAnomaly.region}</span>
              </div>
              <div>
                <span className="text-ink-dim">Detected at:</span>{' '}
                <span className="text-ink-dim">
                  {new Date(selectedAnomaly.occurred_at).toUTCString()}
                </span>
              </div>
            </div>

            <button
              onClick={() => setSelectedAnomaly(null)}
              className="w-full py-2 bg-raised text-accent border border-line rounded-lg text-xs font-bold hover:bg-overlay transition-colors cursor-pointer mt-2"
            >
              DISMISS
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
