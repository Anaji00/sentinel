'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { Tabs } from './ui/Tabs';

interface RadarAnomaly {
  event_id: string;
  ticker: string;
  entity_name: string;
  anomaly_score: number;
  z_score: number;
  occurred_at: string;
  region: string;
  details?: Record<string, any>;
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

export default function QuantRadarPanel() {
  const [subTab, setSubTab] = useState<'radar' | 'agents'>('radar');

  const { data: radarData } = useSWR<RadarResponse>(
    '/radar/anomalies',
    fetcher,
    { refreshInterval: 4000 }
  );

  const { data: sweepData } = useSWR<any>(
    '/radar/sweeps',
    fetcher,
    { refreshInterval: 6000 }
  );

  const { data: agentData } = useSWR<AgentResponse>(
    '/agents/processes',
    fetcher,
    { refreshInterval: 4000 }
  );

  const anomalies = radarData?.anomalies || [];
  const agents = agentData?.agents || [];
  const decisions = agentData?.recent_decisions || [];

  return (
    <Card
      title="COLLECTOR.RADAR & AGENTIC PROCESSES"
      badge={
        <Badge variant="live" pulse>
          SWEEPING 4,500+ TICKERS
        </Badge>
      }
      headerAction={
        <Tabs
          tabs={[
            { id: 'radar', label: 'RADAR SWEEPS', count: anomalies.length },
            { id: 'agents', label: 'AGENTIC OUTPUT', count: agents.length }
          ]}
          activeTab={subTab}
          onChange={(id) => setSubTab(id as 'radar' | 'agents')}
        />
      }
      noPadding
    >
      <div className="p-3 space-y-3 flex-1 overflow-y-auto font-mono text-xs">
        {subTab === 'radar' ? (
          <>
            {/* Radar Baseline Status HUD */}
            <div className="p-2.5 rounded-lg bg-slate-950 border border-cyan-500/20 space-y-1">
              <div className="flex items-center justify-between text-[#00f2fe] font-bold">
                <span>SWEEP ENGINE</span>
                <span className="text-emerald-400">Z-SCORE &gt; 3.0 THRESHOLD</span>
              </div>
              <div className="grid grid-cols-2 gap-2 text-[10px] text-slate-300 pt-1">
                <div>SCANNED: <span className="text-white font-bold">{sweepData?.total_universe_scanned || 4500} US Equities</span></div>
                <div>BASELINES: <span className="text-emerald-400 font-bold">{sweepData?.tracked_baselines || 1840} EWMA Keys</span></div>
              </div>
            </div>

            {/* Radar Volume Anomalies List */}
            <div className="space-y-2">
              <span className="text-[10px] text-slate-400 uppercase font-bold tracking-wider">
                QUANTITATIVE VOLUME ANOMALIES (Z &gt; 3.0)
              </span>
              {anomalies.map((a, idx) => (
                <div
                  key={idx}
                  className="p-2.5 rounded-lg bg-slate-900/80 border border-cyan-500/20 hover:border-[#00f2fe]/50 transition-all space-y-1"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-bold text-white text-xs">{a.ticker} ({a.entity_name})</span>
                    <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-rose-500/20 text-rose-400 border border-rose-500/40 glow-crimson">
                      +{a.z_score.toFixed(2)}σ ANOMALY
                    </span>
                  </div>
                  <div className="flex items-center justify-between text-[10px] text-slate-400">
                    <span>Region: {a.region}</span>
                    <span>{new Date(a.occurred_at).toLocaleTimeString()}</span>
                  </div>
                </div>
              ))}
            </div>
          </>
        ) : (
          <>
            {/* Agentic Processes List */}
            <div className="space-y-2">
              <span className="text-[10px] text-slate-400 uppercase font-bold tracking-wider">
                ACTIVE LLM AGENTIC SWARM ({agents.length} AGENTS)
              </span>
              {agents.map((ag, idx) => (
                <div
                  key={idx}
                  className="p-2.5 rounded-lg bg-slate-900/80 border border-purple-500/30 hover:border-purple-400/60 transition-all space-y-1.5"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-bold text-purple-300 uppercase text-[11px] flex items-center gap-1.5">
                      <span className="h-1.5 w-1.5 rounded-full bg-purple-400 animate-pulse" />
                      {ag.name}
                    </span>
                    <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-purple-500/20 text-purple-300 border border-purple-500/40">
                      {ag.model}
                    </span>
                  </div>
                  <p className="text-[10px] text-slate-300 font-sans leading-snug">
                    <span className="text-slate-500 font-mono">ACTION:</span> {ag.last_action}
                  </p>
                  <div className="text-[9px] text-slate-400 flex items-center justify-between border-t border-slate-800 pt-1 font-mono">
                    <span>IN: {ag.input_topic}</span>
                    <span>OUT: {ag.output_topic}</span>
                  </div>
                </div>
              ))}
            </div>

            {/* Agent Decisions & Telemetry */}
            <div className="space-y-2 pt-1">
              <span className="text-[10px] text-slate-400 uppercase font-bold tracking-wider">
                AGENT REASONING TRACES
              </span>
              {decisions.map((dec, idx) => (
                <div key={idx} className="p-2.5 rounded-lg bg-slate-950 border border-slate-800 text-[10px] space-y-1">
                  <div className="flex items-center justify-between text-cyan-400 font-bold">
                    <span>[{dec.agent}] {dec.action}</span>
                    <span className="text-slate-500">{new Date(dec.timestamp).toLocaleTimeString()}</span>
                  </div>
                  <p className="text-slate-300 font-sans leading-relaxed">{dec.rationale}</p>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </Card>
  );
}
