'use client';

/**
 * AgentSwarmTelemetry
 *
 * Liveness and conclusions for the LLM swarm, from the two endpoints that
 * actually hold them: /agents/processes (who is running, on which model) and
 * /agents/swarm (what they have concluded).
 *
 * This component previously called /api/v1/health/agents -- a route that does
 * not exist -- and fell back to a hardcoded array of six agents with invented
 * throughput figures, under a header reading "REAL-TIME AGENT HEALTH". It also
 * printed "6/6 AGENTS ACTIVE" and "HEALTH: 100% NOMINAL" as literals, with a
 * green pulse next to every row regardless of status. The deployment runs ten
 * agents on qwen2.5:3b and 1.5b; the fallback claimed six on qwen2.5:7b, which
 * is not deployed at all.
 *
 * Nothing here is defaulted. When a value is missing it is shown as missing,
 * because an operator cannot act on a panel that always shows activity.
 */

import React from 'react';
import useSWR from 'swr';
import { Activity, AlertTriangle, Bot, GitBranch, Scale } from 'lucide-react';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';

interface AgentProcess {
  name: string | null;
  tier: string;
  model: string | null;
  fallback_model: string | null;
  status: string;
  heartbeat_age_seconds: number | null;
  detail?: string;
  processed?: number;
  errors?: number;
}

interface ConsensusSignal {
  ticker: string;
  direction: string;
  contributing_agents: number;
  agreement_ratio: number;
  consensus_score: number;
}

interface SwarmIntelligence {
  consensus: {
    summary: string;
    generated_at: string | null;
    agents_reporting: string[];
    stale_agents: string[];
    contradictions: number;
    corroborated_signals: ConsensusSignal[];
    single_agent_signals: ConsensusSignal[];
  };
  bulletins: Array<{
    agent_name: string;
    bulletin_type: string;
    summary: string;
    ticker?: string | null;
    conviction?: number;
  }>;
  scorecards: Array<{
    agent_name: string;
    predictions_made?: number;
    predictions_correct?: number;
    consensus_weight?: number;
  }>;
  open_predictions: number;
  calibration: { paired_forecasts: number; resolved: number };
}

const STATUS_TONE: Record<string, string> = {
  HEALTHY: 'bg-emerald-400',
  DEGRADED: 'bg-amber-400',
  OFFLINE: 'bg-rose-500',
};

function Stat({ label, value, hint }: { label: string; value: React.ReactNode; hint?: string }) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] uppercase tracking-wider text-slate-500">{label}</div>
      <div className="text-lg font-semibold text-slate-100 tabular-nums leading-tight truncate">{value}</div>
      {hint && <div className="text-[10px] text-slate-500 truncate">{hint}</div>}
    </div>
  );
}

export default function AgentSwarmTelemetry() {
  const { data: processes, error: procError } = useSWR<{
    active_agents_count: number;
    agents: AgentProcess[];
  }>('/agents/processes', fetcher, { refreshInterval: 8000 });

  const { data: swarm } = useSWR<SwarmIntelligence>('/agents/swarm', fetcher, {
    refreshInterval: 10000,
  });

  const agents = (processes?.agents || []).filter((a) => a.name);
  const online = agents.filter((a) => a.status === 'HEALTHY').length;
  const cons = swarm?.consensus;

  return (
    <Card className="h-full flex flex-col overflow-hidden">
      <div className="flex flex-wrap items-start justify-between gap-3 px-4 py-3 border-b border-slate-800 bg-slate-950/50 shrink-0">
        <div className="flex items-center gap-2.5">
          <Bot className="h-4 w-4 text-cyan-300" />
          <div>
            <h2 className="text-xs font-semibold tracking-wide text-slate-100 uppercase">Agent Swarm</h2>
            <p className="text-[10px] text-slate-500">Liveness and current conclusions</p>
          </div>
        </div>
        <div className="text-[11px] text-slate-400 tabular-nums">
          {procError ? (
            <span className="text-rose-400">telemetry unavailable</span>
          ) : processes ? (
            <span>
              <strong className="text-slate-100">{online}</strong> of {agents.length} healthy
            </span>
          ) : (
            <span className="text-slate-500">loading…</span>
          )}
        </div>
      </div>

      {/* Swarm conclusions. Empty is shown as empty. */}
      <div className="px-4 py-3 border-b border-slate-800 bg-slate-950/30 shrink-0">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Stat label="Open predictions" value={swarm ? swarm.open_predictions : '—'} />
          <Stat
            label="Scorecards"
            value={swarm ? swarm.scorecards.length : '—'}
            hint={swarm && swarm.scorecards.length === 0 ? 'none resolved yet' : undefined}
          />
          <Stat
            label="Paired forecasts"
            value={swarm ? swarm.calibration.paired_forecasts : '—'}
            hint={swarm ? `${swarm.calibration.resolved} resolved` : undefined}
          />
          <Stat label="Contradictions" value={cons ? cons.contradictions : '—'} />
        </div>

        {cons && (
          <div className="mt-3 flex items-start gap-2 text-[11px] leading-relaxed">
            <GitBranch className="h-3.5 w-3.5 text-slate-500 mt-0.5 shrink-0" />
            <p className="text-slate-300">
              {cons.summary || 'No consensus report published yet.'}
            </p>
          </div>
        )}

        {cons && cons.stale_agents.length > 0 && (
          <div className="mt-2 flex items-center gap-2 text-[11px] text-amber-300">
            <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
            <span>Context drift: {cons.stale_agents.join(', ')}</span>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-y-auto">
        {agents.length === 0 && (
          <div className="p-6 text-center text-xs text-slate-500">
            {processes?.agents?.[0]?.detail || 'No agent roster reported.'}
          </div>
        )}

        <ul className="divide-y divide-slate-800/70">
          {agents.map((ag) => (
            <li key={`${ag.tier}:${ag.name}`} className="px-4 py-2.5 hover:bg-slate-900/40 transition-colors">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2.5 min-w-0">
                  <span
                    className={`h-2 w-2 rounded-full shrink-0 ${STATUS_TONE[ag.status] || 'bg-slate-600'}`}
                    title={ag.status}
                  />
                  <span className="text-xs text-slate-200 truncate">{ag.name?.replace(/_/g, ' ')}</span>
                </div>
                <div className="flex items-center gap-3 text-[10px] text-slate-500 shrink-0 tabular-nums">
                  {typeof ag.processed === 'number' && (
                    <span title="messages processed">{ag.processed.toLocaleString()}</span>
                  )}
                  {typeof ag.errors === 'number' && ag.errors > 0 && (
                    <span className="text-rose-400" title="errors">{ag.errors}</span>
                  )}
                  <span className="px-1.5 py-0.5 rounded bg-slate-800/80 text-slate-400">
                    {ag.model || 'model unreported'}
                  </span>
                  <span className="uppercase tracking-wide">{ag.tier}</span>
                </div>
              </div>
            </li>
          ))}
        </ul>

        {swarm && swarm.bulletins.length > 0 && (
          <div className="border-t border-slate-800">
            <div className="px-4 py-2 flex items-center gap-2 text-[10px] uppercase tracking-wider text-slate-500">
              <Activity className="h-3 w-3" /> Open bulletins
            </div>
            <ul className="divide-y divide-slate-800/70">
              {swarm.bulletins.slice(0, 8).map((b, i) => (
                <li key={i} className="px-4 py-2 text-[11px]">
                  <div className="flex items-center gap-2 text-slate-500">
                    <span className="text-cyan-300">{b.agent_name?.replace(/_/g, ' ')}</span>
                    <span>·</span>
                    <span>{b.bulletin_type}</span>
                    {b.ticker && <span className="text-slate-300">{b.ticker}</span>}
                  </div>
                  <p className="text-slate-300 mt-0.5 line-clamp-2">{b.summary}</p>
                </li>
              ))}
            </ul>
          </div>
        )}

        {swarm && swarm.scorecards.length > 0 && (
          <div className="border-t border-slate-800">
            <div className="px-4 py-2 flex items-center gap-2 text-[10px] uppercase tracking-wider text-slate-500">
              <Scale className="h-3 w-3" /> Track record
            </div>
            <ul className="divide-y divide-slate-800/70">
              {swarm.scorecards.map((s) => (
                <li key={s.agent_name} className="px-4 py-2 flex items-center justify-between text-[11px]">
                  <span className="text-slate-300">{s.agent_name?.replace(/_/g, ' ')}</span>
                  <span className="text-slate-500 tabular-nums">
                    {s.predictions_correct ?? 0}/{s.predictions_made ?? 0} · weight{' '}
                    {(s.consensus_weight ?? 0).toFixed(2)}
                  </span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </Card>
  );
}
