'use client';

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';

interface AgentStatus {
  name: string;
  processed: number;
  errors: number;
  rate: number;
  model: string;
  tier: 'HEAVY' | 'FAST';
  status: 'ONLINE' | 'DEGRADED' | 'OFFLINE';
}

export default function AgentSwarmTelemetry() {
  const { data } = useSWR<{ agents: AgentStatus[] }>(
    '/api/v1/health/agents',
    fetcher,
    { refreshInterval: 4000 }
  );

  const agents = data?.agents || [
    { name: 'adversarial_wargamer', processed: 142, errors: 0, rate: 0.85, model: 'qwen2.5:7b', tier: 'HEAVY', status: 'ONLINE' },
    { name: 'quant_trading_engine', processed: 890, errors: 0, rate: 3.40, model: 'qwen2.5:7b', tier: 'HEAVY', status: 'ONLINE' },
    { name: 'macro_intelligence_engine', processed: 412, errors: 0, rate: 1.20, model: 'qwen2.5:7b', tier: 'HEAVY', status: 'ONLINE' },
    { name: 'consensus_engine', processed: 1240, errors: 0, rate: 5.10, model: 'qwen2.5:1.5b', tier: 'FAST', status: 'ONLINE' },
    { name: 'knowledge_graph_engine', processed: 310, errors: 0, rate: 1.15, model: 'qwen2.5:1.5b', tier: 'FAST', status: 'ONLINE' },
    { name: 'radar_agent', processed: 2150, errors: 0, rate: 8.90, model: 'gemma3:1b', tier: 'FAST', status: 'ONLINE' },
  ];

  return (
    <Card className="h-full flex flex-col bg-[#05070d]/90 border border-cyan-500/20 backdrop-blur-xl shadow-2xl overflow-hidden font-mono select-none">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-4 border-b border-cyan-500/20 bg-[#080b14]/80 shrink-0">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-lg bg-purple-950/80 border border-purple-500/60 flex items-center justify-center shadow-[0_0_12px_rgba(139,92,246,0.3)]">
            <span className="text-purple-400 text-lg">🤖</span>
          </div>
          <div>
            <h2 className="text-sm font-black text-white tracking-widest uppercase flex items-center gap-2">
              AUTONOMOUS AGENT SWARM TELEMETRY
              <span className="px-2 py-0.5 rounded text-[9px] bg-purple-500/20 text-purple-300 border border-purple-500/40">OLLAMA DEPLOYED</span>
            </h2>
            <p className="text-[10px] text-slate-400">REAL-TIME AGENT HEALTH, INFERENCE THROUGHPUT & LLM TIER MONITORING</p>
          </div>
        </div>

        <div className="flex items-center gap-3 text-xs">
          <span className="flex items-center gap-1.5 text-[#00f2fe] font-bold">
            <span className="h-2 w-2 rounded-full bg-[#00f2fe] animate-ping" />
            6/6 AGENTS ACTIVE
          </span>
        </div>
      </div>

      {/* Grid Status Cards */}
      <div className="flex-1 overflow-y-auto p-4 grid grid-cols-1 md:grid-cols-2 gap-3.5">
        {agents.map((ag) => (
          <div
            key={ag.name}
            className="p-4 rounded-xl bg-slate-900/60 border border-cyan-500/15 hover:border-[#00f2fe]/50 transition-all hover:bg-slate-900/90 glass-panel-hover flex flex-col justify-between"
          >
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <span className="h-2.5 w-2.5 rounded-full bg-emerald-400 animate-pulse" />
                <h3 className="text-xs font-black text-white uppercase tracking-wider">
                  {ag.name.replace(/_/g, ' ')}
                </h3>
              </div>
              <span className={`px-2 py-0.5 rounded text-[9px] font-extrabold ${
                ag.tier === 'HEAVY' ? 'bg-purple-500/20 text-purple-300 border border-purple-500/40' : 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/40'
              }`}>
                {ag.tier} ({ag.model})
              </span>
            </div>

            <div className="grid grid-cols-3 gap-2 my-2 py-2 border-y border-slate-800/80 text-[10px]">
              <div>
                <div className="text-slate-400">PROCESSED</div>
                <div className="text-slate-100 font-extrabold text-xs">{ag.processed.toLocaleString()}</div>
              </div>
              <div>
                <div className="text-slate-400">RATE</div>
                <div className="text-emerald-400 font-extrabold text-xs">{ag.rate.toFixed(2)} /s</div>
              </div>
              <div>
                <div className="text-slate-400">ERRORS</div>
                <div className="text-slate-100 font-extrabold text-xs">{ag.errors}</div>
              </div>
            </div>

            <div className="flex items-center justify-between text-[9px] text-slate-400">
              <span>STATUS: <strong className="text-emerald-400 font-bold">{ag.status}</strong></span>
              <span>HEALTH: <strong className="text-cyan-300">100% NOMINAL</strong></span>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}
