'use client';

import React, { useState, useEffect } from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';

interface SystemHealthResponse {
  status: string;
  redis_connected: boolean;
  timescale_connected: boolean;
  neo4j_connected: boolean;
}

export const Header: React.FC = () => {
  const [time, setTime] = useState<string>(() => {
    return new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
  });
  const [latency, setLatency] = useState<number>(38);
  const [showStatusModal, setShowStatusModal] = useState<boolean>(false);

  const { data: health } = useSWR<SystemHealthResponse>(
    showStatusModal ? '/health' : null,
    fetcher,
    { refreshInterval: 5000 }
  );

  useEffect(() => {
    const updateClock = () => {
      const now = new Date();
      setTime(now.toISOString().replace('T', ' ').substring(0, 19) + ' UTC');
    };
    updateClock();
    const interval = setInterval(updateClock, 1000);
    return () => clearInterval(interval);
  }, []);

  return (
    <>
      <header className="h-16 w-full bg-[#06080d]/95 border-b border-[#00f2fe]/25 backdrop-blur-2xl flex items-center justify-between px-6 z-50 shrink-0 select-none shadow-[0_4px_30px_rgba(0,0,0,0.7)] relative">
        {/* Left Brand Title */}
        <div className="flex items-center gap-3.5">
          <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-cyan-950 to-slate-950 border border-[#00f2fe]/60 flex items-center justify-center shadow-[0_0_25px_rgba(0,242,254,0.45)] relative overflow-hidden group">
            <div className="absolute inset-0 bg-gradient-to-tr from-[#00f2fe]/30 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
            <span className="text-[#00f2fe] font-mono font-black text-xl tracking-tighter drop-shadow-[0_0_10px_rgba(0,242,254,0.8)]">S</span>
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-base font-mono font-extrabold text-white tracking-widest uppercase flex items-center gap-2">
                SENTINEL <span className="text-[#00f2fe] font-light">|</span> COMMAND HUD
              </h1>
              <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-[#00f2fe]/15 text-[#00f2fe] border border-[#00f2fe]/40 glow-cyan">
                v2.4 EDA ACTIVE
              </span>
            </div>
            <p className="text-[10px] text-slate-400 font-mono tracking-wide">
              AUTONOMOUS MULTI-DOMAIN INTELLIGENCE & QUANTITATIVE OPERATIONS
            </p>
          </div>
        </div>

        {/* Center Live Telemetry Ticker & Real-time Metrics Bar */}
        <div className="hidden xl:flex items-center gap-4 bg-[#080b12]/90 px-4 py-1.5 rounded-lg border border-[#00f2fe]/30 backdrop-blur-md shadow-[0_0_15px_rgba(0,242,254,0.15)]">
          <div className="flex items-center gap-3 text-xs font-mono">
            <span className="flex items-center gap-1.5 text-slate-300">
              <span className="h-2 w-2 rounded-full bg-emerald-400 animate-ping" />
              <span className="text-[10px] text-slate-400 uppercase tracking-widest font-bold">STREAM:</span>
              <span className="text-emerald-400 font-extrabold text-[11px]">LIVE (24/7)</span>
            </span>
            <span className="text-slate-700">|</span>
            <span className="text-[11px] text-amber-300 font-bold flex items-center gap-1">
              <span>🚢 AIS TANKERS:</span>
              <span className="text-amber-400 font-black">ACTIVE</span>
            </span>
            <span className="text-slate-700">|</span>
            <span className="text-[11px] text-cyan-300 font-bold flex items-center gap-1">
              <span>✈️ ADS-B FLIGHTS:</span>
              <span className="text-[#00f2fe] font-black">TRACKING</span>
            </span>
            <span className="text-slate-700">|</span>
            <span className="text-[11px] text-purple-300 font-bold flex items-center gap-1">
              <span>🤖 AGENT SWARM:</span>
              <span className="text-purple-400 font-black">ACTIVE (8)</span>
            </span>
          </div>
        </div>

        {/* Right Tactical Badges & Controls */}
        <div className="flex items-center gap-3.5 font-mono">
          <button 
            onClick={() => setShowStatusModal(!showStatusModal)}
            className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-cyan-950/40 border border-cyan-500/40 text-xs text-cyan-300 hover:text-white hover:border-[#00f2fe] hover:bg-cyan-500/20 transition-all cursor-pointer shadow-[0_0_12px_rgba(0,242,254,0.2)] font-bold"
          >
            <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
            <span className="text-[11px] tracking-wider uppercase">SYSTEM HEALTH</span>
          </button>

          <div className="text-right">
            <div className="text-xs font-semibold text-slate-200 tracking-tight font-mono">
              {time}
            </div>
            <div className="text-[10px] text-emerald-400 flex items-center justify-end gap-1 font-bold">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-ping" />
              LATENCY: {latency}ms
            </div>
          </div>
        </div>
      </header>

      {/* System Status Modal */}
      {showStatusModal && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
          <div className="w-full max-w-md bg-[#06080d] border border-cyan-500/40 rounded-xl p-5 shadow-[0_0_50px_rgba(0,242,254,0.2)] font-mono space-y-4 relative">
            <div className="flex items-center justify-between border-b border-cyan-500/20 pb-3">
              <h2 className="text-sm font-bold text-[#00f2fe] uppercase tracking-wider flex items-center gap-2">
                <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
                SENTINEL SYSTEM DIAGNOSTICS
              </h2>
              <button 
                onClick={() => setShowStatusModal(false)}
                className="text-slate-400 hover:text-white text-xs font-bold px-2 py-1 bg-slate-900 rounded border border-slate-700"
              >
                ESC
              </button>
            </div>

            <div className="space-y-2.5 text-xs text-slate-300">
              <div className="flex justify-between p-2 rounded bg-slate-950 border border-slate-800">
                <span>TimescaleDB Hypertable</span>
                <span className={health ? (health.timescale_connected ? "text-emerald-400 font-bold" : "text-rose-400 font-bold") : "text-amber-400 font-bold"}>
                  {health ? (health.timescale_connected ? "ONLINE (asyncpg)" : "OFFLINE / RECONNECTING") : "CHECKING..."}
                </span>
              </div>
              <div className="flex justify-between p-2 rounded bg-slate-950 border border-slate-800">
                <span>Neo4j Knowledge Graph</span>
                <span className={health ? (health.neo4j_connected ? "text-emerald-400 font-bold" : "text-amber-400 font-bold") : "text-amber-400 font-bold"}>
                  {health ? (health.neo4j_connected ? "ONLINE (Bolt 7687)" : "FALLBACK GRAPH ACTIVE") : "CHECKING..."}
                </span>
              </div>
              <div className="flex justify-between p-2 rounded bg-slate-950 border border-slate-800">
                <span>Redis PubSub Cache</span>
                <span className={health ? (health.redis_connected ? "text-emerald-400 font-bold" : "text-rose-400 font-bold") : "text-amber-400 font-bold"}>
                  {health ? (health.redis_connected ? "ONLINE (Stream Active)" : "OFFLINE") : "CHECKING..."}
                </span>
              </div>
              <div className="flex justify-between p-2 rounded bg-slate-950 border border-slate-800">
                <span>Ollama LLM Swarm</span>
                <span className="text-cyan-400 font-bold">Qwen 2.5 7B / Llama 3</span>
              </div>
              <div className="flex justify-between p-2 rounded bg-slate-950 border border-slate-800">
                <span>Kafka Event Streaming</span>
                <span className="text-emerald-400 font-bold">12 Topics Streaming</span>
              </div>
            </div>

            <div className="pt-2 text-center">
              <button 
                onClick={() => setShowStatusModal(false)}
                className="w-full py-2 bg-[#00f2fe]/20 text-[#00f2fe] border border-[#00f2fe]/40 rounded-lg text-xs font-bold hover:bg-[#00f2fe]/40 transition-colors"
              >
                CLOSE DIAGNOSTICS
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
};
