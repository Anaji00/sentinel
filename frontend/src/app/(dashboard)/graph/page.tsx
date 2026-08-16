'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';
import { Network, Activity, Cpu, Database, Zap } from 'lucide-react';

const GraphExplorer = dynamic(() => import('@/components/GraphExplorer'), {
  loading: () => <PanelSkeleton title="Initializing SENTINEL Knowledge Graph & Correlation Matrix..." />,
  ssr: false,
});

export default function GraphPage() {
  return (
    <div className="flex h-full w-full flex-col p-4 gap-4 overflow-y-auto font-mono bg-[#06080d] text-slate-100">
      
      {/* Header & Status Bar */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-3 border-b border-cyan-500/20">
        <div>
          <div className="flex items-center gap-2">
            <div className="p-2 bg-cyan-500/10 border border-cyan-500/30 rounded-lg text-[#00f2fe]">
              <Network className="w-5 h-5" />
            </div>
            <div>
              <h1 className="text-lg sm:text-xl font-black text-white uppercase tracking-widest flex items-center gap-2">
                KNOWLEDGE GRAPH & EMPIRICAL CORRELATION MATRIX
              </h1>
              <p className="text-xs text-slate-400">
                TOPOLOGY-GUIDED DISCOVERY, GRANGER CAUSALITY, HAWKES CONTAGION & 30-DAY EDGE DECAY (§3–§9)
              </p>
            </div>
          </div>
        </div>

        {/* Live Status Badges */}
        <div className="flex items-center gap-2 flex-wrap">
          <div className="flex items-center gap-1.5 px-3 py-1 bg-emerald-500/15 border border-emerald-500/30 rounded-lg text-emerald-400 text-xs font-bold">
            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
            NEO4J LIVE
          </div>
          <div className="flex items-center gap-1.5 px-3 py-1 bg-cyan-500/15 border border-cyan-500/30 rounded-lg text-[#00f2fe] text-xs font-bold">
            <Database className="w-3.5 h-3.5" />
            TIMESCALE CAGG
          </div>
          <div className="flex items-center gap-1.5 px-3 py-1 bg-purple-500/15 border border-purple-500/30 rounded-lg text-purple-400 text-xs font-bold">
            <Zap className="w-3.5 h-3.5" />
            KAFKA SUPERVISED
          </div>
        </div>
      </div>

      {/* Epistemic Class Legend / KPI Strip */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
        <div className="p-3 bg-[#090d16] border border-cyan-500/30 rounded-xl flex items-center justify-between">
          <div>
            <span className="text-[10px] text-slate-400 uppercase font-bold">Statistical / Granger</span>
            <div className="text-sm font-extrabold text-[#00f2fe] mt-0.5 flex items-center gap-1.5">
              <span className="w-3 h-0.5 bg-[#00f2fe] border-t border-dashed" />
              Empirical (r, p, F, lag)
            </div>
          </div>
          <Activity className="w-4 h-4 text-[#00f2fe] opacity-80" />
        </div>

        <div className="p-3 bg-[#090d16] border border-purple-500/30 rounded-xl flex items-center justify-between">
          <div>
            <span className="text-[10px] text-slate-400 uppercase font-bold">Structural / Index</span>
            <div className="text-sm font-extrabold text-purple-400 mt-0.5 flex items-center gap-1.5">
              <span className="w-3 h-0.5 bg-purple-400" />
              MEMBER_OF / OPERATES_IN
            </div>
          </div>
          <Cpu className="w-4 h-4 text-purple-400 opacity-80" />
        </div>

        <div className="p-3 bg-[#090d16] border border-emerald-500/30 rounded-xl flex items-center justify-between">
          <div>
            <span className="text-[10px] text-slate-400 uppercase font-bold">Supply Chain & Narrative</span>
            <div className="text-sm font-extrabold text-emerald-400 mt-0.5 flex items-center gap-1.5">
              <span className="w-3 h-0.5 bg-emerald-400" />
              SUPPLIER_TO / COMPETES_WITH
            </div>
          </div>
          <Network className="w-4 h-4 text-emerald-400 opacity-80" />
        </div>

        <div className="p-3 bg-[#090d16] border border-amber-500/30 rounded-xl flex items-center justify-between">
          <div>
            <span className="text-[10px] text-slate-400 uppercase font-bold">Decay Convention</span>
            <div className="text-sm font-extrabold text-amber-400 mt-0.5">
              30-Day Half-Life (t½=30d)
            </div>
          </div>
          <Zap className="w-4 h-4 text-amber-400 opacity-80" />
        </div>
      </div>

      {/* Main Graph Explorer Interactive Surface */}
      <div className="flex-1 min-h-[650px] bg-[#090d16] border border-cyan-500/20 rounded-2xl overflow-hidden shadow-2xl p-2 sm:p-4">
        <GraphExplorer />
      </div>
    </div>
  );
}
