'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Activity, Database, Server, Shield, Cpu, X } from 'lucide-react';
import { DataGrid } from './ui/DataGrid';

interface SystemHealthResponse {
  status: string;
  redis_connected: boolean;
  timescale_connected: boolean;
  neo4j_connected: boolean;
  active_configuration: {
    maritime_dark_thresholds?: number;
    tracked_financial_instruments?: number;
  };
}

interface MetricsSummary {
  [key: string]: any;
}

export default function SystemHealthHUD() {
  const [isOpen, setIsOpen] = useState(false);

  const { data: healthData } = useSWR<SystemHealthResponse>(
    '/health',
    fetcher,
    { refreshInterval: 5000 }
  );

  const { data: metricsData } = useSWR<MetricsSummary>(
    '/health/metrics/json',
    fetcher,
    { refreshInterval: 5000 }
  );

  const isHealthy = healthData?.status === 'online';

  return (
    <>
      {/* Trigger Pill in Header */}
      <button
        onClick={() => setIsOpen(true)}
        className={`flex items-center gap-2.5 px-3.5 py-2 rounded-xl text-xs font-mono border transition-all cursor-pointer shadow-md ${
          isHealthy
            ? 'bg-emerald-950/40 text-emerald-400 border-emerald-500/40 hover:bg-emerald-900/50 glow-emerald'
            : 'bg-amber-950/40 text-amber-400 border-amber-500/40 hover:bg-amber-900/50'
        }`}
      >
        <span className={`h-2 w-2 rounded-full ${isHealthy ? 'bg-emerald-400 animate-pulse' : 'bg-amber-400'}`} />
        <span className="font-bold uppercase tracking-wider">
          {healthData?.status ? `SYS: ${healthData.status.toUpperCase()}` : 'CONNECTING...'}
        </span>
      </button>

      {/* Infrastructure Telemetry Modal */}
      {isOpen && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4">
          <div className="bg-[#0b0e17] border border-[#00f2fe]/40 rounded-xl max-w-lg w-full p-5 space-y-4 shadow-[0_0_35px_rgba(0,242,254,0.25)] font-mono text-xs text-slate-200">
            {/* Header */}
            <div className="flex items-center justify-between border-b border-cyan-500/20 pb-3">
              <div className="flex items-center gap-2">
                <Activity className="w-4 h-4 text-[#00f2fe]" />
                <span className="font-bold text-[#00f2fe] uppercase tracking-wider">SENTINEL C4I INFRASTRUCTURE HUD</span>
              </div>
              <button
                onClick={() => setIsOpen(false)}
                className="text-slate-400 hover:text-white p-1 rounded bg-slate-800 cursor-pointer"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            {/* Service Connectivity Grid */}
            <div className="space-y-2">
              <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">CORE BACKEND CLUSTER CONNECTIVITY</span>
              <div className="grid grid-cols-3 gap-2">
                <div className={`p-3 rounded-lg border flex flex-col items-center gap-1 ${healthData?.redis_connected ? 'bg-slate-900 border-emerald-500/40 text-emerald-400' : 'bg-slate-900 border-rose-500/40 text-rose-400'}`}>
                  <Server className="w-4 h-4" />
                  <span className="font-bold text-[11px]">REDIS K/V</span>
                  <span className="text-[9px] uppercase">{healthData?.redis_connected ? 'CONNECTED' : 'DISCONNECTED'}</span>
                </div>

                <div className={`p-3 rounded-lg border flex flex-col items-center gap-1 ${healthData?.timescale_connected ? 'bg-slate-900 border-emerald-500/40 text-emerald-400' : 'bg-slate-900 border-rose-500/40 text-rose-400'}`}>
                  <Database className="w-4 h-4" />
                  <span className="font-bold text-[11px]">TIMESCALE DB</span>
                  <span className="text-[9px] uppercase">{healthData?.timescale_connected ? 'CONNECTED' : 'DISCONNECTED'}</span>
                </div>

                <div className={`p-3 rounded-lg border flex flex-col items-center gap-1 ${healthData?.neo4j_connected ? 'bg-slate-900 border-emerald-500/40 text-emerald-400' : 'bg-slate-900 border-rose-500/40 text-rose-400'}`}>
                  <Cpu className="w-4 h-4" />
                  <span className="font-bold text-[11px]">NEO4J GRAPH</span>
                  <span className="text-[9px] uppercase">{healthData?.neo4j_connected ? 'CONNECTED' : 'DISCONNECTED'}</span>
                </div>
              </div>
            </div>

            {/* Active Configuration */}
            <div className="p-3 bg-slate-950 border border-slate-800 rounded-lg space-y-1.5">
              <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider block">ACTIVE RUNTIME CONFIGURATION</span>
              <div className="flex justify-between items-center text-[11px]">
                <span className="text-slate-400">Maritime Dark Threshold:</span>
                <span className="text-cyan-400 font-bold">{healthData?.active_configuration?.maritime_dark_thresholds || 4} Hours</span>
              </div>
              <div className="flex justify-between items-center text-[11px]">
                <span className="text-slate-400">Tracked Geopolitical Instruments:</span>
                <span className="text-amber-400 font-bold">{healthData?.active_configuration?.tracked_financial_instruments || 14} Assets</span>
              </div>
            </div>

            {/* System Metrics Summary */}
            {metricsData && (
              <div className="p-3 bg-slate-950 border border-purple-500/30 rounded-lg space-y-1.5">
                <span className="text-[10px] text-purple-400 font-bold uppercase tracking-wider block flex items-center gap-1">
                  <Shield className="w-3.5 h-3.5" /> PROMETHEUS TELEMETRY SUMMARY
                </span>
                <div className="max-h-32 overflow-y-auto">
                  <DataGrid data={metricsData} emptyLabel="No telemetry reported" />
                </div>
              </div>
            )}

            {/* Close Button */}
            <button
              onClick={() => setIsOpen(false)}
              className="w-full py-2 bg-slate-900 text-[#00f2fe] border border-cyan-500/30 rounded-lg text-xs font-bold hover:bg-slate-800 transition-colors cursor-pointer"
            >
              CLOSE SYSTEM HUD
            </button>
          </div>
        </div>
      )}
    </>
  );
}
