'use client';

import React, { useState } from 'react';
import useSWR, { mutate } from 'swr';
import { fetcher } from '../lib/api';
import { Shield, ShieldAlert, Power, Sliders, RefreshCw, AlertOctagon, CheckCircle2, Lock } from 'lucide-react';
import { Badge } from './ui/Badge';

interface FlagConfig {
  description: string;
  enabled: boolean;
  rollout_pct: number;
  enabled_tickers: string[];
  kill_switched: boolean;
  reason: string;
  updated_at: string;
  updated_by: string;
}

interface FlagsResponse {
  master_kill_switch: {
    active: boolean;
    reason: string;
    tripped_at?: string;
  };
  signals: Record<string, FlagConfig>;
}

export default function FeatureFlagManager() {
  const { data, error, isLoading } = useSWR<FlagsResponse>('/flags', fetcher, { refreshInterval: 5000 });
  const [processingFlag, setProcessingFlag] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);

  const handleToggle = async (flagName: string, currentEnabled: boolean, rollout: number) => {
    setProcessingFlag(flagName);
    try {
      const res = await fetch('/api/v1/flags/toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          flag_name: flagName,
          enabled: !currentEnabled,
          rollout_pct: rollout,
          reason: 'Manual dashboard toggle',
        }),
      });
      if (res.ok) {
        setActionMessage(`Flag '${flagName}' toggled to ${!currentEnabled ? 'ENABLED' : 'DISABLED'}`);
        mutate('/flags');
      }
    } catch (e) {
      console.error(e);
    } finally {
      setProcessingFlag(null);
      setTimeout(() => setActionMessage(null), 4000);
    }
  };

  const handleTripKillSwitch = async (flagName: string) => {
    setProcessingFlag(flagName);
    try {
      const res = await fetch('/api/v1/flags/kill-switch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          flag_name: flagName,
          reason: 'Operator Emergency Shutdown',
        }),
      });
      if (res.ok) {
        setActionMessage(`🚨 Kill switch TRIPPED for ${flagName}`);
        mutate('/flags');
      }
    } catch (e) {
      console.error(e);
    } finally {
      setProcessingFlag(null);
      setTimeout(() => setActionMessage(null), 4000);
    }
  };

  const handleResetKillSwitch = async (flagName: string) => {
    setProcessingFlag(flagName);
    try {
      const res = await fetch('/api/v1/flags/reset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ flag_name: flagName }),
      });
      if (res.ok) {
        setActionMessage(`✅ Kill switch RESET for ${flagName}`);
        mutate('/flags');
      }
    } catch (e) {
      console.error(e);
    } finally {
      setProcessingFlag(null);
      setTimeout(() => setActionMessage(null), 4000);
    }
  };

  const isMasterActive = data?.master_kill_switch?.active;

  return (
    <div className="bg-[#0b0e17]/90 border border-slate-800/80 rounded-2xl p-5 space-y-5 font-mono text-xs text-slate-200 shadow-xl backdrop-blur-md">
      {/* Header & Master Control */}
      <div className="flex items-center justify-between border-b border-slate-800/80 pb-4">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-xl bg-indigo-500/10 border border-indigo-500/30 text-indigo-400">
            <Sliders className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-sm font-bold tracking-wider text-slate-100 uppercase">
                Signal Governance & Kill Switches
              </h2>
              <Badge variant={isMasterActive ? 'anomaly' : 'success'}>
                {isMasterActive ? 'EMERGENCY HALT' : 'OPERATIONAL'}
              </Badge>
            </div>
            <p className="text-[11px] text-slate-400">
              Zero-downtime platform-wide signal gating and gradual rollout management
            </p>
          </div>
        </div>

        {/* Master Kill Switch Button */}
        <button
          onClick={() => (isMasterActive ? handleResetKillSwitch('MASTER') : handleTripKillSwitch('MASTER'))}
          disabled={processingFlag === 'MASTER'}
          className={`flex items-center gap-2 px-3.5 py-2 rounded-xl font-bold uppercase tracking-wider text-xs border transition-all cursor-pointer shadow-lg ${
            isMasterActive
              ? 'bg-emerald-950/60 text-emerald-400 border-emerald-500/50 hover:bg-emerald-900/60'
              : 'bg-rose-950/60 text-rose-400 border-rose-500/50 hover:bg-rose-900/70 hover:shadow-rose-500/20'
          }`}
        >
          <AlertOctagon className="w-4 h-4" />
          {isMasterActive ? 'RESET MASTER KILL SWITCH' : 'TRIP MASTER KILL SWITCH'}
        </button>
      </div>

      {actionMessage && (
        <div className="px-3.5 py-2 rounded-xl bg-cyan-950/50 border border-cyan-500/40 text-cyan-300 text-[11px] flex items-center gap-2 animate-fadeIn">
          <CheckCircle2 className="w-4 h-4 text-cyan-400" />
          {actionMessage}
        </div>
      )}

      {/* Signal Flag Cards Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5">
        {Object.entries(data?.signals || {}).map(([key, flag]) => {
          const isKilled = flag.kill_switched || !flag.enabled || isMasterActive;

          return (
            <div
              key={key}
              className={`p-4 rounded-xl border transition-all space-y-3 ${
                isKilled
                  ? 'bg-slate-950/40 border-rose-950/60 text-slate-400'
                  : 'bg-slate-900/40 border-slate-800/80 hover:border-slate-700/80 text-slate-200'
              }`}
            >
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="font-bold text-xs text-slate-100">{key}</span>
                    <Badge variant={flag.kill_switched ? 'anomaly' : flag.enabled ? 'success' : 'warning'}>
                      {flag.kill_switched ? 'KILLED' : flag.enabled ? 'ACTIVE' : 'DISABLED'}
                    </Badge>
                  </div>
                  <p className="text-[10px] text-slate-400 mt-1 leading-relaxed">{flag.description}</p>
                </div>
              </div>

              {/* Controls Footer */}
              <div className="flex items-center justify-between pt-2 border-t border-slate-800/60 text-[10px]">
                <div className="flex items-center gap-2 text-slate-400">
                  <span>Rollout: <strong className="text-cyan-400">{flag.rollout_pct}%</strong></span>
                </div>

                <div className="flex items-center gap-2">
                  <button
                    onClick={() => handleToggle(key, flag.enabled, flag.rollout_pct)}
                    disabled={flag.kill_switched || isMasterActive || processingFlag === key}
                    className={`px-2.5 py-1 rounded-lg border text-[10px] font-bold uppercase transition-all cursor-pointer ${
                      flag.enabled
                        ? 'bg-slate-800 text-slate-300 border-slate-700 hover:bg-slate-700'
                        : 'bg-emerald-950/40 text-emerald-400 border-emerald-500/40 hover:bg-emerald-900/50'
                    }`}
                  >
                    {flag.enabled ? 'Disable' : 'Enable'}
                  </button>

                  {flag.kill_switched ? (
                    <button
                      onClick={() => handleResetKillSwitch(key)}
                      disabled={processingFlag === key}
                      className="px-2.5 py-1 rounded-lg bg-emerald-950/40 text-emerald-400 border border-emerald-500/40 hover:bg-emerald-900/50 transition-all font-bold cursor-pointer"
                    >
                      Reset
                    </button>
                  ) : (
                    <button
                      onClick={() => handleTripKillSwitch(key)}
                      disabled={processingFlag === key}
                      className="px-2.5 py-1 rounded-lg bg-rose-950/40 text-rose-400 border border-rose-500/40 hover:bg-rose-900/50 transition-all font-bold cursor-pointer"
                    >
                      Kill
                    </button>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
