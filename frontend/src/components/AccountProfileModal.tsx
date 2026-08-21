'use client';

import React, { useState } from 'react';
import { AccountProfile } from '../lib/types';

interface AccountProfileModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export const AccountProfileModal: React.FC<AccountProfileModalProps> = ({ isOpen, onClose }) => {
  const [profile] = useState<AccountProfile>({
    id: 'usr_inst_9402',
    name: 'Alexander Vance',
    email: 'vance@sentinel-quant.io',
    role: 'Managing Director, Quantitative Research',
    tier: 'INSTITUTIONAL',
    apiKey: '••••••••••••••••••••••',
    apiUsageMonth: 482190,
    apiQuotaMonth: 1000000,
    activeAgents: 8,
    watchlistCount: 142,
    notificationChannels: ['SLACK_HQ', 'WEBSOCKET_STREAM', 'EMAIL_CRITICAL'],
    created_at: '2025-01-15T00:00:00Z',
  });

  const [copiedKey, setCopiedKey] = useState(false);
  const [activeTab, setActiveTab] = useState<'overview' | 'apikeys' | 'telemetry' | 'preferences'>('overview');

  if (!isOpen) return null;

  const handleCopyKey = () => {
    navigator.clipboard.writeText(profile.apiKey);
    setCopiedKey(true);
    setTimeout(() => setCopiedKey(false), 2000);
  };

  const usagePct = Math.round((profile.apiUsageMonth / profile.apiQuotaMonth) * 100);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-md p-4 animate-in fade-in duration-200">
      <div className="relative w-full max-w-2xl bg-[#0b0e17] border border-[#00f2fe]/40 rounded-2xl shadow-[0_0_50px_rgba(0,242,254,0.2)] overflow-hidden font-mono text-slate-200">
        
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 bg-slate-950/80 border-b border-cyan-500/20">
          <div className="flex items-center gap-3">
            <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-white font-bold text-lg shadow-[0_0_15px_rgba(0,242,254,0.5)]">
              AV
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-bold text-white uppercase tracking-wider">{profile.name}</h3>
                <span className="px-2 py-0.5 rounded text-[10px] font-extrabold bg-cyan-950 text-cyan-400 border border-cyan-500/40">
                  {profile.tier} TIER
                </span>
              </div>
              <p className="text-[11px] text-slate-400">{profile.email}</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="h-8 w-8 rounded-lg bg-slate-900 border border-slate-800 text-slate-400 hover:text-white hover:border-cyan-500/50 flex items-center justify-center transition-all"
          >
            ✕
          </button>
        </div>

        {/* Tab Navigation */}
        <div className="flex border-b border-slate-800 bg-slate-950/40 px-6 gap-2 text-xs">
          <button
            onClick={() => setActiveTab('overview')}
            className={`py-3 px-4 font-bold border-b-2 transition-all ${activeTab === 'overview' ? 'border-cyan-400 text-cyan-400 bg-cyan-950/30' : 'border-transparent text-slate-400 hover:text-slate-200'}`}
          >
            ACCOUNT OVERVIEW
          </button>
          <button
            onClick={() => setActiveTab('apikeys')}
            className={`py-3 px-4 font-bold border-b-2 transition-all ${activeTab === 'apikeys' ? 'border-cyan-400 text-cyan-400 bg-cyan-950/30' : 'border-transparent text-slate-400 hover:text-slate-200'}`}
          >
            API KEYS & SECURITY
          </button>
          <button
            onClick={() => setActiveTab('telemetry')}
            className={`py-3 px-4 font-bold border-b-2 transition-all ${activeTab === 'telemetry' ? 'border-cyan-400 text-cyan-400 bg-cyan-950/30' : 'border-transparent text-slate-400 hover:text-slate-200'}`}
          >
            TELEMETRY & QUOTAS
          </button>
        </div>

        {/* Content Body */}
        <div className="p-6 space-y-6 max-h-[60vh] overflow-y-auto">
          {activeTab === 'overview' && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-slate-900/60 rounded-xl border border-slate-800">
                  <span className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">User Role</span>
                  <p className="text-xs font-bold text-white mt-1">{profile.role}</p>
                </div>
                <div className="p-3 bg-slate-900/60 rounded-xl border border-slate-800">
                  <span className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Active Agent Swarms</span>
                  <p className="text-xs font-bold text-emerald-400 mt-1">{profile.activeAgents} Swarm Workers Online</p>
                </div>
                <div className="p-3 bg-slate-900/60 rounded-xl border border-slate-800">
                  <span className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Monitored Watchlist</span>
                  <p className="text-xs font-bold text-cyan-400 mt-1">{profile.watchlistCount} Equities & Wallets</p>
                </div>
                <div className="p-3 bg-slate-900/60 rounded-xl border border-slate-800">
                  <span className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Security Clearance</span>
                  <p className="text-xs font-bold text-purple-400 mt-1">Level 4 Institutional Access</p>
                </div>
              </div>

              {/* Account Status Card */}
              <div className="p-4 bg-gradient-to-r from-cyan-950/30 to-blue-950/30 border border-cyan-500/30 rounded-xl flex items-center justify-between">
                <div>
                  <h4 className="text-xs font-bold text-cyan-300">SENTINEL INSTITUTIONAL SUBSCRIPTION</h4>
                  <p className="text-[11px] text-slate-400 mt-0.5">Full multi-domain intelligence, options flow & microstructures unlocked.</p>
                </div>
                <span className="px-3 py-1 bg-emerald-500/20 text-emerald-400 border border-emerald-500/40 rounded-lg text-xs font-bold">
                  ACTIVE
                </span>
              </div>
            </div>
          )}

          {activeTab === 'apikeys' && (
            <div className="space-y-4">
              <div>
                <label className="text-[11px] text-slate-400 font-bold uppercase">Master API Gateway Key</label>
                <div className="flex items-center gap-2 mt-1.5">
                  <input
                    type="text"
                    readOnly
                    value={profile.apiKey}
                    className="flex-1 bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-xs font-mono text-cyan-300 outline-none"
                  />
                  <button
                    onClick={handleCopyKey}
                    className="px-3 py-2 bg-cyan-950 text-cyan-400 border border-cyan-500/40 rounded-lg text-xs font-bold hover:bg-cyan-900 transition-colors"
                  >
                    {copiedKey ? 'COPIED!' : 'COPY'}
                  </button>
                </div>
                <p className="text-[10px] text-slate-500 mt-1">
                  Required in the <code className="text-cyan-400">X-API-KEY</code> header for REST queries or <code className="text-cyan-400">?api_key=</code> for WebSocket streams.
                </p>
              </div>

              <div className="p-3 bg-slate-900/60 rounded-xl border border-slate-800 space-y-2">
                <span className="text-[11px] font-bold text-slate-300">Security Credentials & Policy</span>
                <ul className="text-[10px] text-slate-400 space-y-1 list-disc list-inside">
                  <li>HMAC SHA-256 header validation enforced across REST endpoints</li>
                  <li>WebSocket handshake token authentication enabled</li>
                  <li>Rate Limit: 2,500 req/min per IP</li>
                </ul>
              </div>
            </div>
          )}

          {activeTab === 'telemetry' && (
            <div className="space-y-4">
              <div>
                <div className="flex justify-between text-xs font-bold mb-1">
                  <span className="text-slate-300">MONTHLY API CONSUMPTION</span>
                  <span className="text-cyan-400">{profile.apiUsageMonth.toLocaleString()} / {profile.apiQuotaMonth.toLocaleString()} reqs ({usagePct}%)</span>
                </div>
                <div className="w-full h-2 bg-slate-950 rounded-full overflow-hidden border border-slate-800">
                  <div
                    className="h-full bg-gradient-to-r from-cyan-500 to-emerald-400 rounded-full transition-all duration-500"
                    style={{ width: `${usagePct}%` }}
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3 text-xs">
                <div className="p-3 bg-slate-900/60 rounded-xl border border-slate-800">
                  <span className="text-[10px] text-slate-400">WebSocket Live Connections</span>
                  <p className="text-sm font-extrabold text-white mt-1">1 / 20 Active</p>
                </div>
                <div className="p-3 bg-slate-900/60 rounded-xl border border-slate-800">
                  <span className="text-[10px] text-slate-400">Avg Gateway Latency</span>
                  <p className="text-sm font-extrabold text-emerald-400 mt-1">12 ms</p>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 bg-slate-950/80 border-t border-slate-800">
          <span className="text-[10px] text-slate-500">SENTINEL EDA v2.4 | AUTHENTICATED SESSION</span>
          <button
            onClick={onClose}
            className="px-4 py-1.5 bg-slate-800 hover:bg-slate-700 text-white rounded-lg text-xs font-bold transition-colors"
          >
            CLOSE
          </button>
        </div>

      </div>
    </div>
  );
};
