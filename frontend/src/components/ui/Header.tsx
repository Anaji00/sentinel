'use client';

import React, { useState, useEffect } from 'react';
import SystemHealthHUD from '../SystemHealthHUD';
import { apiClient } from '../../lib/api';
import { useTelemetryStore } from '../../lib/store';
import { AccountProfileModal } from '../AccountProfileModal';

export const Header: React.FC = () => {
  const [time, setTime] = useState<string>('');
  const [timezone, setTimezone] = useState<string>('America/New_York');
  const [latency, setLatency] = useState<number>(12);
  const [isProfileOpen, setIsProfileOpen] = useState<boolean>(false);

  const isConnected = useTelemetryStore((state) => state.isConnected);
  // Distinct from "connecting": the feed was refused for lack of a session.
  const authRequired = useTelemetryStore((state) => state.authRequired);

  useEffect(() => {
    const updateClock = () => {
      const now = new Date();
      try {
        const parts = new Intl.DateTimeFormat('en-US', {
          timeZone: timezone,
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hourCycle: 'h23',
          timeZoneName: 'short',
        }).formatToParts(now);

        const p: Record<string, string> = {};
        parts.forEach((part) => {
          p[part.type] = part.value;
        });

        setTime(`${p.hour}:${p.minute}:${p.second} ${p.timeZoneName || ''}`);
      } catch {
        setTime(now.toISOString().substring(11, 19) + ' UTC');
      }
    };
    updateClock();
    const interval = setInterval(updateClock, 1000);
    return () => clearInterval(interval);
  }, [timezone]);

  useEffect(() => {
    const measureLatency = async () => {
      const start = Date.now();
      try {
        await apiClient.get('/health');
        setLatency(Math.max(1, Date.now() - start));
      } catch {
        setLatency(Math.max(1, Date.now() - start));
      }
    };
    measureLatency();
    const timer = setInterval(measureLatency, 10000);
    return () => clearInterval(timer);
  }, []);

  return (
    <header className="h-20 min-h-[80px] w-full bg-[#06080d]/95 border-b border-[#00f2fe]/20 backdrop-blur-2xl flex items-center justify-between px-6 sm:px-8 z-40 shrink-0 select-none shadow-[0_4px_30px_rgba(0,0,0,0.8)] relative gap-4">
      
      {/* Left Brand Identity */}
      <div className="flex items-center gap-3.5 sm:gap-4 shrink-0">
        <div className="h-10 w-10 sm:h-11 sm:w-11 rounded-xl bg-gradient-to-br from-cyan-950 via-slate-950 to-blue-950 border border-[#00f2fe]/60 flex items-center justify-center shadow-[0_0_20px_rgba(0,242,254,0.4)] relative overflow-hidden group shrink-0">
          <div className="absolute inset-0 bg-gradient-to-tr from-[#00f2fe]/30 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
          <span className="text-[#00f2fe] font-mono font-black text-xl sm:text-2xl tracking-tighter drop-shadow-[0_0_10px_rgba(0,242,254,0.8)]">S</span>
        </div>
        <div className="flex flex-col gap-0.5 min-w-0">
          <div className="flex items-center gap-2.5 flex-wrap">
            <h1 className="text-sm sm:text-base font-mono font-black text-white tracking-widest uppercase flex items-center gap-2 whitespace-nowrap">
              SENTINEL <span className="text-[#00f2fe] font-light">|</span> COMMAND HUD
            </h1>
            <span className="px-2.5 py-0.5 rounded-md text-[10px] font-mono font-extrabold tracking-wider bg-[#00f2fe]/10 text-[#00f2fe] border border-[#00f2fe]/30 whitespace-nowrap shadow-[0_0_10px_rgba(0,242,254,0.15)]">
              v2.4 EDA ACTIVE
            </span>
          </div>
          <p className="text-[10px] text-slate-400 font-mono tracking-wide truncate max-w-[360px] sm:max-w-[480px]">
            AUTONOMOUS MULTI-DOMAIN INTELLIGENCE & QUANTITATIVE OPERATIONS
          </p>
        </div>
      </div>

      {/* Center Live Telemetry Ticker & Metrics Bar */}
      <div className="hidden 2xl:flex items-center gap-4 bg-[#080d1a]/90 px-4 py-2 rounded-xl border border-[#00f2fe]/30 backdrop-blur-md shadow-[0_0_15px_rgba(0,242,254,0.1)] font-mono shrink-0">
        <div className="flex items-center gap-3.5 text-xs whitespace-nowrap">
          <span className="flex items-center gap-2 text-slate-300">
            <span className={`h-2 w-2 rounded-full ${isConnected ? 'bg-emerald-400 animate-ping' : authRequired ? 'bg-rose-400' : 'bg-amber-400'}`} />
            <span className="text-[10px] text-slate-400 uppercase tracking-widest font-bold">STREAM:</span>
            {authRequired ? (
              <a
                href="/login"
                className="text-rose-400 font-extrabold text-[11px] underline underline-offset-2 hover:text-rose-300"
              >
                SIGN IN TO STREAM
              </a>
            ) : (
              <span className={isConnected ? 'text-emerald-400 font-extrabold text-[11px]' : 'text-amber-400 font-extrabold text-[11px]'}>
                {isConnected ? 'LIVE (24/7)' : 'CONNECTING...'}
              </span>
            )}
          </span>
          <span className="text-slate-800">|</span>
          <span className="text-[11px] text-amber-300 font-bold flex items-center gap-1.5">
            <span>🚢 AIS TANKERS:</span>
            <span className="text-amber-400 font-black">ACTIVE</span>
          </span>
          <span className="text-slate-800">|</span>
          <span className="text-[11px] text-cyan-300 font-bold flex items-center gap-1.5">
            <span>✈️ ADS-B FLIGHTS:</span>
            <span className="text-[#00f2fe] font-black">TRACKING</span>
          </span>
          <span className="text-slate-800">|</span>
          <span className="text-[11px] text-purple-300 font-bold flex items-center gap-1.5">
            <span>🤖 AGENT SWARM:</span>
            <span className="text-purple-400 font-black">ACTIVE (8)</span>
          </span>
        </div>
      </div>

      {/* Right Controls & Account Profile */}
      <div className="flex items-center gap-3 sm:gap-4 font-mono shrink-0">
        
        {/* Clock & Latency Capsule */}
        <div className="hidden xl:flex items-center gap-3 bg-[#080d1a]/90 px-3.5 py-1.5 rounded-xl border border-slate-800/90 shadow-md shrink-0">
          <div className="flex items-center gap-2">
            <span className="text-xs font-bold text-white font-mono tracking-tight whitespace-nowrap" suppressHydrationWarning>
              {time}
            </span>
            <select
              value={timezone}
              onChange={(e) => setTimezone(e.target.value)}
              className="bg-slate-950 text-[10px] text-cyan-400 font-mono font-bold border border-cyan-500/30 rounded-md px-2 py-1 outline-none cursor-pointer hover:border-cyan-400 transition-colors whitespace-nowrap"
              title="Select Clock Timezone"
            >
              <option value="America/New_York">US EST</option>
              <option value="UTC">UTC</option>
              <option value="America/Chicago">US CST</option>
              <option value="America/Denver">US MST</option>
              <option value="America/Los_Angeles">US PST</option>
              <option value="Europe/London">GMT</option>
              <option value="Europe/Paris">CET</option>
              <option value="Asia/Tokyo">JST</option>
            </select>
          </div>

          <span className="text-slate-800">|</span>

          <div className="px-2 py-0.5 bg-emerald-950/50 text-emerald-400 border border-emerald-500/30 rounded-md text-[10px] font-extrabold flex items-center gap-1.5 whitespace-nowrap">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-ping" />
            {latency}ms
          </div>
        </div>

        {/* System Health HUD Trigger */}
        <div className="shrink-0">
          <SystemHealthHUD />
        </div>

        {/* Persistent Account Profile Button */}
        <button
          onClick={() => setIsProfileOpen(true)}
          className="flex items-center gap-2.5 px-3 py-1.5 rounded-xl bg-[#080d1a] border border-[#00f2fe]/40 hover:border-[#00f2fe] hover:bg-cyan-950/40 text-slate-200 transition-all duration-200 shadow-[0_0_15px_rgba(0,242,254,0.15)] group shrink-0"
          title="Account Profile & Settings"
        >
          <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-white font-black text-xs shadow-[0_0_10px_rgba(0,242,254,0.5)] group-hover:scale-105 transition-transform shrink-0">
            AV
          </div>
          <div className="hidden sm:flex flex-col text-left font-mono gap-0.5 pr-0.5 shrink-0">
            <span className="text-xs font-black text-white leading-none whitespace-nowrap">A. VANCE</span>
            <span className="text-[9px] text-cyan-400 font-bold leading-none whitespace-nowrap">INSTITUTIONAL</span>
          </div>
        </button>

      </div>

      {/* Account Profile Drawer/Modal */}
      <AccountProfileModal isOpen={isProfileOpen} onClose={() => setIsProfileOpen(false)} />
    </header>
  );
};
