'use client';

import React, { useState, useEffect } from 'react';

export const Header: React.FC = () => {
  const [time, setTime] = useState<string>('');
  const [latency, setLatency] = useState<number>(38);

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
    <header className="h-16 w-full bg-[#06080d]/90 border-b border-[#00f2fe]/20 backdrop-blur-2xl flex items-center justify-between px-6 z-50 shrink-0 select-none shadow-[0_4px_20px_rgba(0,0,0,0.5)]">
      {/* Left Brand Title */}
      <div className="flex items-center gap-3.5">
        <div className="h-9 w-9 rounded-xl bg-cyan-950/80 border border-[#00f2fe]/50 flex items-center justify-center shadow-[0_0_20px_rgba(0,242,254,0.4)] relative overflow-hidden group">
          <div className="absolute inset-0 bg-gradient-to-tr from-[#00f2fe]/20 to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
          <span className="text-[#00f2fe] font-mono font-black text-lg tracking-tighter">S</span>
        </div>
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-base font-mono font-extrabold text-white tracking-wider uppercase flex items-center gap-2">
              SENTINEL <span className="text-[#00f2fe] font-normal">|</span> COMMAND HUD
            </h1>
            <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-[#00f2fe]/10 text-[#00f2fe] border border-[#00f2fe]/30 glow-cyan">
              v2.4 EDA
            </span>
          </div>
          <p className="text-[10px] text-slate-400 font-mono tracking-wide">
            REAL-TIME MULTI-DOMAIN INTELLIGENCE & THREAT CORRELATION ENGINE
          </p>
        </div>
      </div>

      {/* Center Tactical Status & Flashpoint Gauge */}
      <div className="hidden lg:flex items-center gap-6">
        {/* Flashpoint Cascade Meter */}
        <div className="flex items-center gap-3 bg-slate-950/80 px-3.5 py-1.5 rounded-lg border border-cyan-500/20 backdrop-blur-md">
          <div className="flex flex-col text-right font-mono">
            <span className="text-[9px] text-slate-400 tracking-wider uppercase">CASCADE FLASHPOINT</span>
            <span className="text-xs font-bold text-amber-400">74.2 / 100 [ELEVATED]</span>
          </div>
          <div className="w-24 h-2 rounded-full bg-slate-800 relative overflow-hidden border border-slate-700">
            <div className="h-full bg-gradient-to-r from-emerald-500 via-amber-400 to-rose-500 w-[74%]" />
          </div>
        </div>

        {/* Operational Status Badges */}
        <div className="flex items-center gap-2.5 font-mono text-xs">
          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 font-semibold shadow-[0_0_12px_rgba(16,185,129,0.2)]">
            <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
            8 DOMAINS ONLINE
          </span>
          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-cyan-500/10 text-cyan-300 border border-cyan-500/30">
            12/12 AGENTS ACTIVE
          </span>
        </div>
      </div>

      {/* Right Controls & Live Clock */}
      <div className="flex items-center gap-5">
        <div className="text-right font-mono">
          <div className="text-xs font-semibold text-slate-200 tracking-tight">
            {time || '2026-07-22 00:21:00 UTC'}
          </div>
          <div className="text-[10px] text-emerald-400 flex items-center justify-end gap-1 font-bold">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-ping" />
            LATENCY: {latency}ms
          </div>
        </div>
      </div>
    </header>
  );
};
