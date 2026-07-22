'use client';

import React, { useState, useEffect } from 'react';
import { Badge } from './Badge';

export const Header: React.FC = () => {
  const [time, setTime] = useState<string>('');
  const [systemLatency, setSystemLatency] = useState<number>(42);

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
    <header className="h-14 w-full bg-[#08090c]/90 border-b border-cyan-500/20 backdrop-blur-xl flex items-center justify-between px-6 z-50 shrink-0">
      {/* Brand Title */}
      <div className="flex items-center gap-3">
        <div className="h-7 w-7 rounded-lg bg-cyan-950/80 border border-[#66fcf1]/40 flex items-center justify-center shadow-[0_0_15px_rgba(102,252,241,0.3)]">
          <span className="text-[#66fcf1] font-mono font-bold text-sm">S</span>
        </div>
        <div>
          <h1 className="text-sm font-mono font-bold text-[#66fcf1] tracking-wider uppercase flex items-center gap-2">
            SENTINEL <span className="text-slate-500 font-normal">|</span> QUANTITATIVE COMMAND HUD
          </h1>
          <p className="text-[10px] text-slate-400 font-mono">AUTONOMOUS MULTI-AGENT MARKET INTELLIGENCE & TELEMETRY</p>
        </div>
      </div>

      {/* Center Status Indicators */}
      <div className="hidden md:flex items-center gap-4">
        <Badge variant="live" pulse>
          SYSTEM ONLINE
        </Badge>
        <Badge variant="info">
          REGIME: BULLISH VOLATILITY (α=0.10)
        </Badge>
        <Badge variant="success">
          SWARM AGENTS: 9/9 ACTIVE
        </Badge>
      </div>

      {/* Right Controls & Clock */}
      <div className="flex items-center gap-4">
        <div className="text-right font-mono">
          <div className="text-xs font-semibold text-slate-200">{time || '2026-07-21 13:22:00 UTC'}</div>
          <div className="text-[9px] text-emerald-400 flex items-center justify-end gap-1">
            <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
            LATENCY: {systemLatency}ms
          </div>
        </div>
      </div>
    </header>
  );
};
