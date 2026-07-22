'use client';

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';

interface TechnicalIndicators {
  rsi: number;
  ema_12: number;
  ema_26: number;
  atr: number;
  current_price: number;
}

interface TradingSignal {
  ticker: string;
  action: 'BUY' | 'SELL' | 'HOLD';
  entry_level: number;
  target_price: number;
  stop_loss: number;
  risk_reward_ratio: number;
  kelly_allocation_pct: number;
  conviction_score: number;
  technical_indicators?: TechnicalIndicators;
  quantitative_rationale: string;
}

interface AdviceBrief {
  market_regime: string;
  highest_conviction_plays: TradingSignal[];
  general_hedging_strategy: string;
}

interface AdviceResponse {
  agent: string;
  brief?: AdviceBrief;
}

export default function FinancialAdvisorAdvice() {
  const { data } = useSWR<AdviceResponse>(
    '/financial/advice',
    fetcher,
    { refreshInterval: 8000 }
  );

  const brief = data?.brief;
  const plays = brief?.highest_conviction_plays?.length ? brief.highest_conviction_plays : [
    {
      ticker: 'NVDA',
      action: 'BUY' as const,
      entry_level: 122.50,
      target_price: 148.00,
      stop_loss: 116.20,
      risk_reward_ratio: 3.42,
      kelly_allocation_pct: 12.5,
      conviction_score: 0.88,
      quantitative_rationale: 'RSI divergence + 12 EMA cross. Options Put/Call IV skew compression signals upside breakout.',
    },
    {
      ticker: 'XLE',
      action: 'BUY' as const,
      entry_level: 88.20,
      target_price: 104.00,
      stop_loss: 84.50,
      risk_reward_ratio: 2.85,
      kelly_allocation_pct: 8.0,
      conviction_score: 0.76,
      quantitative_rationale: 'Hormuz maritime chokepoint cascade trigger. Macro cointegration hedge against oil supply shock.',
    }
  ];

  return (
    <Card
      title="QUANT PORTFOLIO ALLOCATOR"
      badge={
        <Badge variant="info">
          REGIME: {brief?.market_regime || 'INVERTED YIELD STRESS'}
        </Badge>
      }
      noPadding
    >
      <div className="p-3.5 space-y-3.5 flex-1 overflow-y-auto font-mono">
        {/* Dynamic Hedging Mandate Header */}
        <div className="p-3 rounded-lg bg-[#06080d] border border-cyan-500/20 text-xs space-y-1">
          <div className="flex items-center justify-between text-cyan-400 font-bold">
            <span>RISK MANDATE</span>
            <span className="text-emerald-400">QUARTER-KELLY ACTIVE</span>
          </div>
          <p className="text-[11px] text-slate-300 font-sans leading-relaxed">
            {brief?.general_hedging_strategy || 'Yield curve inversion active. Position sizes capped at 12.5% with strict stop-losses.'}
          </p>
        </div>

        {/* Conviction Plays */}
        <div className="space-y-3">
          {plays.map((p, idx) => (
            <div
              key={idx}
              className="p-3 rounded-xl bg-slate-900/80 border border-cyan-500/20 hover:border-[#00f2fe]/50 transition-all space-y-2 glass-panel-hover"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-black text-white">{p.ticker}</span>
                  <span
                    className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                      p.action === 'BUY'
                        ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40 glow-emerald'
                        : 'bg-rose-500/20 text-rose-400 border border-rose-500/40 glow-crimson'
                    }`}
                  >
                    {p.action}
                  </span>
                </div>
                <span className="text-xs text-amber-400 font-bold">
                  KELLY: {p.kelly_allocation_pct}%
                </span>
              </div>

              {/* Allocation Visual Bar */}
              <div className="w-full h-1.5 rounded-full bg-slate-800 relative overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-[#00f2fe] to-emerald-400"
                  style={{ width: `${Math.min(100, p.kelly_allocation_pct * 4)}%` }}
                />
              </div>

              <div className="grid grid-cols-3 gap-2 text-[10px] text-slate-300 bg-slate-950/60 p-2 rounded-lg border border-slate-800">
                <div>Entry: <span className="text-white font-bold">${p.entry_level}</span></div>
                <div>Target: <span className="text-emerald-400 font-bold">${p.target_price}</span></div>
                <div>Stop: <span className="text-rose-400 font-bold">${p.stop_loss}</span></div>
              </div>

              <p className="text-[11px] text-slate-300 font-sans leading-snug">
                {p.quantitative_rationale}
              </p>
            </div>
          ))}
        </div>
      </div>
    </Card>
  );
}
