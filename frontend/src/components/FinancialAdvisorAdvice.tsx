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
  sigma_shock?: number;
  expected_move_usd?: number;
  expected_move_pct?: number;
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
  const plays = brief?.highest_conviction_plays || [];

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
              className="p-3 rounded-lg bg-slate-950 border border-slate-800 hover:border-cyan-500/40 transition-all space-y-2"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-bold text-white">{p.ticker}</span>
                  <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                    p.action === 'BUY' ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40' : 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                  }`}>
                    {p.action}
                  </span>
                  {p.sigma_shock !== undefined && (
                    <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-[#00f2fe]/20 text-[#00f2fe] border border-[#00f2fe]/40">
                      +{p.sigma_shock.toFixed(2)}σ SHOCK
                    </span>
                  )}
                </div>
                <span className="text-xs text-cyan-400 font-bold">
                  KELLY {p.kelly_allocation_pct}%
                </span>
              </div>

              {/* Price & Volatility Sizing Breakdown */}
              <div className="grid grid-cols-3 gap-2 text-[10px] bg-slate-900/60 p-2 rounded border border-slate-800">
                <div>
                  <span className="text-slate-500 block">ENTRY / TARGET</span>
                  <span className="text-slate-200 font-bold">${p.entry_level} / ${p.target_price}</span>
                </div>
                <div>
                  <span className="text-slate-500 block">RISK / REWARD</span>
                  <span className="text-emerald-400 font-bold">{p.risk_reward_ratio}x</span>
                </div>
                <div>
                  <span className="text-slate-500 block">EXPECTED MOVE</span>
                  <span className="text-[#00f2fe] font-bold">
                    {p.expected_move_pct !== undefined ? `+${p.expected_move_pct.toFixed(1)}%` : 'N/A'}
                  </span>
                </div>
              </div>

              <p className="text-[10px] text-slate-400 font-sans leading-snug">
                {p.quantitative_rationale}
              </p>
            </div>
          ))}
        </div>
      </div>
    </Card>
  );
}
