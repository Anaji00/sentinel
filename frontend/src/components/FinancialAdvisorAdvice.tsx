'use client';

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import { MetricCard } from './ui/MetricCard';

interface TechnicalIndicators {
  rsi: number;
  ema_12: number;
  ema_26: number;
  atr: number;
  current_price: number;
}

interface FibLevels {
  [key: string]: number;
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
  technical_indicators: TechnicalIndicators;
  fib_levels: FibLevels;
  quantitative_rationale: string;
}

interface AdviceBrief {
  market_regime: string;
  highest_conviction_plays: TradingSignal[];
  general_hedging_strategy: string;
}

interface AdviceResponse {
  agent: string;
  agent_run_id: string;
  trace_id: string;
  created_at: string;
  brief?: AdviceBrief;
  message?: string;
}

export default function FinancialAdvisorAdvice() {
  const { data, error, isLoading } = useSWR<AdviceResponse>(
    '/financial/advice',
    fetcher,
    { refreshInterval: 10000 }
  );

  if (error) {
    return (
      <Card title="QUANT PORTFOLIO ALLOCATOR" badge={<Badge variant="anomaly">ERROR</Badge>}>
        <div className="text-rose-400 text-xs font-mono p-4 text-center">
          Failed to synchronize financial advisor telemetry stream.
        </div>
      </Card>
    );
  }

  if (isLoading) {
    return (
      <Card title="QUANT PORTFOLIO ALLOCATOR" badge={<Badge variant="live" pulse>LOADING</Badge>}>
        <div className="flex-1 flex items-center justify-center p-8">
          <div className="text-slate-400 text-xs font-mono animate-pulse flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-[#66fcf1] animate-ping" />
            Synchronizing Kelly Portfolio Allocations...
          </div>
        </div>
      </Card>
    );
  }

  const brief = data?.brief;
  const plays = brief?.highest_conviction_plays || [];

  return (
    <Card
      title="QUANT PORTFOLIO ALLOCATOR"
      badge={
        brief?.market_regime ? (
          <Badge variant="info">REGIME: {brief.market_regime}</Badge>
        ) : (
          <Badge variant="live" pulse>ACTIVE</Badge>
        )
      }
      noPadding
    >
      <div className="p-4 space-y-5">
        {plays.length === 0 ? (
          <div className="text-slate-400 text-xs font-mono text-center py-12 glass-panel rounded-lg">
            {data?.message || 'No high-conviction quantitative plays evaluated yet.'}
          </div>
        ) : (
          plays.map((play, idx) => {
            const isBuy = play.action === 'BUY';
            const isSell = play.action === 'SELL';
            const indicators = play.technical_indicators || {};
            const fibs = play.fib_levels || {};

            return (
              <div
                key={idx}
                className="glass-panel glass-panel-hover rounded-lg overflow-hidden border border-cyan-500/15"
              >
                {/* Play Header */}
                <div className="px-3.5 py-2.5 bg-slate-950/60 border-b border-cyan-500/10 flex justify-between items-center">
                  <div className="flex items-center gap-2">
                    <span className="text-base font-extrabold text-slate-100 font-mono tracking-tight">
                      {play.ticker}
                    </span>
                    <Badge
                      variant={isBuy ? 'success' : isSell ? 'anomaly' : 'neutral'}
                      pulse={isBuy || isSell}
                    >
                      {play.action}
                    </Badge>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <span className="text-[10px] font-mono text-slate-400">Conviction:</span>
                    <Badge variant="info">{(play.conviction_score * 100).toFixed(0)}%</Badge>
                  </div>
                </div>

                {/* Capital Allocation & Risk/Reward */}
                <div className="p-3 grid grid-cols-2 gap-3 border-b border-cyan-500/10 bg-slate-900/30">
                  <MetricCard
                    label="Kelly Allocation"
                    value={`${play.kelly_allocation_pct.toFixed(1)}%`}
                    subtext="Optimal Capital Stake"
                  />
                  <MetricCard
                    label="Risk / Reward"
                    value={`1 : ${play.risk_reward_ratio.toFixed(2)}`}
                    subtext="Asymmetric Target Ratio"
                  />
                </div>

                {/* Order Levels */}
                <div className="p-3 grid grid-cols-3 gap-2 text-center font-mono border-b border-cyan-500/10 bg-slate-950/40">
                  <div>
                    <div className="text-[9px] text-slate-400 uppercase">Entry</div>
                    <div className="text-xs text-slate-100 font-bold mt-0.5">${play.entry_level.toFixed(2)}</div>
                  </div>
                  <div>
                    <div className="text-[9px] text-slate-400 uppercase">Stop Loss</div>
                    <div className="text-xs text-rose-400 font-bold mt-0.5">${play.stop_loss.toFixed(2)}</div>
                  </div>
                  <div>
                    <div className="text-[9px] text-slate-400 uppercase">Target Price</div>
                    <div className="text-xs text-emerald-400 font-bold mt-0.5">${play.target_price.toFixed(2)}</div>
                  </div>
                </div>

                {/* Technical Analysis Indicators */}
                <div className="p-3 font-mono space-y-2 border-b border-cyan-500/10">
                  <div className="text-slate-400 uppercase text-[9px] font-bold tracking-wider">
                    Technical Indicators
                  </div>
                  <div className="grid grid-cols-3 gap-2">
                    <div className="p-2 rounded bg-slate-900/50 border border-cyan-500/10">
                      <div className="text-[9px] text-slate-400">RSI (14)</div>
                      <div
                        className={`text-xs font-bold mt-0.5 ${
                          (indicators.rsi || 50) >= 70
                            ? 'text-rose-400'
                            : (indicators.rsi || 50) <= 30
                            ? 'text-emerald-400'
                            : 'text-slate-100'
                        }`}
                      >
                        {(indicators.rsi || 50).toFixed(1)}
                      </div>
                    </div>
                    <div className="p-2 rounded bg-slate-900/50 border border-cyan-500/10">
                      <div className="text-[9px] text-slate-400">ATR Volatility</div>
                      <div className="text-xs font-bold text-slate-100 mt-0.5">
                        ${(indicators.atr || 0).toFixed(2)}
                      </div>
                    </div>
                    <div className="p-2 rounded bg-slate-900/50 border border-cyan-500/10">
                      <div className="text-[9px] text-slate-400">EMA Cross</div>
                      <div
                        className={`text-xs font-bold mt-0.5 ${
                          (indicators.ema_12 || 0) >= (indicators.ema_26 || 0)
                            ? 'text-emerald-400'
                            : 'text-rose-400'
                        }`}
                      >
                        {(indicators.ema_12 || 0) >= (indicators.ema_26 || 0) ? 'BULL' : 'BEAR'}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Fibonacci Levels */}
                {Object.keys(fibs).length > 0 && (
                  <div className="p-3 font-mono space-y-1.5 bg-slate-950/20 border-b border-cyan-500/10">
                    <div className="text-slate-400 uppercase text-[9px] font-bold tracking-wider">
                      Fibonacci Retracement Matrix
                    </div>
                    <div className="grid grid-cols-4 gap-1.5 text-[9px] text-slate-400">
                      {['0.236', '0.382', '0.500', '0.618'].map((lvl) => (
                        <div
                          key={lvl}
                          className="p-1.5 rounded bg-slate-900/60 border border-cyan-500/10 text-center"
                        >
                          <div>{lvl}</div>
                          <div className="text-slate-100 font-bold mt-0.5">${fibs[lvl]?.toFixed(2)}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Rationale */}
                <div className="p-3.5 text-xs text-slate-300 leading-relaxed bg-slate-950/40">
                  <div className="text-cyan-400 font-mono text-[9px] uppercase font-bold tracking-wider mb-1">
                    Quant Rationale
                  </div>
                  {play.quantitative_rationale}
                </div>
              </div>
            );
          })
        )}

        {/* Hedging strategy */}
        {brief?.general_hedging_strategy && (
          <div className="p-4 rounded-lg bg-amber-950/20 border border-amber-500/30 glass-panel">
            <h4 className="text-xs font-bold text-amber-400 uppercase tracking-wider mb-2 font-mono flex items-center gap-2">
              <span className="h-2 w-2 rounded-full bg-amber-400 animate-ping" />
              SYSTEMIC HEDGING MATRIX
            </h4>
            <p className="text-xs text-slate-300 leading-relaxed font-sans">
              {brief.general_hedging_strategy}
            </p>
          </div>
        )}
      </div>
    </Card>
  );
}
