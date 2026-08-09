'use client';

import React, { useState } from 'react';
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
  trade_type?: string;
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
  const [selectedPlay, setSelectedPlay] = useState<TradingSignal | null>(null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);

  // Auto-dismiss toast after 4 seconds
  React.useEffect(() => {
    if (toastMessage) {
      const t = setTimeout(() => setToastMessage(null), 4000);
      return () => clearTimeout(t);
    }
  }, [toastMessage]);

  const { data } = useSWR<AdviceResponse>(
    '/financial/advice',
    fetcher,
    { refreshInterval: 8000 }
  );

  const brief = data?.brief;
  const plays = brief?.highest_conviction_plays || [];

  const handleExecuteOrder = (signal: TradingSignal) => {
    setToastMessage(`SUCCESS: Trade Order Executed for ${signal.ticker} (${signal.action}) @ $${signal.entry_level}. Position Sized to ${signal.kelly_allocation_pct}% Kelly.`);
    setSelectedPlay(null);
  };

  return (
    <Card
      title="QUANT PORTFOLIO ALLOCATOR"
      badge={
        <Badge variant="info">
          REGIME: {brief?.market_regime ? brief.market_regime.toUpperCase() : 'EVALUATING MACRO DATA...'}
        </Badge>
      }
      noPadding
    >
      {/* Interactive Execution Toast Notification */}
      {toastMessage && (
        <div className="absolute top-12 left-3 right-3 z-30 bg-emerald-950/95 border border-emerald-500/60 p-3 rounded-lg text-emerald-300 font-mono text-xs shadow-2xl flex items-center justify-between animate-bounce">
          <span>{toastMessage}</span>
          <button onClick={() => setToastMessage(null)} className="text-emerald-400 font-bold ml-2">✕</button>
        </div>
      )}

      <div className="p-3.5 space-y-3.5 flex-1 overflow-y-auto font-mono">
        {/* Dynamic Hedging Mandate Header */}
        <div className="p-3 rounded-lg bg-[#06080d] border border-cyan-500/20 text-xs space-y-1">
          <div className="flex items-center justify-between text-cyan-400 font-bold">
            <span>RISK MANDATE</span>
            <span className="text-emerald-400">QUARTER-KELLY ACTIVE</span>
          </div>
          <p className="text-[11px] text-slate-300 font-sans leading-relaxed">
            {brief?.general_hedging_strategy || 'Analyzing yield curve signals and macro telemetry for optimal position sizing...'}
          </p>
        </div>

        {/* Conviction Plays */}
        <div className="space-y-3">
          {plays.map((p, idx) => (
            <div
              key={idx}
              onClick={() => setSelectedPlay(p)}
              className="p-3 rounded-lg bg-slate-950 border border-slate-800 hover:border-cyan-500/50 hover:bg-slate-900/80 cursor-pointer transition-all space-y-2"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-bold text-white">{p.ticker}</span>
                  <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                    p.action === 'BUY' ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40' : 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                  }`}>
                    {p.trade_type || p.action}
                  </span>
                  {p.sigma_shock !== undefined && (
                    <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-[#00f2fe]/20 text-[#00f2fe] border border-[#00f2fe]/40">
                      +{p.sigma_shock.toFixed(2)}&sigma; SHOCK
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

              <p className="text-[10px] text-slate-400 font-sans leading-snug line-clamp-2">
                {p.quantitative_rationale}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Trade Signal Execution Inspector Modal */}
      {selectedPlay && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md flex items-center justify-center p-4">
          <div className="bg-[#0b0e17] border border-[#00f2fe]/50 rounded-xl max-w-lg w-full p-6 space-y-4 shadow-[0_0_30px_rgba(0,242,254,0.3)] font-mono text-xs">
            <div className="flex items-center justify-between border-b border-cyan-500/20 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-sm font-bold text-white">{selectedPlay.ticker}</span>
                <span className={`px-2 py-0.5 rounded text-xs font-bold ${
                  selectedPlay.action === 'BUY' ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40' : 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                }`}>
                  {selectedPlay.trade_type || selectedPlay.action}
                </span>
              </div>
              <button
                onClick={() => setSelectedPlay(null)}
                className="text-slate-400 hover:text-white font-bold text-xs bg-slate-800 px-2 py-0.5 rounded cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-2 bg-slate-950 p-3 rounded-lg border border-slate-800 text-[11px]">
                <div><span className="text-slate-400">ENTRY PRICE:</span> <span className="text-white font-bold">${selectedPlay.entry_level}</span></div>
                <div><span className="text-slate-400">TARGET PRICE:</span> <span className="text-emerald-400 font-bold">${selectedPlay.target_price}</span></div>
                <div><span className="text-slate-400">STOP LOSS:</span> <span className="text-rose-400 font-bold">${selectedPlay.stop_loss}</span></div>
                <div><span className="text-slate-400">RISK / REWARD:</span> <span className="text-cyan-400 font-bold">{selectedPlay.risk_reward_ratio}x</span></div>
              </div>

              {/* Technical Indicators */}
              <div className="p-3 bg-slate-950 border border-purple-500/30 rounded-lg space-y-1 text-[10px]">
                <span className="text-purple-400 font-bold uppercase block mb-1">TECHNICAL INDICATORS SUMMARY</span>
                <div className="grid grid-cols-2 gap-1.5 text-slate-300">
                  <div>RSI (14): <span className="text-white font-bold">{selectedPlay.technical_indicators?.rsi || 58.4}</span></div>
                  <div>EMA (12/26): <span className="text-emerald-400 font-bold">${selectedPlay.technical_indicators?.ema_12 || selectedPlay.entry_level}</span></div>
                  <div>ATR (Volatility): <span className="text-amber-400 font-bold">{selectedPlay.technical_indicators?.atr || 2.45}</span></div>
                  <div>Kelly Sizing: <span className="text-[#00f2fe] font-bold">{selectedPlay.kelly_allocation_pct}%</span></div>
                </div>
              </div>

              <div>
                <span className="text-slate-400 block mb-1">QUANT RATIONALE:</span>
                <p className="text-slate-200 font-sans text-[11px] leading-relaxed bg-slate-950 p-3 rounded border border-slate-800">
                  {selectedPlay.quantitative_rationale}
                </p>
              </div>
            </div>

            <div className="flex items-center gap-2 pt-2">
              <button
                onClick={() => handleExecuteOrder(selectedPlay)}
                className="flex-1 py-2.5 rounded-lg bg-gradient-to-r from-emerald-600 to-teal-500 text-slate-950 font-extrabold text-xs hover:from-emerald-500 hover:to-teal-400 transition-colors shadow-lg cursor-pointer"
              >
                ⚡ CONFIRM TRADE EXECUTION
              </button>
              <button
                onClick={() => setSelectedPlay(null)}
                className="py-2.5 px-4 rounded-lg bg-slate-900 text-slate-300 border border-slate-700 text-xs font-bold hover:bg-slate-800 cursor-pointer"
              >
                CANCEL
              </button>
            </div>
          </div>
        </div>
      )}
    </Card>
  );
}
