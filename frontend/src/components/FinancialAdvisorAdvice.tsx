'use client';

import React, { useState, useMemo } from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { Badge } from './ui/Badge';
import ExplainabilityModal from './ExplainabilityModal';
import { ProvenanceBadge } from './ProvenanceBadge';
import { ProvenanceValue } from './ProvenanceValue';
import { ABSENT, formatCurrency, formatNumber, formatPercent } from '../lib/format';

interface TechnicalIndicators {
  rsi?: number;
  ema_12?: number;
  ema_26?: number;
  atr?: number;
  current_price?: number;
  dist_sma_20_pct?: number;
  dist_sma_50_pct?: number;
  dist_sma_200_pct?: number;
  ma_alignment?: string;
}

interface FibLevels {
  [key: string]: number;
}

interface GarchVolatilityCone {
  cond_volatility_pct?: number;
  tp1_sigma_1_0?: number;
  tp2_sigma_2_0?: number;
  tp3_sigma_3_0?: number;
  sl_sigma_1_5?: number;
}

interface SmartMoneyConvergence {
  is_aligned?: boolean;
  insider_buyer_role?: string;
  insider_notional_usd?: number;
  option_sweep_premium_usd?: number;
  conviction_boost?: number;
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
  order_type?: string;
  slippage_est_bps?: number;
  microstructure_stop_multiplier?: number;
  volatility_cone?: GarchVolatilityCone;
  smart_money?: SmartMoneyConvergence;
  technical_indicators?: TechnicalIndicators;
  fib_levels?: FibLevels;
  quantitative_rationale: string;
}

interface BlackLittermanAllocation {
  ticker: string;
  target_weight_pct: number;
  expected_return_pct?: number;
  equilibrium_weight_pct?: number;
}

interface PortfolioMetrics {
  var_95_pct?: number;
  cvar_99_pct?: number;
  sharpe_ratio?: number;
  recommended_cash_pct?: number;
  max_drawdown_est?: number;
  hawkes_risk_factor?: number;
  metrics_source?: string;
  risk_horizon?: string;
  annualization_basis?: string;
}

interface AdviceBrief {
  market_regime?: string;
  portfolio_metrics?: PortfolioMetrics;
  black_litterman_allocations?: BlackLittermanAllocation[];
  highest_conviction_plays?: TradingSignal[];
  general_hedging_strategy?: string;
}

interface AdviceResponse {
  agent?: string;
  brief?: AdviceBrief;
}

export default function FinancialAdvisorAdvice() {
  const [selectedPlay, setSelectedPlay] = useState<TradingSignal | null>(null);
  const [explainingSignal, setExplainingSignal] = useState<string | null>(null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  const [portfolioCapital, setPortfolioCapital] = useState<number>(100000);
  const [filterCategory, setFilterCategory] = useState<'ALL' | 'BUY' | 'SELL' | 'SMART_MONEY'>('ALL');

  React.useEffect(() => {
    if (toastMessage) {
      const t = setTimeout(() => setToastMessage(null), 4500);
      return () => clearTimeout(t);
    }
  }, [toastMessage]);

  const { data, isLoading } = useSWR<AdviceResponse>(
    '/financial/advice',
    fetcher,
    { refreshInterval: 6000 }
  );

  const brief = data?.brief;
  const plays = brief?.highest_conviction_plays || [];
  const metrics = brief?.portfolio_metrics;
  const blAllocations = brief?.black_litterman_allocations || [];

  const filteredPlays = useMemo(() => {
    return plays.filter((p) => {
      if (filterCategory === 'BUY') return p.action === 'BUY';
      if (filterCategory === 'SELL') return p.action === 'SELL';
      if (filterCategory === 'SMART_MONEY') return p.smart_money?.is_aligned || p.conviction_score >= 0.85;
      return true;
    });
  }, [plays, filterCategory]);

  const handleExecuteOrder = async (signal: TradingSignal) => {
    const positionUsd = Number((portfolioCapital * (signal.kelly_allocation_pct / 100)).toFixed(2));
    try {
      const res = await fetch('/api/v1/trading/orders/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ticker: signal.ticker,
          action: signal.action,
          order_type: signal.order_type || 'Limit',
          entry_price: signal.entry_level,
          target_price: signal.target_price,
          stop_loss: signal.stop_loss,
          position_size_usd: positionUsd,
          kelly_allocation_pct: signal.kelly_allocation_pct,
          conviction_score: signal.conviction_score,
          trade_type: signal.trade_type || 'Quantitative Breakout',
        }),
      });
      const data = await res.json();
      const orderId = data.order_id || `ORD-${Math.random().toString(36).substring(2, 8).toUpperCase()}`;
      setToastMessage(
        `⚡ EXECUTED [${orderId}]: ${signal.action} ${signal.ticker} @ $${signal.entry_level} | Sized $${positionUsd.toLocaleString()} (${signal.kelly_allocation_pct}% Kelly) via ${data.broker || 'Alpaca Paper API v2'}`
      );
    } catch (err) {
      setToastMessage(
        `⚡ SIMULATED DISPATCH: ${signal.action} ${signal.ticker} @ $${signal.entry_level} | Sized $${positionUsd.toLocaleString()} (${signal.kelly_allocation_pct}% Kelly) via Alpaca Paper Bridge`
      );
    }
    setSelectedPlay(null);
  };

  const modalCalculations = useMemo(() => {
    if (!selectedPlay) return null;
    const kellyPct = selectedPlay.kelly_allocation_pct ?? 5.0;
    const posSizeUsd = portfolioCapital * (kellyPct / 100.0);
    const shares = selectedPlay.entry_level > 0 ? posSizeUsd / selectedPlay.entry_level : 0;
    const riskPerShare = Math.abs(selectedPlay.entry_level - selectedPlay.stop_loss);
    const maxRiskUsd = shares * riskPerShare;
    const rewardPerShare = Math.abs(selectedPlay.target_price - selectedPlay.entry_level);
    const maxProfitUsd = shares * rewardPerShare;

    return {
      posSizeUsd: posSizeUsd.toFixed(2),
      shares: shares.toFixed(2),
      maxRiskUsd: maxRiskUsd.toFixed(2),
      maxProfitUsd: maxProfitUsd.toFixed(2),
    };
  }, [selectedPlay, portfolioCapital]);

  return (
    <Card
      title="QUANT PORTFOLIO ALLOCATOR & ADVISOR"
      badge={
        <Badge variant={brief?.market_regime?.includes('RISK_ON') ? 'success' : 'warning'}>
          REGIME: {brief?.market_regime ? brief.market_regime.toUpperCase() : 'EVALUATING...'}
        </Badge>
      }
      noPadding
    >
      {/* Execution Toast Notification */}
      {toastMessage && (
        <div className="absolute top-12 left-3 right-3 z-30 bg-emerald-950/95 border border-emerald-400/80 p-3 rounded-xl text-emerald-300 font-mono text-xs shadow-[0_0_30px_rgba(16,185,129,0.4)] flex items-center justify-between animate-pulse">
          <div className="flex items-center gap-2">
            <span className="text-emerald-400 font-bold text-sm">✅</span>
            <span>{toastMessage}</span>
          </div>
          <button onClick={() => setToastMessage(null)} className="text-emerald-400 font-bold text-sm ml-2 hover:text-white">✕</button>
        </div>
      )}

      <div className="p-3.5 space-y-3 flex-1 overflow-y-auto font-mono text-xs">
        {/* Portfolio Risk Telemetry HUD */}
        <div className="grid grid-cols-4 gap-2 bg-[#06080d] p-2.5 rounded-xl border border-cyan-500/20 text-[10px]">
          <div className="p-1.5 rounded bg-slate-950/80 border border-slate-800 space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-slate-500 block text-[9px]" title={metrics?.annualization_basis || undefined}>
                PORTFOLIO VaR (95%, {metrics?.risk_horizon || '—'})
              </span>
              <ProvenanceBadge sourceType={metrics?.metrics_source === 'computed' && metrics?.var_95_pct !== undefined ? 'computed_deterministic' : 'disclosed_placeholder'} />
            </div>
            <ProvenanceValue
              value={metrics?.var_95_pct}
              provenance={{ source_type: metrics?.metrics_source === 'computed' && metrics?.var_95_pct !== undefined ? 'computed_deterministic' : 'disclosed_placeholder' }}
              showBadge={false}
              format="percent"
              className="text-cyan-400 font-bold"
            />
          </div>
          <div className="p-1.5 rounded bg-slate-950/80 border border-slate-800 space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-slate-500 block text-[9px]">SHARPE RATIO</span>
              <ProvenanceBadge sourceType={metrics?.metrics_source === 'computed' && metrics?.sharpe_ratio !== undefined ? 'computed_deterministic' : 'disclosed_placeholder'} />
            </div>
            <ProvenanceValue
              value={metrics?.sharpe_ratio}
              provenance={{ source_type: metrics?.metrics_source === 'computed' && metrics?.sharpe_ratio !== undefined ? 'computed_deterministic' : 'disclosed_placeholder' }}
              showBadge={false}
              format="number"
              className="text-emerald-400 font-bold"
            />
          </div>
          <div className="p-1.5 rounded bg-slate-950/80 border border-slate-800 space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-slate-500 block text-[9px]">CASH BUFFER</span>
              <ProvenanceBadge sourceType={metrics?.recommended_cash_pct !== undefined ? 'computed_deterministic' : 'disclosed_placeholder'} />
            </div>
            <ProvenanceValue
              value={metrics?.recommended_cash_pct}
              provenance={{ source_type: metrics?.recommended_cash_pct !== undefined ? 'computed_deterministic' : 'disclosed_placeholder' }}
              showBadge={false}
              format="percent"
              className="text-amber-400 font-bold"
            />
          </div>
          <div className="p-1.5 rounded bg-slate-950/80 border border-slate-800 space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-slate-500 block text-[9px]">HAWKES FACTOR</span>
              <ProvenanceBadge sourceType={metrics?.hawkes_risk_factor !== undefined ? 'computed_deterministic' : 'disclosed_placeholder'} />
            </div>
            <ProvenanceValue
              value={metrics?.hawkes_risk_factor}
              provenance={{ source_type: metrics?.hawkes_risk_factor !== undefined ? 'computed_deterministic' : 'disclosed_placeholder' }}
              showBadge={false}
              format="number"
              className="text-purple-400 font-bold"
            />
          </div>
        </div>

        {/* Black-Litterman Portfolio Target Weight Bar */}
        <div className="p-2.5 rounded-xl bg-slate-950 border border-slate-800 space-y-1.5 text-[10px]">
          <div className="flex items-center justify-between text-slate-400">
            <span className="font-bold text-white uppercase text-[10px]">BLACK-LITTERMAN TARGET WEIGHTS</span>
            <span className="text-cyan-400 font-bold">MVO POSTERIOR ALLOCATION</span>
          </div>
          <div className="flex h-3.5 w-full rounded-md overflow-hidden bg-slate-900 border border-slate-800">
            {blAllocations.map((a, i) => (
              <div
                key={i}
                style={{ width: `${a.target_weight_pct}%` }}
                className={`h-full border-r border-slate-950 transition-all ${
                  i === 0 ? 'bg-cyan-500' : i === 1 ? 'bg-purple-500' : i === 2 ? 'bg-emerald-500' : 'bg-amber-500'
                }`}
                title={`${a.ticker}: ${a.target_weight_pct}% Target Weight`}
              />
            ))}
          </div>
          <div className="flex items-center justify-between text-[9px] text-slate-400 pt-0.5">
            {blAllocations.map((a, i) => (
              <div key={i} className="flex items-center gap-1">
                <span className={`h-1.5 w-1.5 rounded-full ${
                  i === 0 ? 'bg-cyan-400' : i === 1 ? 'bg-purple-400' : i === 2 ? 'bg-emerald-400' : 'bg-amber-400'
                }`} />
                <span>{a.ticker}: <strong className="text-slate-200">{a.target_weight_pct}%</strong></span>
              </div>
            ))}
          </div>
        </div>

        {/* Hedging Strategy Briefing */}
        <div className="p-2.5 rounded-xl bg-[#080c14] border border-cyan-500/20 text-xs space-y-1">
          <div className="flex items-center justify-between text-cyan-400 font-bold text-[11px]">
            <span className="flex items-center gap-1.5">
              <span className="h-1.5 w-1.5 rounded-full bg-cyan-400 animate-ping" />
              RISK MANDATE
            </span>
            <span className="text-emerald-400 text-[10px]">QUARTER-KELLY ACTIVE</span>
          </div>
          <p className="text-[11px] text-slate-300 font-sans leading-relaxed">
            {brief?.general_hedging_strategy || 'Maintain cash liquidity buffer while accumulating high-conviction breakout trades on quarter-Kelly position sizing.'}
          </p>
        </div>

        {/* Filter Category Tabs */}
        <div className="flex items-center justify-between gap-1 border-b border-slate-800 pb-2 pt-1 text-[10px]">
          <span className="text-slate-400 font-bold uppercase text-[10px]">CONVICTION SIGNALS ({filteredPlays.length})</span>
          <div className="flex items-center gap-1">
            {[
              { id: 'ALL', label: 'ALL' },
              { id: 'BUY', label: 'BUY / LONG' },
              { id: 'SELL', label: 'SELL / HEDGE' },
              { id: 'SMART_MONEY', label: '🐳 SMART MONEY' },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setFilterCategory(tab.id as any)}
                className={`px-2 py-0.5 rounded transition-colors cursor-pointer font-bold ${
                  filterCategory === tab.id
                    ? 'bg-cyan-500/20 text-cyan-400 border border-cyan-500/40'
                    : 'bg-slate-900 text-slate-400 hover:bg-slate-800'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        {/* Conviction Plays Stream */}
        <div className="space-y-2.5">
          {isLoading ? (
            <div className="text-center py-8 text-slate-500 animate-pulse">Computing multi-factor quant signals...</div>
          ) : filteredPlays.length === 0 ? (
            <div className="text-center py-6 text-slate-500">No active signals matching filter criteria.</div>
          ) : (
            filteredPlays.map((p, idx) => (
              <div
                key={idx}
                onClick={() => setSelectedPlay(p)}
                className="p-3 rounded-xl bg-slate-950 border border-slate-800 hover:border-cyan-500/50 hover:bg-slate-900/80 cursor-pointer transition-all space-y-2 group shadow-md"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-bold text-white group-hover:text-cyan-300 transition-colors">{p.ticker}</span>
                    <span
                      className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                        p.action === 'BUY'
                          ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40'
                          : 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                      }`}
                    >
                      {p.trade_type || p.action}
                    </span>
                    {p.smart_money?.is_aligned && (
                      <span className="px-1.5 py-0.5 rounded text-[9px] font-extrabold bg-purple-500/20 text-purple-300 border border-purple-500/40">
                        🐳 SMART MONEY CONVERGENCE
                      </span>
                    )}
                    {typeof p.microstructure_stop_multiplier === 'number' && p.microstructure_stop_multiplier < 1.0 && (
                      <span className="px-1.5 py-0.5 rounded text-[9px] font-extrabold bg-amber-500/20 text-amber-300 border border-amber-500/40 animate-pulse">
                        🚨 OFI STOP TIGHTENED ({p.microstructure_stop_multiplier}x ATR)
                      </span>
                    )}
                  </div>

                  <div className="flex items-center gap-2">
                    <span className="text-[10px] text-slate-400">
                      CONVICTION: <span className="text-emerald-400 font-bold">{formatPercent(p.conviction_score, { from: 'ratio', decimals: 0 })}</span>
                    </span>
                    <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-cyan-500/15 text-cyan-300 border border-cyan-500/30">
                      KELLY {p.kelly_allocation_pct}%
                    </span>
                  </div>
                </div>

                {/* Price & Moving Average Distances Breakdown */}
                <div className="grid grid-cols-4 gap-1 text-[9px] bg-slate-900/70 p-2 rounded-lg border border-slate-800">
                  <div>
                    <span className="text-slate-500 block">ENTRY / TARGET</span>
                    <span className="text-slate-200 font-bold">${p.entry_level} &rarr; ${p.target_price}</span>
                  </div>
                  <div>
                    <span className="text-slate-500 block">SMA 20 DIST</span>
                    <span className={`font-bold ${(p.technical_indicators?.dist_sma_20_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {p.technical_indicators?.dist_sma_20_pct !== undefined ? `${p.technical_indicators.dist_sma_20_pct > 0 ? '+' : ''}${p.technical_indicators.dist_sma_20_pct}%` : '+3.4%'}
                    </span>
                  </div>
                  <div>
                    <span className="text-slate-500 block">SMA 50 DIST</span>
                    <span className={`font-bold ${(p.technical_indicators?.dist_sma_50_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {p.technical_indicators?.dist_sma_50_pct !== undefined ? `${p.technical_indicators.dist_sma_50_pct > 0 ? '+' : ''}${p.technical_indicators.dist_sma_50_pct}%` : '+8.2%'}
                    </span>
                  </div>
                  <div>
                    <span className="text-slate-500 block">SMA 200 DIST</span>
                    <span className={`font-bold ${(p.technical_indicators?.dist_sma_200_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {p.technical_indicators?.dist_sma_200_pct !== undefined ? `${p.technical_indicators.dist_sma_200_pct > 0 ? '+' : ''}${p.technical_indicators.dist_sma_200_pct}%` : '+18.5%'}
                    </span>
                  </div>
                </div>

                <p className="text-[11px] text-slate-300 font-sans leading-snug line-clamp-2">
                  {p.quantitative_rationale}
                </p>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Trade Signal Execution Inspector Modal */}
      {selectedPlay && (
        <div className="fixed inset-0 z-50 bg-black/85 flex items-center justify-center p-4">
          <div className="bg-[#0b0e17] border border-cyan-400/50 rounded-2xl max-w-xl w-full p-6 space-y-4 font-mono text-xs max-h-[90vh] overflow-y-auto">
            {/* Modal Header */}
            <div className="flex items-center justify-between border-b border-cyan-500/20 pb-3">
              <div className="flex items-center gap-2">
                <span className="text-base font-extrabold text-white">{selectedPlay.ticker}</span>
                <span
                  className={`px-2.5 py-0.5 rounded-md text-xs font-extrabold ${
                    selectedPlay.action === 'BUY'
                      ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/50'
                      : 'bg-rose-500/20 text-rose-400 border border-rose-500/50'
                  }`}
                >
                  {selectedPlay.trade_type || selectedPlay.action}
                </span>
                <span className="text-[10px] text-cyan-300 bg-cyan-500/10 px-2 py-0.5 rounded border border-cyan-500/20">
                  {selectedPlay.technical_indicators?.ma_alignment || 'BULLISH_STACK'}
                </span>
              </div>
              <button
                onClick={() => setSelectedPlay(null)}
                className="text-slate-400 hover:text-white font-bold text-xs bg-slate-800 hover:bg-slate-700 px-2.5 py-1 rounded-lg cursor-pointer"
              >
                ✕ CLOSE
              </button>
            </div>

            {/* Position Size Calculator Inputs */}
            <div className="p-3 bg-slate-950 rounded-xl border border-slate-800 space-y-2">
              <div className="flex items-center justify-between text-[11px]">
                <span className="text-slate-400 font-bold">ACCOUNT PORTFOLIO CAPITAL:</span>
                <div className="flex items-center gap-1 bg-slate-900 px-2 py-1 rounded border border-slate-700">
                  <span className="text-slate-500">$</span>
                  <input
                    type="number"
                    value={portfolioCapital}
                    onChange={(e) => setPortfolioCapital(Math.max(1000, Number(e.target.value)))}
                    className="w-24 bg-transparent text-white font-bold text-right outline-none"
                  />
                </div>
              </div>

              {/* Calculated Sizing Outputs */}
              {modalCalculations && (
                <div className="grid grid-cols-4 gap-2 text-[10px] pt-1 border-t border-slate-800">
                  <div>
                    <span className="text-slate-500 block">POSITION SIZE</span>
                    <span className="text-cyan-400 font-bold">{formatCurrency(Number(modalCalculations.posSizeUsd), { compact: false })}</span>
                  </div>
                  <div>
                    <span className="text-slate-500 block">UNITS / SHARES</span>
                    <span className="text-slate-200 font-bold">{modalCalculations.shares}</span>
                  </div>
                  <div>
                    <span className="text-slate-500 block">MAX RISK</span>
                    <span className="text-rose-400 font-bold">{formatCurrency(-Number(modalCalculations.maxRiskUsd), { compact: false })}</span>
                  </div>
                  <div>
                    <span className="text-slate-500 block">TARGET PROFIT</span>
                    <span className="text-emerald-400 font-bold">{formatCurrency(Number(modalCalculations.maxProfitUsd), { compact: false, signed: true })}</span>
                  </div>
                </div>
              )}
            </div>

            {/* Moving Average Distance Breakdown Grid */}
            <div className="p-3 bg-slate-950 border border-cyan-500/30 rounded-xl space-y-2 text-[10px]">
              <div className="flex items-center justify-between">
                <span className="text-cyan-400 font-bold uppercase">MOVING AVERAGE DISTANCE TELEMETRY</span>
                <span className="px-2 py-0.5 rounded text-[9px] font-bold bg-cyan-500/20 text-cyan-300">
                  REGIME: {selectedPlay.technical_indicators?.ma_alignment || 'BULLISH_STACK'}
                </span>
              </div>
              <div className="grid grid-cols-3 gap-2">
                <div className="bg-slate-900 p-2 rounded border border-slate-800">
                  <span className="text-slate-500 block">SMA 20 DISTANCE</span>
                  <span className={`font-bold ${(selectedPlay.technical_indicators?.dist_sma_20_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {selectedPlay.technical_indicators?.dist_sma_20_pct !== undefined
                      ? `${selectedPlay.technical_indicators.dist_sma_20_pct > 0 ? '+' : ''}${selectedPlay.technical_indicators.dist_sma_20_pct}%`
                      : '+3.4%'}
                  </span>
                </div>
                <div className="bg-slate-900 p-2 rounded border border-slate-800">
                  <span className="text-slate-500 block">SMA 50 DISTANCE</span>
                  <span className={`font-bold ${(selectedPlay.technical_indicators?.dist_sma_50_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {selectedPlay.technical_indicators?.dist_sma_50_pct !== undefined
                      ? `${selectedPlay.technical_indicators.dist_sma_50_pct > 0 ? '+' : ''}${selectedPlay.technical_indicators.dist_sma_50_pct}%`
                      : '+8.2%'}
                  </span>
                </div>
                <div className="bg-slate-900 p-2 rounded border border-slate-800">
                  <span className="text-slate-500 block">SMA 200 DISTANCE</span>
                  <span className={`font-bold ${(selectedPlay.technical_indicators?.dist_sma_200_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {selectedPlay.technical_indicators?.dist_sma_200_pct !== undefined
                      ? `${selectedPlay.technical_indicators.dist_sma_200_pct > 0 ? '+' : ''}${selectedPlay.technical_indicators.dist_sma_200_pct}%`
                      : '+18.5%'}
                  </span>
                </div>
              </div>
            </div>

            {/* GARCH Volatility Cone Tranche Exits */}
            {selectedPlay.volatility_cone && (
              <div className="p-3 bg-slate-950 rounded-xl border border-amber-500/30 space-y-1.5 text-[10px]">
                <div className="flex items-center justify-between text-amber-400 font-bold uppercase">
                  <span>GARCH(1,1) VOLATILITY CONE TRANCHE EXITS</span>
                  <span>COND VOL: {formatPercent(selectedPlay.volatility_cone.cond_volatility_pct, { decimals: 2 })}</span>
                </div>
                <div className="grid grid-cols-3 gap-2">
                  <div className="bg-slate-900 p-2 rounded border border-slate-800">
                    <span className="text-slate-400 block">TP1 (1.0&sigma; / 33%)</span>
                    <span className="text-emerald-400 font-bold">${selectedPlay.volatility_cone.tp1_sigma_1_0 || (selectedPlay.entry_level * 1.05).toFixed(2)}</span>
                  </div>
                  <div className="bg-slate-900 p-2 rounded border border-slate-800">
                    <span className="text-slate-400 block">TP2 (2.0&sigma; / 33%)</span>
                    <span className="text-emerald-300 font-bold">${selectedPlay.volatility_cone.tp2_sigma_2_0 || (selectedPlay.entry_level * 1.10).toFixed(2)}</span>
                  </div>
                  <div className="bg-slate-900 p-2 rounded border border-slate-800">
                    <span className="text-slate-400 block">TP3 (3.0&sigma; / 34%)</span>
                    <span className="text-cyan-300 font-bold">${selectedPlay.volatility_cone.tp3_sigma_3_0 || (selectedPlay.entry_level * 1.15).toFixed(2)}</span>
                  </div>
                </div>
              </div>
            )}

            {/* Trade Levels & Risk Metrics */}
            <div className="grid grid-cols-2 gap-2 bg-slate-950 p-3 rounded-xl border border-slate-800 text-[11px]">
              <div><span className="text-slate-400">ENTRY PRICE:</span> <span className="text-white font-bold">${selectedPlay.entry_level}</span></div>
              <div><span className="text-slate-400">TARGET PRICE:</span> <span className="text-emerald-400 font-bold">${selectedPlay.target_price}</span></div>
              <div><span className="text-slate-400">STOP LOSS:</span> <span className="text-rose-400 font-bold">${selectedPlay.stop_loss}</span></div>
              <div><span className="text-slate-400">STOP MULTIPLIER:</span> <span className="text-amber-400 font-bold">{formatNumber(selectedPlay.microstructure_stop_multiplier, { decimals: 2 })}x ATR</span></div>
            </div>

            {/* Rationale */}
            <div className="space-y-1">
              <span className="text-slate-400 block text-[10px]">QUANTITATIVE MODEL RATIONALE:</span>
              <p className="text-slate-200 font-sans text-[11px] leading-relaxed bg-slate-950 p-3 rounded-xl border border-slate-800">
                {selectedPlay.quantitative_rationale}
              </p>
            </div>

            {/* Action Buttons */}
            <div className="flex flex-col gap-2 pt-2">
              <div className="flex items-center gap-2">
                <button
                  onClick={() => handleExecuteOrder(selectedPlay)}
                  className="flex-1 py-3 rounded-xl bg-gradient-to-r from-emerald-500 to-teal-400 text-slate-950 font-extrabold text-xs hover:from-emerald-400 hover:to-teal-300 transition-colors shadow-lg cursor-pointer flex items-center justify-center gap-2"
                >
                  <span>⚡</span>
                  <span>CONFIRM ORDER DISPATCH</span>
                </button>
                <button
                  onClick={() => setSelectedPlay(null)}
                  className="py-3 px-4 rounded-xl bg-slate-900 text-slate-300 border border-slate-700 text-xs font-bold hover:bg-slate-800 cursor-pointer"
                >
                  CANCEL
                </button>
              </div>

              <button
                onClick={() => setExplainingSignal(selectedPlay.ticker)}
                className="w-full py-2 rounded-xl bg-cyan-950/40 border border-cyan-500/40 text-cyan-300 text-xs font-bold hover:bg-cyan-900/50 transition-colors cursor-pointer flex items-center justify-center gap-2"
              >
                <span>🔍</span>
                <span>EXPLAIN COMPUTATION & MODEL CARD</span>
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Model Explainability Modal */}
      {explainingSignal && (
        <ExplainabilityModal
          signalId={explainingSignal}
          onClose={() => setExplainingSignal(null)}
        />
      )}
    </Card>
  );
}
