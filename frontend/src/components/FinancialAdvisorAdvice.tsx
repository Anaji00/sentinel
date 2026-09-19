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
import { IconCheck, IconClose, IconSignal, IconTarget } from '@/components/ui/icons';
import { useDialog } from './ui/useDialog';
import { POLL } from './ui/DataProvider';

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

  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(Boolean(selectedPlay), () => setSelectedPlay(null), 'Play detail');
  const [explainingSignal, setExplainingSignal] = useState<string | null>(null);
  const [toastMessage, setToastMessage] = useState<string | null>(null);
  // Kelly sizes against the book, not against a literal.
  //
  // This was `useState(100000)` and nothing ever read the real account, so a
  // 2% Kelly position was 2% of a number typed into the source -- which had
  // been harmless only because the paper book never held anything: a fill
  // landed in a broker discarded with the request, so the balance never moved
  // from 100,000 anyway. Now that a position persists and cash moves, sizing
  // off a stale figure is sizing off the wrong book.
  //
  // The override stays: an operator sizing for capital held elsewhere is a
  // real thing to want. It just is not the default any more.
  const [capitalOverride, setCapitalOverride] = useState<number | null>(null);
  const [filterCategory, setFilterCategory] = useState<'ALL' | 'BUY' | 'SELL' | 'SMART_MONEY'>(
    'ALL',
  );

  React.useEffect(() => {
    if (toastMessage) {
      const t = setTimeout(() => setToastMessage(null), 4500);
      return () => clearTimeout(t);
    }
  }, [toastMessage]);

  const { data, isLoading } = useSWR<AdviceResponse>('/financial/advice', fetcher, {
    refreshInterval: POLL.live,
  });

  const { data: account } = useSWR<{ portfolio_value: number; buying_power: number }>(
    '/portfolio/account',
    fetcher,
    { refreshInterval: POLL.standard },
  );

  // The override, then the real book, then the historical default -- which is
  // reached only while the account request is still in flight.
  const portfolioCapital = capitalOverride ?? account?.portfolio_value ?? 100000;
  const capitalIsFromBook = capitalOverride === null && account?.portfolio_value !== undefined;

  const brief = data?.brief;
  const plays = brief?.highest_conviction_plays || [];
  const metrics = brief?.portfolio_metrics;
  const blAllocations = brief?.black_litterman_allocations || [];

  const filteredPlays = useMemo(() => {
    return plays.filter((p) => {
      if (filterCategory === 'BUY') return p.action === 'BUY';
      if (filterCategory === 'SELL') return p.action === 'SELL';
      if (filterCategory === 'SMART_MONEY')
        return p.smart_money?.is_aligned || p.conviction_score >= 0.85;
      return true;
    });
  }, [plays, filterCategory]);

  const handleExecuteOrder = async (signal: TradingSignal) => {
    const positionUsd = Number((portfolioCapital * (signal.kelly_allocation_pct / 100)).toFixed(2));

    // An entry price of zero is not a degraded signal, it is an instruction that
    // cannot be followed -- and the endpoint divides the position size by
    // max(0.01, entry_price), so a zero would submit an order for a hundred
    // times the intended dollar value. Refuse before it reaches the wire.
    if (!Number.isFinite(signal.entry_level) || signal.entry_level <= 0) {
      setToastMessage(
        `REFUSED: ${signal.ticker} carries no entry price ($${signal.entry_level}). Nothing was sent.`,
      );
      setSelectedPlay(null);
      return;
    }

    try {
      const res = await fetch('/api/proxy/api/v1/trading/orders/execute', {
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

      // fetch() rejects only on a network failure, so a 400, 403 or 500 arrives
      // here as a resolved response. Without this check the rejection fell into
      // the success branch, data.order_id was undefined, and the `||` below
      // minted a random string that was shown to the operator as confirmation
      // of an order the broker had refused.
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = (typeof data?.detail === 'string' && data.detail) || `HTTP ${res.status}`;
        setToastMessage(`REJECTED: ${signal.action} ${signal.ticker} — ${detail}`);
        setSelectedPlay(null);
        return;
      }

      // Only a response that actually carries an order id is a placed order.
      if (!data.order_id) {
        setToastMessage(
          `UNCONFIRMED: ${signal.action} ${signal.ticker} — the broker returned no order id. Check the account before retrying.`,
        );
        setSelectedPlay(null);
        return;
      }

      setToastMessage(
        `EXECUTED [${data.order_id}]: ${signal.action} ${signal.ticker} @ $${signal.entry_level} | Sized ${formatCurrency(positionUsd, { decimals: 0 })} (${signal.kelly_allocation_pct}% Kelly) via ${data.broker || 'broker'}`,
      );
    } catch (err) {
      // A genuine network failure. The request did not arrive, so nothing was
      // placed -- the old copy here read "SIMULATED DISPATCH ... via Alpaca
      // Paper Bridge", which a reader takes for a successful paper trade.
      setToastMessage(
        `NOT SENT: ${signal.action} ${signal.ticker} — could not reach the trading API. No order was placed.`,
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
      title="Portfolio"
      badge={
        <Badge variant={brief?.market_regime?.includes('RISK_ON') ? 'success' : 'warning'}>
          REGIME: {brief?.market_regime ? brief.market_regime.toUpperCase() : 'EVALUATING...'}
        </Badge>
      }
      noPadding
    >
      {/* Execution Toast Notification */}
      {toastMessage && (
        <div className="absolute top-12 left-3 right-3 z-30 bg-emerald-950/95 border border-emerald-400/80 p-3 rounded-xl text-emerald-300 text-xs shadow-panel flex items-center justify-between animate-pulse">
          <div className="flex items-center gap-2">
            <span className="text-emerald-400 font-bold text-sm">
              <IconCheck className="inline-block shrink-0" />
            </span>
            <span>{toastMessage}</span>
          </div>
          <button
            onClick={() => setToastMessage(null)}
            aria-label="Dismiss"
            className="text-emerald-400 font-bold text-sm ml-2 hover:text-white"
          >
            <IconClose />
          </button>
        </div>
      )}

      <div className="p-3.5 space-y-3 flex-1 overflow-y-auto text-xs">
        {/* Portfolio Risk Telemetry HUD */}
        <div className="grid grid-cols-4 gap-2 bg-inset p-2.5 rounded-xl border border-cyan-500/20 text-micro">
          <div className="p-1.5 rounded bg-page/80 border border-line space-y-1">
            <div className="flex items-center justify-between">
              <span
                className="text-ink-mute block text-micro"
                title={metrics?.annualization_basis || undefined}
              >
                PORTFOLIO VaR (95%, {metrics?.risk_horizon || '—'})
              </span>
              <ProvenanceBadge
                sourceType={
                  metrics?.metrics_source === 'computed' && metrics?.var_95_pct !== undefined
                    ? 'computed_deterministic'
                    : 'disclosed_placeholder'
                }
              />
            </div>
            <ProvenanceValue
              value={metrics?.var_95_pct}
              provenance={{
                source_type:
                  metrics?.metrics_source === 'computed' && metrics?.var_95_pct !== undefined
                    ? 'computed_deterministic'
                    : 'disclosed_placeholder',
              }}
              showBadge={false}
              format="percent"
              className="text-cyan-400 font-bold"
            />
          </div>
          <div className="p-1.5 rounded bg-page/80 border border-line space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-ink-mute block text-micro">SHARPE RATIO</span>
              <ProvenanceBadge
                sourceType={
                  metrics?.metrics_source === 'computed' && metrics?.sharpe_ratio !== undefined
                    ? 'computed_deterministic'
                    : 'disclosed_placeholder'
                }
              />
            </div>
            <ProvenanceValue
              value={metrics?.sharpe_ratio}
              provenance={{
                source_type:
                  metrics?.metrics_source === 'computed' && metrics?.sharpe_ratio !== undefined
                    ? 'computed_deterministic'
                    : 'disclosed_placeholder',
              }}
              showBadge={false}
              format="number"
              className="text-emerald-400 font-bold"
            />
          </div>
          <div className="p-1.5 rounded bg-page/80 border border-line space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-ink-mute block text-micro">CASH BUFFER</span>
              <ProvenanceBadge
                sourceType={
                  metrics?.recommended_cash_pct !== undefined
                    ? 'computed_deterministic'
                    : 'disclosed_placeholder'
                }
              />
            </div>
            <ProvenanceValue
              value={metrics?.recommended_cash_pct}
              provenance={{
                source_type:
                  metrics?.recommended_cash_pct !== undefined
                    ? 'computed_deterministic'
                    : 'disclosed_placeholder',
              }}
              showBadge={false}
              format="percent"
              className="text-amber-400 font-bold"
            />
          </div>
          <div className="p-1.5 rounded bg-page/80 border border-line space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-ink-mute block text-micro">HAWKES FACTOR</span>
              <ProvenanceBadge
                sourceType={
                  metrics?.hawkes_risk_factor !== undefined
                    ? 'computed_deterministic'
                    : 'disclosed_placeholder'
                }
              />
            </div>
            <ProvenanceValue
              value={metrics?.hawkes_risk_factor}
              provenance={{
                source_type:
                  metrics?.hawkes_risk_factor !== undefined
                    ? 'computed_deterministic'
                    : 'disclosed_placeholder',
              }}
              showBadge={false}
              format="number"
              className="text-purple-400 font-bold"
            />
          </div>
        </div>

        {/* Black-Litterman Portfolio Target Weight Bar */}
        <div className="p-2.5 rounded-xl bg-page border border-line space-y-1.5 text-micro">
          <div className="flex items-center justify-between text-ink-dim">
            <span className="font-semibold text-ink text-xs">Black-Litterman target weights</span>
            <span className="text-cyan-400 font-bold">MVO POSTERIOR ALLOCATION</span>
          </div>
          <div className="flex h-3.5 w-full rounded-md overflow-hidden bg-raised border border-line">
            {blAllocations.map((a, i) => (
              <div
                key={i}
                style={{ width: `${a.target_weight_pct}%` }}
                className={`h-full border-r border-line transition-all ${
                  i === 0
                    ? 'bg-cyan-500'
                    : i === 1
                      ? 'bg-purple-500'
                      : i === 2
                        ? 'bg-emerald-500'
                        : 'bg-amber-500'
                }`}
                title={`${a.ticker}: ${a.target_weight_pct}% Target Weight`}
              />
            ))}
          </div>
          <div className="flex items-center justify-between text-micro text-ink-dim pt-0.5">
            {blAllocations.map((a, i) => (
              <div key={i} className="flex items-center gap-1">
                <span
                  className={`h-1.5 w-1.5 rounded-full ${
                    i === 0
                      ? 'bg-cyan-400'
                      : i === 1
                        ? 'bg-purple-400'
                        : i === 2
                          ? 'bg-emerald-400'
                          : 'bg-amber-400'
                  }`}
                />
                <span>
                  {a.ticker}: <strong className="text-ink">{a.target_weight_pct}%</strong>
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Hedging Strategy Briefing */}
        <div className="p-2.5 rounded-xl bg-[#080c14] border border-cyan-500/20 text-xs space-y-1">
          <div className="flex items-center justify-between text-cyan-400 font-bold text-micro">
            <span className="flex items-center gap-1.5">
              <span className="h-1.5 w-1.5 rounded-full bg-cyan-400 animate-ping" />
              RISK MANDATE
            </span>
            <span className="text-emerald-400 text-micro">QUARTER-KELLY ACTIVE</span>
          </div>
          <p className="text-micro text-ink-dim font-sans leading-relaxed">
            {brief?.general_hedging_strategy ||
              'Maintain cash liquidity buffer while accumulating high-conviction breakout trades on quarter-Kelly position sizing.'}
          </p>
        </div>

        {/* Filter Category Tabs */}
        <div className="flex items-center justify-between gap-1 border-b border-line pb-2 pt-1 text-micro">
          <span className="text-ink-dim font-bold uppercase text-micro">
            CONVICTION SIGNALS ({filteredPlays.length})
          </span>
          <div className="flex items-center gap-1">
            {[
              { id: 'ALL', label: 'ALL' },
              { id: 'BUY', label: 'BUY / LONG' },
              { id: 'SELL', label: 'SELL / HEDGE' },
              { id: 'SMART_MONEY', label: 'SMART MONEY' },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setFilterCategory(tab.id as any)}
                className={`px-2 py-0.5 rounded transition-colors cursor-pointer font-bold ${
                  filterCategory === tab.id
                    ? 'bg-cyan-500/20 text-cyan-400 border border-cyan-500/40'
                    : 'bg-raised text-ink-dim hover:bg-overlay'
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
            <div className="text-center py-8 text-ink-mute animate-pulse">
              Computing multi-factor quant signals...
            </div>
          ) : filteredPlays.length === 0 ? (
            <div className="text-center py-6 text-ink-mute">
              No active signals matching filter criteria.
            </div>
          ) : (
            filteredPlays.map((p, idx) => (
              <div
                key={idx}
                onClick={() => setSelectedPlay(p)}
                className="p-3 rounded-xl bg-page border border-line hover:border-cyan-500/50 hover:bg-raised/80 cursor-pointer transition-all space-y-2 group shadow-md"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-bold text-white group-hover:text-cyan-300 transition-colors">
                      {p.ticker}
                    </span>
                    <span
                      className={`px-2 py-0.5 rounded text-micro font-bold ${
                        p.action === 'BUY'
                          ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40'
                          : 'bg-rose-500/20 text-rose-400 border border-rose-500/40'
                      }`}
                    >
                      {p.trade_type || p.action}
                    </span>
                    {p.smart_money?.is_aligned && (
                      <span className="px-1.5 py-0.5 rounded text-micro font-extrabold bg-purple-500/20 text-purple-300 border border-purple-500/40">
                        Institutional agreement
                      </span>
                    )}
                    {typeof p.microstructure_stop_multiplier === 'number' &&
                      p.microstructure_stop_multiplier < 1.0 && (
                        <span className="px-1.5 py-0.5 rounded text-micro font-extrabold bg-amber-500/20 text-amber-300 border border-amber-500/40 animate-pulse">
                          OFI STOP TIGHTENED ({p.microstructure_stop_multiplier}x ATR)
                        </span>
                      )}
                  </div>

                  <div className="flex items-center gap-2">
                    <span className="text-micro text-ink-dim">
                      CONVICTION:{' '}
                      <span className="text-emerald-400 font-bold">
                        {formatPercent(p.conviction_score, { from: 'ratio', decimals: 0 })}
                      </span>
                    </span>
                    <span className="px-2 py-0.5 rounded text-micro font-bold bg-cyan-500/15 text-cyan-300 border border-cyan-500/30">
                      KELLY {p.kelly_allocation_pct}%
                    </span>
                  </div>
                </div>

                {/* Price & Moving Average Distances Breakdown */}
                <div className="grid grid-cols-4 gap-1 text-micro bg-raised/70 p-2 rounded-lg border border-line">
                  <div>
                    <span className="text-ink-mute block">ENTRY / TARGET</span>
                    <span className="text-ink font-bold">
                      ${p.entry_level} &rarr; ${p.target_price}
                    </span>
                  </div>
                  <div>
                    <span className="text-ink-mute block">SMA 20 DIST</span>
                    <span
                      className={`font-bold ${(p.technical_indicators?.dist_sma_20_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                    >
                      {p.technical_indicators?.dist_sma_20_pct !== undefined
                        ? `${p.technical_indicators.dist_sma_20_pct > 0 ? '+' : ''}${p.technical_indicators.dist_sma_20_pct}%`
                        : '+3.4%'}
                    </span>
                  </div>
                  <div>
                    <span className="text-ink-mute block">SMA 50 DIST</span>
                    <span
                      className={`font-bold ${(p.technical_indicators?.dist_sma_50_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                    >
                      {p.technical_indicators?.dist_sma_50_pct !== undefined
                        ? `${p.technical_indicators.dist_sma_50_pct > 0 ? '+' : ''}${p.technical_indicators.dist_sma_50_pct}%`
                        : '+8.2%'}
                    </span>
                  </div>
                  <div>
                    <span className="text-ink-mute block">SMA 200 DIST</span>
                    <span
                      className={`font-bold ${(p.technical_indicators?.dist_sma_200_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                    >
                      {p.technical_indicators?.dist_sma_200_pct !== undefined
                        ? `${p.technical_indicators.dist_sma_200_pct > 0 ? '+' : ''}${p.technical_indicators.dist_sma_200_pct}%`
                        : '+18.5%'}
                    </span>
                  </div>
                </div>

                <p className="text-micro text-ink-dim font-sans leading-snug line-clamp-2">
                  {p.quantitative_rationale}
                </p>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Trade Signal Execution Inspector Modal */}
      {selectedPlay && (
        <div
          className="fixed inset-0 z-50 bg-black/85 flex items-center justify-center p-4"
          {...dialog.overlayProps}
        >
          <div
            className="bg-raised border border-cyan-400/50 rounded-2xl max-w-xl w-full p-6 space-y-4 text-xs max-h-[90vh] overflow-y-auto"
            {...dialog.panelProps}
          >
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
                <span className="text-micro text-cyan-300 bg-cyan-500/10 px-2 py-0.5 rounded border border-cyan-500/20">
                  {selectedPlay.technical_indicators?.ma_alignment || 'BULLISH_STACK'}
                </span>
              </div>
              <button
                onClick={() => setSelectedPlay(null)}
                className="text-ink-dim hover:text-white font-bold text-xs bg-overlay hover:bg-slate-700 px-2.5 py-1 rounded-lg cursor-pointer"
              >
                CLOSE
              </button>
            </div>

            {/* Position Size Calculator Inputs */}
            <div className="p-3 bg-page rounded-xl border border-line space-y-2">
              <div className="flex items-center justify-between gap-3 text-micro">
                <span className="flex min-w-0 flex-col">
                  <span className="text-ink-dim">Capital to size against</span>
                  <span className="text-ink-mute">
                    {capitalIsFromBook ? 'from the trading book' : 'entered by you'}
                  </span>
                </span>
                <div className="flex items-center gap-1 rounded border border-line-strong bg-raised px-2 py-1">
                  <span className="text-ink-mute">$</span>
                  <label className="sr-only" htmlFor="sizing-capital">
                    Capital to size against
                  </label>
                  <input
                    id="sizing-capital"
                    type="number"
                    value={portfolioCapital}
                    onChange={(e) => setCapitalOverride(Math.max(1000, Number(e.target.value)))}
                    className="w-24 bg-transparent text-right font-semibold text-ink outline-none"
                  />
                  {!capitalIsFromBook && (
                    <button
                      onClick={() => setCapitalOverride(null)}
                      title="Size against the trading book again"
                      className="cursor-pointer text-accent hover:text-ink"
                    >
                      reset
                    </button>
                  )}
                </div>
              </div>

              {/* Calculated Sizing Outputs */}
              {modalCalculations && (
                <div className="grid grid-cols-4 gap-2 text-micro pt-1 border-t border-line">
                  <div>
                    <span className="text-ink-mute block">POSITION SIZE</span>
                    <span className="text-cyan-400 font-bold">
                      {formatCurrency(Number(modalCalculations.posSizeUsd), { compact: false })}
                    </span>
                  </div>
                  <div>
                    <span className="text-ink-mute block">UNITS / SHARES</span>
                    <span className="text-ink font-bold">{modalCalculations.shares}</span>
                  </div>
                  <div>
                    <span className="text-ink-mute block">MAX RISK</span>
                    <span className="text-rose-400 font-bold">
                      {formatCurrency(-Number(modalCalculations.maxRiskUsd), { compact: false })}
                    </span>
                  </div>
                  <div>
                    <span className="text-ink-mute block">TARGET PROFIT</span>
                    <span className="text-emerald-400 font-bold">
                      {formatCurrency(Number(modalCalculations.maxProfitUsd), {
                        compact: false,
                        signed: true,
                      })}
                    </span>
                  </div>
                </div>
              )}
            </div>

            {/* Moving Average Distance Breakdown Grid */}
            <div className="p-3 bg-page border border-cyan-500/30 rounded-xl space-y-2 text-micro">
              <div className="flex items-center justify-between">
                <span className="text-cyan-400 font-bold uppercase">
                  Distance from moving averages
                </span>
                <span className="px-2 py-0.5 rounded text-micro font-bold bg-cyan-500/20 text-cyan-300">
                  REGIME: {selectedPlay.technical_indicators?.ma_alignment || 'BULLISH_STACK'}
                </span>
              </div>
              <div className="grid grid-cols-3 gap-2">
                <div className="bg-raised p-2 rounded border border-line">
                  <span className="text-ink-mute block">SMA 20 DISTANCE</span>
                  <span
                    className={`font-bold ${(selectedPlay.technical_indicators?.dist_sma_20_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                  >
                    {selectedPlay.technical_indicators?.dist_sma_20_pct !== undefined
                      ? `${selectedPlay.technical_indicators.dist_sma_20_pct > 0 ? '+' : ''}${selectedPlay.technical_indicators.dist_sma_20_pct}%`
                      : '+3.4%'}
                  </span>
                </div>
                <div className="bg-raised p-2 rounded border border-line">
                  <span className="text-ink-mute block">SMA 50 DISTANCE</span>
                  <span
                    className={`font-bold ${(selectedPlay.technical_indicators?.dist_sma_50_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                  >
                    {selectedPlay.technical_indicators?.dist_sma_50_pct !== undefined
                      ? `${selectedPlay.technical_indicators.dist_sma_50_pct > 0 ? '+' : ''}${selectedPlay.technical_indicators.dist_sma_50_pct}%`
                      : '+8.2%'}
                  </span>
                </div>
                <div className="bg-raised p-2 rounded border border-line">
                  <span className="text-ink-mute block">SMA 200 DISTANCE</span>
                  <span
                    className={`font-bold ${(selectedPlay.technical_indicators?.dist_sma_200_pct || 0) >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                  >
                    {selectedPlay.technical_indicators?.dist_sma_200_pct !== undefined
                      ? `${selectedPlay.technical_indicators.dist_sma_200_pct > 0 ? '+' : ''}${selectedPlay.technical_indicators.dist_sma_200_pct}%`
                      : '+18.5%'}
                  </span>
                </div>
              </div>
            </div>

            {/* GARCH Volatility Cone Tranche Exits */}
            {selectedPlay.volatility_cone && (
              <div className="p-3 bg-page rounded-xl border border-amber-500/30 space-y-1.5 text-micro">
                <div className="flex items-center justify-between text-amber-400 font-bold uppercase">
                  <span>GARCH(1,1) VOLATILITY CONE TRANCHE EXITS</span>
                  <span>
                    COND VOL:{' '}
                    {formatPercent(selectedPlay.volatility_cone.cond_volatility_pct, {
                      decimals: 2,
                    })}
                  </span>
                </div>
                <div className="grid grid-cols-3 gap-2">
                  <div className="bg-raised p-2 rounded border border-line">
                    <span className="text-ink-dim block">TP1 (1.0&sigma; / 33%)</span>
                    <span className="text-emerald-400 font-bold">
                      $
                      {selectedPlay.volatility_cone.tp1_sigma_1_0 ||
                        (selectedPlay.entry_level * 1.05).toFixed(2)}
                    </span>
                  </div>
                  <div className="bg-raised p-2 rounded border border-line">
                    <span className="text-ink-dim block">TP2 (2.0&sigma; / 33%)</span>
                    <span className="text-emerald-300 font-bold">
                      $
                      {selectedPlay.volatility_cone.tp2_sigma_2_0 ||
                        (selectedPlay.entry_level * 1.1).toFixed(2)}
                    </span>
                  </div>
                  <div className="bg-raised p-2 rounded border border-line">
                    <span className="text-ink-dim block">TP3 (3.0&sigma; / 34%)</span>
                    <span className="text-cyan-300 font-bold">
                      $
                      {selectedPlay.volatility_cone.tp3_sigma_3_0 ||
                        (selectedPlay.entry_level * 1.15).toFixed(2)}
                    </span>
                  </div>
                </div>
              </div>
            )}

            {/* Trade Levels & Risk Metrics */}
            <div className="grid grid-cols-2 gap-2 bg-page p-3 rounded-xl border border-line text-micro">
              <div>
                <span className="text-ink-dim">ENTRY PRICE:</span>{' '}
                <span className="text-white font-bold">${selectedPlay.entry_level}</span>
              </div>
              <div>
                <span className="text-ink-dim">TARGET PRICE:</span>{' '}
                <span className="text-emerald-400 font-bold">${selectedPlay.target_price}</span>
              </div>
              <div>
                <span className="text-ink-dim">STOP LOSS:</span>{' '}
                <span className="text-rose-400 font-bold">${selectedPlay.stop_loss}</span>
              </div>
              <div>
                <span className="text-ink-dim">STOP MULTIPLIER:</span>{' '}
                <span className="text-amber-400 font-bold">
                  {formatNumber(selectedPlay.microstructure_stop_multiplier, { decimals: 2 })}x ATR
                </span>
              </div>
            </div>

            {/* Rationale */}
            <div className="space-y-1">
              <span className="text-ink-dim block text-micro">QUANTITATIVE MODEL RATIONALE:</span>
              <p className="text-ink font-sans text-micro leading-relaxed bg-page p-3 rounded-xl border border-line">
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
                  <span>
                    <IconSignal className="inline-block shrink-0" />
                  </span>
                  <span>CONFIRM ORDER DISPATCH</span>
                </button>
                <button
                  onClick={() => setSelectedPlay(null)}
                  className="py-3 px-4 rounded-xl bg-raised text-ink-dim border border-line-strong text-xs font-bold hover:bg-overlay cursor-pointer"
                >
                  CANCEL
                </button>
              </div>

              <button
                onClick={() => setExplainingSignal(selectedPlay.ticker)}
                className="w-full py-2 rounded-xl bg-cyan-950/40 border border-cyan-500/40 text-cyan-300 text-xs font-bold hover:bg-cyan-900/50 transition-colors cursor-pointer flex items-center justify-center gap-2"
              >
                <span>
                  <IconTarget className="inline-block shrink-0" />
                </span>
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
