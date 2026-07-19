'use client';

import React from "react";
import useSWR from "swr";
import { fetcher } from "../lib/api";

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
    action: "BUY" | "SELL" | "HOLD";
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
        "/financial/advice",
        fetcher,
        { refreshInterval: 10000 }
    );

    if (error) return <div className="text-red-500 text-xs font-mono p-4">Error loading financial advice.</div>;
    if (isLoading) return <div className="text-gray-500 text-xs font-mono animate-pulse p-4">Loading portfolio allocations...</div>;

    const brief = data?.brief;
    const plays = brief?.highest_conviction_plays || [];

    return (
        <div className="h-full flex flex-col bg-[#0b0c10] border-t border-[#1f2833] text-gray-200 overflow-hidden font-sans">
            {/* Header */}
            <div className="p-4 border-b border-[#1f2833] flex justify-between items-center bg-[#0b0c10]">
                <div className="flex items-center gap-2">
                    <span className="h-2.5 w-2.5 rounded-full bg-emerald-500 animate-pulse" />
                    <h2 className="text-sm font-bold tracking-widest text-[#66fcf1] uppercase font-mono">
                        QUANT PORTFOLIO ALLOCATOR
                    </h2>
                </div>
                {brief?.market_regime && (
                    <span className="text-[10px] font-mono font-bold bg-[#66fcf1]/10 text-[#66fcf1] border border-[#66fcf1]/30 px-2 py-0.5 rounded">
                        REGIME: {brief.market_regime}
                    </span>
                )}
            </div>

            {/* Content Body */}
            <div className="flex-1 overflow-y-auto p-4 space-y-5 scrollbar-thin scrollbar-thumb-gray-800">
                {plays.length === 0 ? (
                    <div className="text-gray-500 text-xs font-mono text-center py-8">
                        {data?.message || "No high-conviction plays evaluated yet."}
                    </div>
                ) : (
                    plays.map((play, idx) => {
                        const isBuy = play.action === "BUY";
                        const isSell = play.action === "SELL";
                        const indicators = play.technical_indicators || {};
                        const fibs = play.fib_levels || {};

                        return (
                            <div key={idx} className="bg-[#151b26]/40 rounded border border-[#1f2833] overflow-hidden">
                                {/* Play header */}
                                <div className="bg-[#151b26]/80 px-3 py-2 border-b border-[#1f2833] flex justify-between items-center">
                                    <div className="flex items-center gap-2">
                                        <span className="text-sm font-extrabold text-white font-mono">{play.ticker}</span>
                                        <span className={`text-[10px] font-mono font-bold px-1.5 py-0.5 rounded ${
                                            isBuy 
                                                ? "bg-emerald-950/50 text-emerald-400 border border-emerald-800" 
                                                : isSell 
                                                    ? "bg-red-950/50 text-red-400 border border-red-800" 
                                                    : "bg-gray-800 text-gray-300"
                                        }`}>
                                            {play.action}
                                        </span>
                                    </div>
                                    <div className="text-right">
                                        <span className="text-[10px] font-mono text-gray-500">Conviction:</span>
                                        <span className="text-[10px] font-mono font-bold text-white ml-1">{(play.conviction_score * 100).toFixed(0)}%</span>
                                    </div>
                                </div>

                                {/* Capital Allocation & Risk/Reward */}
                                <div className="p-3 grid grid-cols-2 gap-3 border-b border-[#1f2833]/50">
                                    <div className="bg-black/30 p-2 rounded border border-[#1f2833]/30 flex flex-col justify-center">
                                        <span className="text-[9px] font-mono text-gray-500 uppercase">Kelly Allocation</span>
                                        <span className="text-lg font-bold text-[#66fcf1] font-mono mt-0.5">
                                            {play.kelly_allocation_pct.toFixed(1)}%
                                        </span>
                                    </div>
                                    <div className="bg-black/30 p-2 rounded border border-[#1f2833]/30 flex flex-col justify-center">
                                        <span className="text-[9px] font-mono text-gray-500 uppercase">Risk/Reward Ratio</span>
                                        <span className="text-lg font-bold text-amber-400 font-mono mt-0.5">
                                            1 : {play.risk_reward_ratio.toFixed(2)}
                                        </span>
                                    </div>
                                </div>

                                {/* Order Levels */}
                                <div className="p-3 grid grid-cols-3 gap-2 text-center text-[10px] font-mono border-b border-[#1f2833]/50 bg-black/10">
                                    <div>
                                        <div className="text-gray-500 mb-0.5">Entry</div>
                                        <div className="text-white font-bold">${play.entry_level.toFixed(2)}</div>
                                    </div>
                                    <div>
                                        <div className="text-gray-500 mb-0.5">Stop Loss</div>
                                        <div className="text-red-400 font-bold">${play.stop_loss.toFixed(2)}</div>
                                    </div>
                                    <div>
                                        <div className="text-gray-500 mb-0.5">Target</div>
                                        <div className="text-emerald-400 font-bold">${play.target_price.toFixed(2)}</div>
                                    </div>
                                </div>

                                {/* Technical Analysis Indicators */}
                                <div className="p-3 text-[10px] font-mono space-y-2 border-b border-[#1f2833]/50">
                                    <div className="text-gray-500 uppercase text-[9px] font-bold tracking-wider">TA Indicators</div>
                                    <div className="grid grid-cols-3 gap-2">
                                        <div className="bg-black/20 p-1.5 rounded border border-[#1f2833]/30">
                                            <div className="text-[9px] text-gray-500">RSI (14)</div>
                                            <div className={`font-bold mt-0.5 ${
                                                (indicators.rsi || 50) >= 70 
                                                    ? "text-red-400" 
                                                    : (indicators.rsi || 50) <= 30 
                                                        ? "text-emerald-400" 
                                                        : "text-white"
                                            }`}>
                                                {(indicators.rsi || 50).toFixed(1)}
                                            </div>
                                        </div>
                                        <div className="bg-black/20 p-1.5 rounded border border-[#1f2833]/30">
                                            <div className="text-[9px] text-gray-500">ATR</div>
                                            <div className="font-bold text-white mt-0.5">
                                                ${(indicators.atr || 0).toFixed(2)}
                                            </div>
                                        </div>
                                        <div className="bg-black/20 p-1.5 rounded border border-[#1f2833]/30">
                                            <div className="text-[9px] text-gray-500">EMA Cross</div>
                                            <div className={`font-bold mt-0.5 ${
                                                (indicators.ema_12 || 0) >= (indicators.ema_26 || 0) 
                                                    ? "text-emerald-400" 
                                                    : "text-red-400"
                                            }`}>
                                                {(indicators.ema_12 || 0) >= (indicators.ema_26 || 0) ? "BULL" : "BEAR"}
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                {/* Fibonacci retracements */}
                                {Object.keys(fibs).length > 0 && (
                                    <div className="p-3 text-[10px] font-mono space-y-1.5 bg-black/10 border-b border-[#1f2833]/50">
                                        <div className="text-gray-500 uppercase text-[9px] font-bold tracking-wider">Fib Retracement Levels</div>
                                        <div className="grid grid-cols-4 gap-1 text-[9px] text-gray-400">
                                            {["0.236", "0.382", "0.500", "0.618"].map((lvl) => (
                                                <div key={lvl} className="bg-black/40 p-1 rounded border border-[#1f2833]/20 text-center">
                                                    <div>{lvl}</div>
                                                    <div className="text-white mt-0.5">${fibs[lvl]?.toFixed(2)}</div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Rationale */}
                                <div className="p-3 text-xs text-gray-300 leading-relaxed bg-[#151b26]/20">
                                    <div className="text-gray-500 font-mono text-[9px] uppercase font-bold tracking-wider mb-1">Jane Street Quant Rationale</div>
                                    {play.quantitative_rationale}
                                </div>
                            </div>
                        );
                    })
                )}

                {/* Hedging strategy */}
                {brief?.general_hedging_strategy && (
                    <div className="bg-amber-950/20 border border-amber-900/50 p-4 rounded-lg">
                        <h4 className="text-xs font-bold text-amber-400 uppercase tracking-wider mb-2 font-mono flex items-center gap-1.5">
                            <span>🛡️</span> SYSTEMIC HEDGING MATRIX
                        </h4>
                        <p className="text-xs text-gray-300 leading-relaxed">
                            {brief.general_hedging_strategy}
                        </p>
                    </div>
                )}
            </div>
        </div>
    );
}
