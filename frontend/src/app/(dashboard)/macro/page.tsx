import React from 'react';
import { GraphExplorerIsland, FinancialAdvisorAdviceIsland, BondYieldsChartIsland, PredictionMarketPanelIsland, CyberIntelligencePanelIsland } from './islands';

export default function MacroPage() {
  return (
    <div className="flex h-full w-full flex-col p-4 gap-4 overflow-y-auto font-mono bg-[#06080d] text-slate-100">
      
      {/* Header & Macro Regime Telemetry Bar */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl font-black text-white uppercase tracking-widest flex items-center gap-2">
              MACROECONOMIC & YIELD CURVE EXPOSURE MATRIX
            </h1>
            <p className="text-xs text-slate-400">
              CROSS-DOMAIN MONETARY POLICY, COINTEGRATION & RISK EXCITATION SENSITIVITY
            </p>
          </div>
          <span className="px-3 py-1 bg-rose-500/20 text-rose-400 border border-rose-500/40 rounded-lg text-xs font-bold animate-pulse">
            2Y/10Y YIELD INVERSION ACTIVE
          </span>
        </div>

        {/* Macro KPI Cards */}
        <div className="grid grid-cols-4 gap-3 text-xs">
          <div className="p-3 bg-[#090d16] border border-rose-500/30 rounded-xl">
            <span className="text-[10px] text-slate-400 uppercase font-bold">10Y - 2Y Spread</span>
            <div className="text-base font-extrabold text-rose-400 mt-0.5">-15.2 bps (Inverted)</div>
          </div>
          <div className="p-3 bg-[#090d16] border border-cyan-500/30 rounded-xl">
            <span className="text-[10px] text-slate-400 uppercase font-bold">Fed Funds Target Rate</span>
            <div className="text-base font-extrabold text-[#00f2fe] mt-0.5">5.25% - 5.50%</div>
          </div>
          <div className="p-3 bg-[#090d16] border border-purple-500/30 rounded-xl">
            <span className="text-[10px] text-slate-400 uppercase font-bold">PolyMarket Sept Cut Odds</span>
            <div className="text-base font-extrabold text-purple-400 mt-0.5">68% Probability</div>
          </div>
          <div className="p-3 bg-[#090d16] border border-emerald-500/30 rounded-xl">
            <span className="text-[10px] text-slate-400 uppercase font-bold">Decoupling Clusters</span>
            <div className="text-base font-extrabold text-emerald-400 mt-0.5">3 Active Signals</div>
          </div>
        </div>
      </div>

      {/* Bond Yields Intraday Chart */}
      <div className="w-full">
        <BondYieldsChartIsland />
      </div>

      {/* Prediction Markets & Cyber Threat Grid */}
      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-6 min-h-[450px]">
          <PredictionMarketPanelIsland />
        </div>
        <div className="col-span-6 min-h-[450px]">
          <CyberIntelligencePanelIsland />
        </div>
      </div>

      {/* Graph & Advisor Grid */}
      <div className="grid grid-cols-12 gap-4 min-h-[450px]">
        <div className="col-span-8 flex flex-col bg-[#090d16] rounded-2xl border border-slate-800 overflow-hidden relative">
          <GraphExplorerIsland />
        </div>
        <div className="col-span-4 flex flex-col bg-[#090d16] rounded-2xl border border-slate-800 overflow-hidden relative">
          <FinancialAdvisorAdviceIsland />
        </div>
      </div>

    </div>
  );
}
