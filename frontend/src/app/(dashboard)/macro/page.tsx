import React from 'react';
import {
  FinancialAdvisorAdviceIsland,
  BondYieldsChartIsland,
  PredictionMarketPanelIsland,
  CyberIntelligencePanelIsland,
} from './islands';

export default function MacroPage() {
  return (
    <div className="flex h-full w-full flex-col p-4 gap-4 overflow-y-auto bg-inset text-ink">
      {/* Header & Macro Regime Telemetry Bar */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-lg font-semibold text-white flex items-center gap-2">
              Macro &amp; yield curve
            </h1>
            <p className="text-xs text-ink-dim">
              Policy rates, cointegration and how shocks propagate
            </p>
          </div>
          <span className="px-3 py-1 bg-rose-500/20 text-rose-400 border border-rose-500/40 rounded-lg text-xs font-bold animate-pulse">
            2Y/10Y curve inverted
          </span>
        </div>

        {/* Macro KPI Cards */}
        <div className="grid grid-cols-4 gap-3 text-xs">
          <div className="p-3 bg-raised border border-rose-500/30 rounded-xl">
            <span className="text-micro text-ink-dim uppercase font-bold">10Y - 2Y Spread</span>
            <div className="text-base font-extrabold text-rose-400 mt-0.5">
              -15.2 bps (Inverted)
            </div>
          </div>
          <div className="p-3 bg-raised border border-cyan-500/30 rounded-xl">
            <span className="text-micro text-ink-dim uppercase font-bold">
              Fed Funds Target Rate
            </span>
            <div className="text-base font-extrabold text-accent mt-0.5">5.25% - 5.50%</div>
          </div>
          <div className="p-3 bg-raised border border-purple-500/30 rounded-xl">
            <span className="stat-label">PolyMarket Sept Cut Odds</span>
            <div className="text-base font-extrabold text-purple-400 mt-0.5">68% Probability</div>
          </div>
          <div className="p-3 bg-raised border border-emerald-500/30 rounded-xl">
            <span className="text-micro text-ink-dim uppercase font-bold">Decoupling Clusters</span>
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

      {/* Financial Advisor */}
      <div className="grid grid-cols-12 gap-4 min-h-[450px]">
        <div className="col-span-12 flex flex-col bg-raised rounded-2xl border border-line overflow-hidden relative">
          <FinancialAdvisorAdviceIsland />
        </div>
      </div>
    </div>
  );
}
