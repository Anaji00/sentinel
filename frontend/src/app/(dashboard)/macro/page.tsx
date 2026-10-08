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
        </div>
        {/* A strip of fixed numbers stood here: "-15.2 bps (Inverted)", a
            5.25-5.50% funds rate, "68%" cut odds and "3 Active Signals", all
            literals, contradicting the live curve directly beneath them. The
            chart below carries the measured yields and spread. */}
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
