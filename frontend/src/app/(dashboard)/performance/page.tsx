import React from 'react';
import AutoTradePanel from '@/components/AutoTradePanel';
import ModelPortfolio from '@/components/ModelPortfolio';
import { ModelHealth } from '@/components/ModelHealth';
import PaperTradingBook from '@/components/PaperTradingBook';
import PortfolioLab from '@/components/PortfolioLab';
import SignalAttribution from '@/components/SignalAttribution';
import StrategyPerformance from '@/components/StrategyPerformance';
import { PaperLedger, ProducerGrades, TrackRecord } from '@/components/TrackRecord';
import { PageHeader } from '@/components/ui/PageHeader';

/**
 * Performance: what were Portfolio & track record and Research. Both answer
 * "is this working", so they read as one ledger, top to bottom: today, this
 * month, over years. The track record lives here only (the Signal board used
 * to carry a copy). The old addresses redirect here.
 */

function Band({ id, title, sub, children }: { id: string; title: string; sub: string; children: React.ReactNode }) {
  return (
    <section
      id={id}
      aria-labelledby={`${id}-h`}
      className="grid grid-cols-1 gap-4 border-t border-line pt-6 lg:grid-cols-[180px_minmax(0,1fr)]"
    >
      <div className="space-y-1 lg:sticky lg:top-0 lg:self-start">
        <h2 id={`${id}-h`} className="text-head font-semibold text-ink">
          {title}
        </h2>
        <p className="text-xs text-ink-mute">{sub}</p>
      </div>
      <div className="min-w-0 space-y-4">{children}</div>
    </section>
  );
}

export default function PerformancePage() {
  return (
    <div className="h-full w-full space-y-6 overflow-y-auto p-2">
      <PageHeader
        title="Performance"
        description="Is it working? Read top to bottom: the paper book today, whether this month's calls were right and by whom, and how the strategies and signals have done over years. Paper trading only; no real money moves."
        related={[
          { href: '/calls', label: 'Calls' },
          { href: '/methodology', label: 'How it works' },
        ]}
      />
      <Band id="today" title="Today" sub="The model's book against the S&P 500, the auto-trader, and your own paper trades">
        <ModelPortfolio />
        <AutoTradePanel />
        <PortfolioLab />
        <ModelHealth />
        <div className="h-[80vh] min-h-[560px]">
          <PaperTradingBook />
        </div>
      </Band>
      <Band id="month" title="This month" sub="Were the calls right, by who made them">
        <TrackRecord />
        <PaperLedger />
        <ProducerGrades />
      </Band>
      <Band id="years" title="Over years" sub="Backtests after costs, and which signals drive the returns">
        <StrategyPerformance />
        <SignalAttribution />
      </Band>
    </div>
  );
}
