'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

/** Client islands for the macro view. */


const FinancialAdvisorAdvice = dynamic(() => import('@/components/FinancialAdvisorAdvice'), {
  loading: () => <PanelSkeleton title="Loading Macro Risk Advisor..." />,
  ssr: false,
});

const BondYieldsChart = dynamic(() => import('@/components/charts/BondYieldsChart'), {
  loading: () => <PanelSkeleton title="Loading Treasury Bond Yields..." />,
  ssr: false,
});

const PredictionMarketPanel = dynamic(() => import('@/components/PredictionMarketPanel'), {
  loading: () => <PanelSkeleton title="Loading Macro Prediction Markets..." />,
  ssr: false,
});

const CyberIntelligencePanel = dynamic(() => import('@/components/CyberIntelligencePanel'), {
  loading: () => <PanelSkeleton title="Loading Cyber Threat Panel..." />,
  ssr: false,
});

export function FinancialAdvisorAdviceIsland() {
  return <FinancialAdvisorAdvice />;
}

export function BondYieldsChartIsland() {
  return <BondYieldsChart />;
}

export function PredictionMarketPanelIsland() {
  return <PredictionMarketPanel />;
}

export function CyberIntelligencePanelIsland() {
  return <CyberIntelligencePanel />;
}
