'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

/** Client islands for the markets page: its Prices and Economy views. */

const BondYieldsChart = dynamic(() => import('@/components/charts/BondYieldsChart'), {
  loading: () => <PanelSkeleton title="Loading Treasury Bond Yields..." />,
  ssr: false,
});

const PredictionMarketPanel = dynamic(() => import('@/components/PredictionMarketPanel'), {
  loading: () => <PanelSkeleton title="Loading Macro Prediction Markets..." />,
  ssr: false,
});

const MarketCharts = dynamic(() => import('@/components/MarketCharts'), {
  loading: () => <PanelSkeleton title="Loading market charts…" />,
  ssr: false,
});

export function BondYieldsChartIsland() {
  return <BondYieldsChart />;
}

export function PredictionMarketPanelIsland() {
  return <PredictionMarketPanel />;
}

export function MarketChartsIsland() {
  return <MarketCharts />;
}
