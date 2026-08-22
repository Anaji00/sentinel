'use client';

import React from 'react';
import dynamic from 'next/dynamic';
import { PanelSkeleton } from '@/components/ui/Skeleton';

/** Client islands for the crypto view. */
const CryptoAnalytics = dynamic(() => import('@/components/CryptoAnalytics'), {
  loading: () => <PanelSkeleton title="Loading crypto analytics…" />,
  ssr: false,
});

const CryptoPriceChart = dynamic(() => import('@/components/charts/CryptoPriceChart'), {
  loading: () => <PanelSkeleton title="Loading BTC chart…" />,
  ssr: false,
});

export function CryptoPriceChartIsland() {
  return <CryptoPriceChart />;
}

export function CryptoAnalyticsIsland() {
  return <CryptoAnalytics />;
}
