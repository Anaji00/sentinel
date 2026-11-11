'use client';

/**
 * The headline for Calls, Markets and Intel. Each reads an endpoint the page already uses (so the browser shares
 * the request) and says so plainly when it cannot be read, never a blank gap that looks like zero.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { failedWithout } from '../lib/dataHealth';
import { callsHeadline, intelHeadline, marketHeadline, type HeadlineView } from '../lib/headlines';
import { POLL } from './ui/DataProvider';
import { Headline } from './ui/Headline';
import { PanelError } from './ui/PanelError';
import { LoadingLines } from './ui/Skeleton';

function Shell({ view, error, data, what }: { view: HeadlineView | null; error: unknown; data: unknown; what: string }) {
  if (failedWithout(error, data)) return <PanelError what={what} inline />;
  if (!data) return <LoadingLines lines={1} label={`Loading ${what}`} />;
  return view ? <Headline view={view} /> : null;
}

export function CallsHeadline() {
  const { data, error } = useSWR<{ board: { net: number; confidence?: string | null }[] }>('/signals/matrix', fetcher, { refreshInterval: POLL.standard });
  return <Shell view={callsHeadline(data?.board)} error={error} data={data} what="today's count" />;
}

export function MarketsHeadline() {
  const { data, error } = useSWR<{ pulse?: { spy_change_pct?: number | null; regime?: string | null } }>('/predictions/recommendations', fetcher, {
    refreshInterval: POLL.standard,
  });
  return <Shell view={marketHeadline(data?.pulse)} error={error} data={data} what="the market today" />;
}

export function IntelHeadline() {
  const zones = useSWR<{ zones?: unknown[] }>('/conflict-zones', fetcher, { refreshInterval: POLL.standard });
  const ships = useSWR<{ count?: number; dark?: number }>('/vessels/live', fetcher, { refreshInterval: POLL.standard });
  const view = intelHeadline(zones.data?.zones?.length, ships.data?.count, ships.data?.dark);
  return <Shell view={view} error={zones.error} data={zones.data} what="the conflict zone count" />;
}
