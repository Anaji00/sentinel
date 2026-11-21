'use client';

/**
 * How past trade ideas on this one stock have done: the count, the share that went the right way and the dates they cover,
 * or a plain "no past ideas". One request for the page. The record is the model's own paper ideas with their stops, not
 * a promise about this stock, and the line says how few cases it usually rests on.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { barLabel } from '../lib/candles';
import { type TrackSlice } from '../lib/liveFigures';
import { stockRecordLine } from '../lib/stockRecord';
import { POLL } from './ui/DataProvider';

export default function StockRecord({ ticker }: { ticker: string }) {
  const { data } = useSWR<TrackSlice>(`/predictions/track-record/slice?horizon=monthly&ticker=${encodeURIComponent(ticker)}`, fetcher, {
    refreshInterval: POLL.rare,
    revalidateOnFocus: false,
    shouldRetryOnError: false,
  });
  const line = stockRecordLine(ticker, data, (iso) => barLabel(`${iso.slice(0, 10)}T00:00:00Z`, '1d'));
  if (!line) return null;
  return (
    <p className="text-xs text-ink-dim" title={line.detail}>
      {line.headline}
      <span className="text-ink-mute"> · paper trades with stops, after costs</span>
    </p>
  );
}
