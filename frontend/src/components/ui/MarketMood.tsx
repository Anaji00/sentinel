'use client';

/**
 * The market's mood in the top bar: calm, stressed or panic, from the nightly gauge (/predictions/equity-regime). It is
 * the same reading the model uses to size its stock bets, so the label says what it means in a sentence on hover.
 * With no reading yet it shows nothing rather than a guess.
 */

import React from 'react';
import useSWR from 'swr';
import { fetcher } from '../../lib/api';
import { POLL } from './DataProvider';

interface Regime {
  level: 'calm' | 'stressed' | 'panic' | null;
  multiplier?: number;
}

const WORDS: Record<string, { label: string; tone: string; meaning: string }> = {
  calm: { label: 'Calm', tone: 'text-ink-dim', meaning: 'Markets are behaving normally. The portfolio runs its full-size stock bets.' },
  stressed: { label: 'Stressed', tone: 'text-caution', meaning: 'Markets are jumpier than usual. The portfolio cuts its stock bets down.' },
  panic: { label: 'Panic', tone: 'text-negative', meaning: 'Markets are in turmoil. The portfolio cuts its stock bets sharply.' },
};

export function MarketMood() {
  const { data } = useSWR<Regime>('/predictions/equity-regime', fetcher, { refreshInterval: POLL.slow });
  const w = data?.level ? WORDS[data.level] : null;
  if (!w) return null;
  return (
    <span className="hidden items-center gap-1.5 whitespace-nowrap text-micro 2xl:flex" title={`${w.meaning}${data?.multiplier != null ? ` Bets at ${Math.round(data.multiplier * 100)}% of full size.` : ''}`}>
      <span className="text-ink-mute">Market mood</span>
      <span className={w.tone}>{w.label}</span>
    </span>
  );
}

export default MarketMood;
