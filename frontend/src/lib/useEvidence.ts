'use client';

/**
 * One request for everything about a stock that the page shows beside its chart. The hero, the key facts and the
 * held-back panel all read it; SWR shares the single request between them.
 */

import useSWR from 'swr';
import { fetcher } from './api';
import { POLL } from '../components/ui/DataProvider';
import { evidenceKey, type Evidence } from './evidence';

export function useEvidence(ticker: string | null | undefined) {
  return useSWR<Evidence>(ticker ? evidenceKey(ticker) : null, fetcher, { refreshInterval: POLL.standard, revalidateOnFocus: false });
}
