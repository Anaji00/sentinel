'use client';

/**
 * The signed-in person's own settings (services/api_gateway/routes/me.py):
 * the stocks they follow, their town, and what home leads with. Saved on the
 * account, so they follow the person across browsers.
 */

import useSWR from 'swr';
import { apiClient, fetcher } from './api';
import type { NotifyPrefs } from './notifications';

export interface MyTown {
  agency: string;
  state: string;
}

export type Focus = 'stocks' | 'civic' | 'both';

export interface Preferences {
  saved: boolean;
  tickers: string[];
  town: MyTown | null;
  focus: Focus;
  welcomed: boolean;
  notify?: NotifyPrefs;
}

export const PREFERENCES_KEY = '/me/preferences';

/** A followed-stocks list with one symbol added or removed, order kept. */
export function toggleTicker(list: string[], ticker: string): string[] {
  const t = ticker.trim().toUpperCase();
  return list.includes(t) ? list.filter((x) => x !== t) : [...list, t];
}

export function usePreferences() {
  const { data, error, mutate } = useSWR<Preferences>(PREFERENCES_KEY, fetcher, {
    revalidateOnFocus: false,
  });
  const save = async (patch: Partial<Omit<Preferences, 'saved'>>): Promise<void> => {
    // Optimistic: the page reflects the choice at once, and the server's
    // validated answer replaces it.
    if (data) await mutate({ ...data, ...patch }, { revalidate: false });
    const res = await apiClient.patch<Preferences>(PREFERENCES_KEY, patch);
    await mutate(res.data, { revalidate: false });
  };
  return { prefs: data, error, save };
}
