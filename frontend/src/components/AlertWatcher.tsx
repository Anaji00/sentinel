'use client';

/**
 * Checks for fired price alerts about once a minute while the dashboard is open. Each new one shows inside the page and,
 * if the person allowed it, as a browser notification; it is then acknowledged so it does not show again. Renders nothing.
 */

import React from 'react';
import useSWR, { mutate as globalMutate } from 'swr';
import { apiClient, fetcher } from '../lib/api';
import { alertLine, newlyTriggered, type PriceAlert } from '../lib/alerts';
import { formatCurrency } from '../lib/format';
import { POLL } from './ui/DataProvider';
import { useFeedback } from './ui/Feedback';

const money = (v: number) => formatCurrency(v, { decimals: 2 });

export default function AlertWatcher() {
  const { toast } = useFeedback();
  const { data } = useSWR<{ triggered: PriceAlert[] }>('/alerts/triggered', fetcher, {
    refreshInterval: POLL.slow, // once a minute: each read evaluates the alerts on the server
    shouldRetryOnError: false,
    revalidateOnFocus: false,
  });
  // Ids already announced in this page, so a slow acknowledgement cannot show one twice.
  const announced = React.useRef<Set<number>>(new Set());

  React.useEffect(() => {
    for (const a of newlyTriggered(data?.triggered?.map((x) => ({ ...x, status: 'triggered' as const })), announced.current)) {
      announced.current.add(a.id);
      const line = alertLine(a, money);
      toast('info', 'Price alert', line);
      if (typeof Notification !== 'undefined' && Notification.permission === 'granted') {
        try {
          new Notification('Price alert', { body: line, tag: `alert-${a.id}` });
        } catch {
          // A browser that refuses to show it still gets the in-page message above.
        }
      }
      void apiClient
        .post(`/alerts/${a.id}/ack`)
        .then(() => globalMutate('/alerts'))
        .catch(() => announced.current.delete(a.id));
    }
  }, [data, toast]);

  return null;
}
