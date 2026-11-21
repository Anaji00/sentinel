'use client';

/**
 * Price alerts for one stock: set "tell me when it rises above or falls below this price", see the ones set and the ones
 * that have fired, remove them, and opt in to browser notifications. The server checks alerts from stored minute bars
 * each time they are read, so they fire only while Sentinel is open; the panel says so rather than implying a push.
 */

import React from 'react';
import useSWR, { mutate as globalMutate } from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { alertLine, alreadyPassed, MAX_NOTE, parseLevel, type PriceAlert } from '../lib/alerts';
import { formatCurrency } from '../lib/format';
import { Button } from './ui/Button';
import { Disclosure } from './ui/Disclosure';
import { Input } from './ui/Input';
import { useFeedback } from './ui/Feedback';
import { POLL } from './ui/DataProvider';

const money = (v: number) => formatCurrency(v, { decimals: 2 });

export default function PriceAlerts({ ticker, price }: { ticker: string; price: number | null }) {
  const { toast } = useFeedback();
  const { data } = useSWR<{ alerts: PriceAlert[] }>('/alerts', fetcher, { refreshInterval: POLL.slow, shouldRetryOnError: false });
  const [direction, setDirection] = React.useState<'above' | 'below'>('above');
  const [level, setLevel] = React.useState('');
  const [note, setNote] = React.useState('');
  const [busy, setBusy] = React.useState(false);
  const [permission, setPermission] = React.useState<string>('');
  React.useEffect(() => {
    setPermission(typeof Notification === 'undefined' ? 'unsupported' : Notification.permission);
  }, []);

  const mine = (data?.alerts ?? []).filter((a) => a.ticker === ticker);
  const parsed = parseLevel(level);
  const warning = parsed != null ? alreadyPassed(direction, parsed, price) : null;

  const create = async () => {
    if (parsed == null || busy) return;
    setBusy(true);
    try {
      await apiClient.post('/alerts', { ticker, direction, level: parsed, ...(note.trim() ? { note: note.trim().slice(0, MAX_NOTE) } : {}) });
      toast('success', 'Alert set', `${ticker} ${direction === 'above' ? 'rises above' : 'falls below'} ${money(parsed)}.`);
      setLevel('');
      setNote('');
      await globalMutate('/alerts');
    } catch (err) {
      toast('error', 'The alert was not set.', describeApiError(err) ?? undefined);
    }
    setBusy(false);
  };

  const remove = async (id: number) => {
    try {
      await apiClient.delete(`/alerts/${id}`);
      await globalMutate('/alerts');
    } catch (err) {
      toast('error', 'The alert was not removed.', describeApiError(err) ?? undefined);
    }
  };

  const allow = async () => {
    if (typeof Notification === 'undefined') return;
    setPermission(await Notification.requestPermission());
  };

  return (
    <section aria-label={`${ticker} price alerts`} className="rounded-md border border-line bg-raised p-3">
      <Disclosure variant="row" label="Price alerts" storageKey={`alerts:${ticker}`} hint={mine.length > 0 ? `${mine.length} set for ${ticker}` : 'Tell me when it reaches a price'}>
        <div className="space-y-3 pt-1">
          <div className="flex flex-wrap items-end gap-2">
            <div role="group" aria-label="Direction" className="flex gap-1.5">
              <Button variant="outline" size="sm" pressed={direction === 'above'} onClick={() => setDirection('above')}>
                Rises above
              </Button>
              <Button variant="outline" size="sm" pressed={direction === 'below'} onClick={() => setDirection('below')}>
                Falls below
              </Button>
            </div>
            <Input label="Price" size="sm" value={level} onChange={(e) => setLevel(e.target.value)} inputMode="decimal" placeholder={price != null ? price.toFixed(2) : '200.00'} containerClassName="w-28" />
            <Input label="Note (optional)" size="sm" value={note} onChange={(e) => setNote(e.target.value)} maxLength={MAX_NOTE} containerClassName="min-w-[10rem] flex-1" />
            <Button variant="primary" size="sm" onClick={() => void create()} disabled={parsed == null} loading={busy}>
              Set alert
            </Button>
          </div>
          {warning && <p className="text-micro text-caution">{warning}</p>}
          {mine.length > 0 && (
            <ul className="space-y-1.5">
              {mine.map((a) => (
                <li key={a.id} className="flex items-center justify-between gap-2 text-xs">
                  <span className={a.status === 'triggered' ? 'text-ink' : 'text-ink-dim'}>
                    {alertLine(a, money)}
                    {a.note ? <span className="text-ink-mute"> {a.note}</span> : null}
                  </span>
                  <Button variant="ghost" size="sm" onClick={() => void remove(a.id)} aria-label={`Remove the ${ticker} alert at ${money(a.level)}`}>
                    Remove
                  </Button>
                </li>
              ))}
            </ul>
          )}
          <p className="text-micro text-ink-mute">
            Alerts are checked while Sentinel is open in a browser; nothing is sent when it is closed. They work for stocks and funds that have minute prices, up to 50 at a time.
          </p>
          {permission === 'default' && (
            <Button variant="outline" size="sm" onClick={() => void allow()}>
              Allow browser notifications
            </Button>
          )}
          {permission === 'denied' && <p className="text-micro text-ink-mute">Browser notifications are blocked for this site. Alerts still appear inside Sentinel.</p>}
        </div>
      </Disclosure>
    </section>
  );
}
