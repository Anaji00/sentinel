'use client';

/**
 * The bell: what changed on the stocks this person follows.
 *
 * Alerts used to reach only the operator's Telegram, so someone could follow a
 * stock and never hear about it. The inbox is filled by
 * services/alert_manager/notifications_job.py. Browser pop-ups are opt-in, asked
 * for only after a click, and fire only when Sentinel is not the tab in view:
 * when it is, the count on the bell already says so.
 */

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Button } from './Button';
import Link from 'next/link';
import useSWR from 'swr';
import { Bell } from 'lucide-react';
import { apiClient, fetcher } from '../../lib/api';
import {
  BROWSER_ALERTS_KEY,
  DEFAULT_NOTIFY,
  KIND_LABELS,
  NOTIFICATIONS_KEY,
  badgeText,
  browserAlertsSupported,
  newUnread,
  popups,
  type InboxResponse,
  type NotifyPrefs,
} from '../../lib/notifications';
import { usePreferences } from '../../lib/preferences';
import { ClockTime } from './ClockTime';
import { POLL } from './DataProvider';

function readBrowserAlerts(): boolean {
  try {
    return window.localStorage.getItem(BROWSER_ALERTS_KEY) === '1';
  } catch {
    return false;
  }
}

function Settings() {
  const { prefs, save } = usePreferences();
  const notify: NotifyPrefs = { ...DEFAULT_NOTIFY, ...prefs?.notify };
  const [browser, setBrowser] = useState(() => browserAlertsSupported() && readBrowserAlerts());
  const [blocked, setBlocked] = useState(false);

  const toggle = (key: keyof NotifyPrefs) => void save({ notify: { ...notify, [key]: !notify[key] } });

  const toggleBrowser = async () => {
    const next = !browser;
    if (next && (await Notification.requestPermission()) !== 'granted') {
      setBlocked(true);
      return;
    }
    setBlocked(false);
    setBrowser(next);
    try {
      window.localStorage.setItem(BROWSER_ALERTS_KEY, next ? '1' : '0');
    } catch {
      /* private mode: the choice lasts for this page */
    }
  };

  return (
    <div className="space-y-2 border-t border-line bg-inset px-4 py-3 text-xs">
      <div className="font-semibold text-ink">Tell me about</div>
      {KIND_LABELS.map(({ key, label, hint }) => (
        <label key={key} className="flex cursor-pointer items-start gap-2 text-ink-dim">
          <input type="checkbox" checked={notify[key]} onChange={() => toggle(key)} className="mt-0.5 accent-[var(--accent)]" />
          <span>
            <span className="text-ink">{label}</span>
            <span className="block text-micro text-ink-mute">{hint}</span>
          </span>
        </label>
      ))}
      <div className="pt-1 font-semibold text-ink">Also send</div>
      <label className="flex cursor-pointer items-start gap-2 text-ink-dim">
        <input
          type="checkbox"
          checked={notify.email_digest}
          onChange={() => toggle('email_digest')}
          className="mt-0.5 accent-[var(--accent)]"
        />
        <span>
          <span className="text-ink">A daily email</span>
          <span className="block text-micro text-ink-mute">Each morning, only if there is something unread</span>
        </span>
      </label>
      {browserAlertsSupported() ? (
        <label className="flex cursor-pointer items-start gap-2 text-ink-dim">
          <input type="checkbox" checked={browser} onChange={() => void toggleBrowser()} className="mt-0.5 accent-[var(--accent)]" />
          <span>
            <span className="text-ink">Browser alerts on this device</span>
            <span className="block text-micro text-ink-mute">
              {blocked
                ? 'Your browser has blocked these for this site. Allow them in its site settings.'
                : 'While Sentinel is open in another tab'}
            </span>
          </span>
        </label>
      ) : (
        <p className="text-micro text-ink-mute">Browser alerts need a secure (https) page.</p>
      )}
    </div>
  );
}

export function NotificationBell() {
  const [open, setOpen] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const root = useRef<HTMLDivElement | null>(null);
  const seen = useRef<Set<number> | null>(null);
  const { data, mutate } = useSWR<InboxResponse>(NOTIFICATIONS_KEY, fetcher, { refreshInterval: POLL.slow });
  const { prefs } = usePreferences();
  const unread = data?.unread ?? 0;

  useEffect(() => {
    if (!data) return;
    const fresh = newUnread(data.items, seen.current);
    seen.current = new Set(data.items.map((n) => n.id));
    if (!fresh.length || !browserAlertsSupported() || !document.hidden) return;
    if (!readBrowserAlerts() || Notification.permission !== 'granted') return;
    for (const p of popups(fresh)) {
      const n = new Notification(p.title, { body: p.body, icon: '/favicon.ico' });
      n.onclick = () => {
        window.focus();
        window.location.assign(p.url);
      };
    }
  }, [data]);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (root.current && !root.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  const markRead = useCallback(
    async (body: { ids?: number[]; all?: boolean }) => {
      await apiClient.post('/me/notifications/read', body);
      await mutate();
    },
    [mutate],
  );

  const followsNothing = (prefs?.tickers?.length ?? 0) === 0;

  return (
    <div ref={root} className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        aria-label={unread ? `Notifications, ${unread} unread` : 'Notifications'}
        title="Notifications"
        className="relative flex h-8 w-8 items-center justify-center rounded-md text-ink-dim transition-colors hover:bg-overlay/60 hover:text-ink"
      >
        <Bell className="h-4 w-4" />
        {unread > 0 && (
          <span className="absolute -right-0.5 -top-0.5 flex h-4 min-w-4 items-center justify-center rounded-full bg-accent px-1 font-mono text-micro font-semibold leading-none text-page">
            {badgeText(unread)}
          </span>
        )}
      </button>

      {open && (
        <div
          role="region"
          aria-label="Notifications"
          className="fixed inset-x-2 top-14 z-50 flex max-h-[80vh] flex-col overflow-hidden rounded-xl border border-line-strong bg-raised shadow-overlay sm:absolute sm:inset-x-auto sm:right-0 sm:top-10 sm:w-96"
        >
          <div className="flex items-center justify-between gap-2 border-b border-line px-4 py-2.5">
            <h2 className="text-sm font-semibold text-ink">Notifications</h2>
            <div className="flex items-center gap-3 text-xs">
              {unread > 0 && (
                <Button variant="link" type="button" onClick={() => void markRead({ all: true })}>
                  Mark all read
                </Button>
              )}
              <button
                type="button"
                onClick={() => setShowSettings((v) => !v)}
                aria-expanded={showSettings}
                className="text-ink-dim hover:text-ink"
              >
                Settings
              </button>
            </div>
          </div>
          {showSettings && <Settings />}
          <div className="min-h-0 flex-1 overflow-y-auto">
            {data && data.items.length > 0 ? (
              <ul>
                {data.items.map((n) => (
                  <li key={n.id} className="border-b border-line/60 last:border-b-0">
                    <Link
                      href={n.url || '/'}
                      onClick={() => {
                        setOpen(false);
                        if (!n.read) void markRead({ ids: [n.id] });
                      }}
                      className={`block px-4 py-3 transition-colors hover:bg-overlay/50 ${n.read ? '' : 'bg-accent-dim'}`}
                    >
                      <span className={`block text-sm ${n.read ? 'text-ink-dim' : 'font-medium text-ink'}`}>{n.title}</span>
                      {n.body && <span className="mt-0.5 block text-xs text-ink-dim">{n.body}</span>}
                      <span className="mt-1 block text-micro text-ink-mute">
                        <ClockTime value={n.created_at} />
                      </span>
                    </Link>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="px-4 py-8 text-center text-sm text-ink-mute">
                {followsNothing
                  ? 'Follow a stock from its page, and changes to it will show up here.'
                  : 'Nothing new on the stocks you follow yet.'}
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default NotificationBell;
