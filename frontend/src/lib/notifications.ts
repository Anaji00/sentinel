/**
 * The inbox's data and the small decisions around it: what counts as new, and
 * whether this browser may pop up an alert. Rows come from
 * services/api_gateway/routes/me.py.
 */

export const NOTIFICATIONS_KEY = '/me/notifications';
export const BROWSER_ALERTS_KEY = 'sentinel.browser_alerts';
/** More new rows than this at once become one summary pop-up, not a stack. */
export const MAX_POPUPS = 3;

export interface NotificationItem {
  id: number;
  kind: string;
  ticker: string | null;
  title: string;
  body: string;
  url: string;
  created_at: string;
  read: boolean;
}

export interface InboxResponse {
  unread: number;
  items: NotificationItem[];
}

export interface NotifyPrefs {
  call: boolean;
  signal: boolean;
  filing: boolean;
  holders: boolean;
  email_digest: boolean;
}

export const DEFAULT_NOTIFY: NotifyPrefs = { call: true, signal: true, filing: true, holders: true, email_digest: false };

export const KIND_LABELS: { key: keyof Omit<NotifyPrefs, 'email_digest'>; label: string; hint: string }[] = [
  { key: 'call', label: 'New or reversed calls', hint: 'From models that passed validation' },
  { key: 'signal', label: 'Evidence shifts', hint: 'A higher-confidence lean appears or flips' },
  { key: 'filing', label: 'Filings', hint: 'Such as large-holder Schedule 13D and 13G' },
  { key: 'holders', label: 'Big institutions moving', hint: 'Several active managers moved the same way' },
];

/** The unread rows not seen on an earlier check. The first check announces nothing. */
export function newUnread(items: NotificationItem[], seen: Set<number> | null): NotificationItem[] {
  if (!seen) return [];
  return items.filter((n) => !n.read && !seen.has(n.id));
}

/** What to pop up for these new rows: each one, or one summary when there are many. */
export function popups(fresh: NotificationItem[], max: number = MAX_POPUPS): { title: string; body: string; url: string }[] {
  if (fresh.length <= max) return fresh.map(({ title, body, url }) => ({ title, body, url }));
  return [{ title: `${fresh.length} new updates on stocks you follow`, body: 'Open Sentinel to read them.', url: '/calls' }];
}

/** The count on the bell. */
export function badgeText(unread: number): string {
  return unread > 99 ? '99+' : String(unread);
}

/** Browser alerts need the API and a secure page; plain HTTP has neither. */
export function browserAlertsSupported(): boolean {
  return typeof window !== 'undefined' && 'Notification' in window && window.isSecureContext;
}
