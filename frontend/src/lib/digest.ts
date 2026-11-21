/**
 * "What changed since you last looked" (`/changes`) and the earnings calendar (`/calendar/earnings`) in plain words.
 * A source that failed is reported as failed, never as zero changes, and the digest says exactly which time it counts
 * from.
 */

import { plainForm } from './plainFiling';

export interface SourceBlock<T> {
  status: 'ok' | 'error';
  count: number | null;
  items: T[];
}

export interface Changes {
  since: string;
  as_of: string | null;
  total: number | null;
  sections: {
    new_calls?: SourceBlock<{ day: string; horizon: string; ticker: string; side: string; validated?: boolean; structure?: string | null }>;
    filings?: SourceBlock<{ ticker: string; company_name?: string | null; form_type: string; filed_at: string | null; headline?: string | null; url?: string | null }>;
    divergence_alerts?: SourceBlock<{ ticker: string; detected_at: string; rule?: string | null; regime?: { label?: string | null; action?: string | null } | null }>;
    price_alerts_fired?: SourceBlock<{ id: number; ticker: string; direction: 'above' | 'below'; level: number; triggered_at: string | null; triggered_price: number | null }>;
    model?: { as_of: string | null; generated_at: string | null; newer: boolean };
  };
}

export interface FeedItem {
  id: string;
  kind: 'filing' | 'call' | 'divergence' | 'price_alert';
  ticker: string | null;
  at: string;
  at_precision: 'second' | 'day';
  title: string;
  detail?: Record<string, unknown> | null;
}

export interface Feed {
  since: string;
  as_of: string | null;
  items: FeedItem[];
  failed: string[];
}

const KIND_GROUP: Record<FeedItem['kind'], string> = { filing: 'Filings', call: 'New ideas', divergence: 'Moving against peers', price_alert: 'Your price alerts' };

/** The merged feed as timeline lines: the backend's own plain title, the time (a date only for day-precision items). */
export function feedLines(f: Feed | null | undefined): DigestLine[] {
  if (!f) return [];
  const out: DigestLine[] = (f.items ?? []).map((i) => ({
    key: i.id,
    group: KIND_GROUP[i.kind] ?? i.kind,
    at: i.at_precision === 'day' ? `${i.at.slice(0, 10)}T00:00:00Z` : i.at,
    ticker: i.ticker ?? undefined,
    // "(events)" is the backend's name for ideas driven by news and filings.
    text: i.title.replace(/\(events\)/i, '(news-driven)'),
  }));
  for (const name of f.failed ?? []) {
    out.push({ key: `err-${name}`, group: name, text: `Could not read ${name}. This is not the same as nothing changing.`, failed: true });
  }
  return out.sort((a, b) => (b.at ? Date.parse(b.at) : -Infinity) - (a.at ? Date.parse(a.at) : -Infinity) || Number(Boolean(b.failed)) - Number(Boolean(a.failed)));
}

export interface DigestLine {
  key: string;
  group: string;
  /** When it happened, if known; ideas carry only a day. */
  at?: string | null;
  text: string;
  ticker?: string;
  failed?: boolean;
}

const money = (v: number) => `$${v.toFixed(2)}`;

/** The holding periods as a person says them; "events" ideas come from news and filings, not a ranking. */
const HORIZON_WORD: Record<string, string> = { daily: 'one-day', weekly: 'one-week', monthly: 'one-month', events: 'news-driven' };

/** Every change as one plain line, newest groups first, with a line for each source that could not be read. */
export function digestLines(c: Changes | null | undefined, day: (iso: string) => string): DigestLine[] {
  if (!c) return [];
  const out: DigestLine[] = [];
  const s = c.sections;
  const block = <T,>(b: SourceBlock<T> | undefined, name: string, add: (items: T[]) => void) => {
    if (!b) return;
    if (b.status === 'error') out.push({ key: `err-${name}`, group: name, text: `Could not read ${name}. This is not the same as nothing changing.`, failed: true });
    else add(b.items);
  };
  block(s.price_alerts_fired, 'your price alerts', (items) =>
    items.forEach((a) => out.push({ key: `pa-${a.id}`, group: 'Your price alerts', at: a.triggered_at, ticker: a.ticker, text: `${a.ticker} ${a.direction === 'above' ? 'rose above' : 'fell below'} ${money(a.level)}${a.triggered_price != null ? `, reaching ${money(a.triggered_price)}` : ''}.` })),
  );
  block(s.new_calls, 'new ideas', (items) =>
    items.forEach((i) => out.push({ key: `nc-${i.ticker}-${i.horizon}-${i.day}`, group: 'New ideas', at: `${i.day.slice(0, 10)}T00:00:00Z`, ticker: i.ticker, text: `New ${HORIZON_WORD[i.horizon] ?? i.horizon} idea: ${i.side === 'short' ? 'bet against' : 'buy'} ${i.ticker}${i.validated ? '' : ' (model still being tested)'}.` })),
  );
  block(s.filings, 'filings', (items) =>
    items.forEach((f) => out.push({ key: `f-${f.ticker}-${f.filed_at}-${f.form_type}`, group: 'Filings', at: f.filed_at, ticker: f.ticker, text: `${f.ticker} filed ${plainForm(f.form_type)} (${f.form_type})${f.filed_at ? ` on ${day(f.filed_at)}` : ''}${f.headline && !/^SEC Filing/i.test(f.headline) && !/filed Form/i.test(f.headline) ? `: ${f.headline}` : '.'}` })),
  );
  block(s.divergence_alerts, 'stocks moving against their peers', (items) =>
    items.forEach((a) => out.push({ key: `da-${a.ticker}-${a.detected_at}`, group: 'Moving against peers', at: a.detected_at, ticker: a.ticker, text: `${a.ticker} began moving away from its peers on ${day(a.detected_at)}${a.regime?.label ? `: ${a.regime.label}` : ''}.` })),
  );
  if (s.model?.newer) out.push({ key: 'model', group: 'The model', at: s.model.generated_at, text: `The model published a new run, with scores dated ${s.model.as_of ? day(s.model.as_of) : 'an unknown day'}.` });
  // Newest first; a line with no time goes last, and a failed source stays visible at the top of its group.
  return out.sort((a, b) => (b.at ? Date.parse(b.at) : -Infinity) - (a.at ? Date.parse(a.at) : -Infinity) || Number(Boolean(b.failed)) - Number(Boolean(a.failed)));
}

/**
 * The time gutter beside a timeline line: the date, and the New York clock time when the instant has one.
 * An instant at exactly midnight UTC is a day with no time of day (a logged idea), so it gets a date only.
 */
export function gutterParts(at: string | null | undefined, day: (iso: string) => string, clock: (iso: string) => string): { date: string; time: string | null } | null {
  if (!at || Number.isNaN(Date.parse(at))) return null;
  const midnight = /T00:00:00(?:\.0+)?(?:Z|\+00:00)$/.test(at);
  return { date: day(at), time: midnight ? null : clock(at) };
}

/** The headline counts, for the heading: how many changes were found, and how many sources could not be read. */
export function digestSummary(c: Changes | null | undefined): { total: number | null; failed: number } {
  if (!c) return { total: null, failed: 0 };
  const blocks = [c.sections.new_calls, c.sections.filings, c.sections.divergence_alerts, c.sections.price_alerts_fired];
  return { total: c.total, failed: blocks.filter((b) => b?.status === 'error').length };
}

export interface EarningsRow {
  ticker: string;
  date: string;
  hour: 'bmo' | 'amc' | 'during_market' | null;
  eps_estimate: number | null;
  revenue_estimate: number | null;
  estimated: boolean;
  market_cap: number | null;
}

export interface EarningsCalendar {
  from: string;
  to: string;
  scope: string;
  count: number;
  rows: EarningsRow[];
}

export function hourWords(h: EarningsRow['hour']): string {
  return h === 'bmo' ? 'before the open' : h === 'amc' ? 'after the close' : h === 'during_market' ? 'during the day' : 'time not stated';
}

/** Rows grouped by date, in date order, keeping the server's order (larger companies first) inside each day. */
export function groupByDate(rows: readonly EarningsRow[] | undefined): { date: string; rows: EarningsRow[] }[] {
  const map = new Map<string, EarningsRow[]>();
  for (const r of rows ?? []) map.set(r.date, [...(map.get(r.date) ?? []), r]);
  return [...map].sort(([a], [b]) => (a < b ? -1 : 1)).map(([date, rs]) => ({ date, rows: rs }));
}

/** An ISO instant for `hours` ago, for the digest's default cut-off. */
export function hoursAgoIso(nowMs: number, hours: number): string {
  return new Date(nowMs - hours * 3_600_000).toISOString();
}
