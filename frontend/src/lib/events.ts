/**
 * Dated events on a stock (`/stocks/{ticker}/events`): earnings dates, filings and alerts, placed on the chart's bars.
 * Markers are neutral in colour because a date is not up or down. Stored rows only, newest first; alert rows can repeat
 * on a busy day, so they are collapsed.
 */

import type { Bar, FrameKey } from './candles';

export interface StockEvent {
  date: string;
  kind: 'earnings' | 'filing' | 'alert';
  label: string;
  url?: string | null;
}

export interface PlacedEvent extends StockEvent {
  /** The index of the bar it sits on. */
  i: number;
  /** Several events that fall on the same bar are listed together on one marker. */
  count: number;
}

/** One entry per date, kind and label. */
export function collapseEvents(events: readonly StockEvent[] | undefined): StockEvent[] {
  const seen = new Set<string>();
  const out: StockEvent[] = [];
  for (const e of events ?? []) {
    const k = `${e.date}|${e.kind}|${e.label}`;
    if (!seen.has(k) && /^\d{4}-\d{2}-\d{2}$/.test(e.date)) {
      seen.add(k);
      out.push(e);
    }
  }
  return out;
}

/** The calendar day a bar belongs to: the date itself for day and week bars, the New York date for intraday bars. */
export function barDay(ts: string, frame: FrameKey): string {
  if (frame === '1d' || frame === '1w') return ts.slice(0, 10);
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts.slice(0, 10);
  const p = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
  return p;
}

const dayMs = (iso: string) => Date.parse(`${iso}T00:00:00Z`);

/**
 * Places events on bars: each goes on the last bar whose day is on or before the event's date (a filing made on a
 * weekend sits on Friday), and only if that bar is within the span the event could belong to. Events before the
 * first bar or after the last are left off.
 */
export function placeEvents(events: readonly StockEvent[], bars: readonly Bar[], frame: FrameKey): PlacedEvent[] {
  if (bars.length === 0) return [];
  const days = bars.map((b) => barDay(b.ts, frame));
  const reach = frame === '1w' ? 7 : 3;
  const byBar = new Map<string, PlacedEvent>();
  for (const e of collapseEvents(events)) {
    let i = -1;
    for (let k = days.length - 1; k >= 0; k--) {
      if (days[k] <= e.date) {
        i = k;
        break;
      }
    }
    if (i < 0) continue;
    if ((dayMs(e.date) - dayMs(days[i])) / 86_400_000 > reach) continue;
    const key = `${i}|${e.kind}`;
    const there = byBar.get(key);
    if (there) {
      there.count += 1;
      there.label = `${there.label}; ${e.label}`;
    } else {
      byBar.set(key, { ...e, i, count: 1 });
    }
  }
  return [...byBar.values()].sort((a, b) => a.i - b.i);
}

export const KIND_WORD: Record<StockEvent['kind'], string> = { earnings: 'Earnings', filing: 'Filing', alert: 'Alert' };
