import { describe, expect, it } from 'vitest';
import { barDay, collapseEvents, placeEvents, type StockEvent } from '../lib/events';
import type { Bar } from '../lib/candles';

const bar = (ts: string): Bar => ({ ts, open: 1, high: 1, low: 1, close: 1, volume: 0 });
const ev = (date: string, kind: StockEvent['kind'], label: string): StockEvent => ({ date, kind, label });

describe('collapseEvents', () => {
  it('keeps one entry per date, kind and label, and drops bad dates', () => {
    const out = collapseEvents([ev('2026-11-10', 'alert', 'a'), ev('2026-11-10', 'alert', 'a'), ev('2026-11-10', 'alert', 'b'), ev('nope', 'filing', 'x')]);
    expect(out.map((e) => e.label)).toEqual(['a', 'b']);
    expect(collapseEvents(undefined)).toEqual([]);
  });
});

describe('barDay', () => {
  it('uses the date for day bars and the New York date for hourly bars', () => {
    expect(barDay('2026-11-10T00:00:00Z', '1d')).toBe('2026-11-10');
    expect(barDay('2026-11-11T02:00:00Z', '1h')).toBe('2026-11-10'); // 21:00 the evening before in New York
  });
});

describe('placeEvents', () => {
  const bars = ['2026-11-06', '2026-11-09', '2026-11-10', '2026-11-11'].map((d) => bar(`${d}T00:00:00Z`));
  it('puts an event on its bar, and a weekend one on Friday', () => {
    const p = placeEvents([ev('2026-11-10', 'earnings', 'Q3'), ev('2026-11-07', 'filing', '8-K')], bars, '1d');
    expect(p.map((x) => [x.i, x.kind])).toEqual([[0, 'filing'], [2, 'earnings']]);
  });
  it('leaves off events before the first bar or too far after the last', () => {
    expect(placeEvents([ev('2026-10-01', 'filing', 'old'), ev('2026-11-20', 'filing', 'future')], bars, '1d')).toEqual([]);
  });
  it('joins events of one kind on one bar', () => {
    const p = placeEvents([ev('2026-11-10', 'alert', 'one'), ev('2026-11-10', 'alert', 'two')], bars, '1d');
    expect(p).toHaveLength(1);
    expect(p[0].count).toBe(2);
    expect(p[0].label).toBe('one; two');
  });
  it('does nothing without bars', () => {
    expect(placeEvents([ev('2026-11-10', 'filing', 'x')], [], '1d')).toEqual([]);
  });
});
