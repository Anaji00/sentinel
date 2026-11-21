import { describe, expect, it } from 'vitest';
import { rangeRequest, splitWarmup, barAt, barLabel, barRange, barTick, cleanBars, isUp, movingAverage, priceAtY, spanDays, summarize } from '../lib/candles';

const bar = (ts: string, o: number, h: number, l: number, c: number, v = 100) => ({ ts, open: o, high: h, low: l, close: c, volume: v });

describe('cleanBars', () => {
  it('sorts oldest first, as the cache path returns newest first', () => {
    const out = cleanBars([bar('2026-11-03', 1, 2, 1, 2), bar('2026-11-02', 1, 2, 1, 1.5)]);
    expect(out.map((b) => b.ts)).toEqual(['2026-11-02', '2026-11-03']);
  });
  it('keeps one bar per timestamp', () => {
    expect(cleanBars([bar('a', 1, 2, 1, 2), bar('a', 1, 3, 1, 3)])).toHaveLength(1);
  });
  it('drops bars that cannot be drawn', () => {
    const out = cleanBars([bar('a', 0, 2, 1, 2), { ts: 'b', open: 1, high: NaN, low: 1, close: 1 }, { open: 1, high: 1, low: 1, close: 1 }, bar('c', 1, 2, 1, 2)]);
    expect(out.map((b) => b.ts)).toEqual(['c']);
  });
  it('widens a wick that does not cover the body', () => {
    const [b] = cleanBars([bar('a', 5, 4, 6, 7)]);
    expect(b.high).toBe(7);
    expect(b.low).toBe(5);
  });
  it('treats missing volume as zero', () => {
    expect(cleanBars([{ ts: 'a', open: 1, high: 1, low: 1, close: 1 }])[0].volume).toBe(0);
  });
});

describe('movingAverage', () => {
  it('is null until enough bars, then the plain average', () => {
    const bars = [1, 2, 3, 4].map((c, i) => bar(`d${i}`, c, c, c, c));
    expect(movingAverage(bars, 3)).toEqual([null, null, 2, 3]);
  });
});

describe('summarize and range', () => {
  const bars = [bar('a', 10, 12, 9, 11), bar('b', 11, 15, 10, 14)];
  it('measures from the first open to the last close', () => {
    const s = summarize(bars)!;
    expect(s.change).toBe(4);
    expect(s.changePct).toBeCloseTo(40);
    expect(s.high).toBe(15);
    expect(s.low).toBe(9);
  });
  it('is null when there are no bars', () => {
    expect(summarize([])).toBeNull();
    expect(barRange([])).toBeNull();
  });
  it('pads the range and finds the biggest volume', () => {
    const r = barRange([bar('a', 10, 12, 9, 11, 50), bar('b', 11, 15, 10, 14, 80)])!;
    expect(r.lo).toBeLessThan(9);
    expect(r.hi).toBeGreaterThan(15);
    expect(r.maxVolume).toBe(80);
  });
});

describe('pointer and labels', () => {
  it('maps a position to a bar and clamps', () => {
    expect(barAt(0, 10)).toBe(0);
    expect(barAt(0.55, 10)).toBe(5);
    expect(barAt(1, 10)).toBe(9);
    expect(barAt(-1, 10)).toBe(0);
    expect(barAt(0.5, 0)).toBe(-1);
  });
  it('calls an unchanged bar up', () => {
    expect(isUp(bar('a', 1, 1, 1, 1))).toBe(true);
    expect(isUp(bar('a', 2, 2, 1, 1))).toBe(false);
  });
  it('labels day bars by date and intraday bars with the New York time', () => {
    expect(barLabel('2026-11-10T00:00:00Z', '1d')).toBe('Nov 10');
    expect(barLabel('2026-11-10T15:00:00Z', '1h')).toMatch(/Nov 10, 10:00 AM New York time/);
  });
});

describe('axis helpers', () => {
  it('labels ticks short', () => {
    expect(barTick('2026-11-10T00:00:00Z', '1d')).toBe('Nov 10');
    expect(barTick('2026-11-10T15:00:00Z', '1h')).toBe('Nov 10 10:00 AM');
  });
  it('counts the days a window covers', () => {
    expect(spanDays([bar('2026-11-01T00:00:00Z', 1, 1, 1, 1), bar('2026-11-04T00:00:00Z', 1, 1, 1, 1)])).toBe(3);
    expect(spanDays([])).toBe(0);
  });
  it('turns a height into a price, and nothing outside the plot', () => {
    expect(priceAtY(0, 100, 10, 20)).toBe(20);
    expect(priceAtY(100, 100, 10, 20)).toBe(10);
    expect(priceAtY(50, 100, 10, 20)).toBe(15);
    expect(priceAtY(150, 100, 10, 20)).toBeNull();
  });
});

describe('ranges and warm-up', () => {
  it('asks for the year to draw three months of daily bars', () => {
    expect(rangeRequest('3M')).toEqual({ ask: '1Y', keep: 66 });
    expect(rangeRequest('1M')).toEqual({ ask: '1M', keep: null });
  });
  it('splits warm-up bars off and trims the averages to match', () => {
    const all = [{ ...bar('a', 1, 1, 1, 1), warmup: true }, { ...bar('b', 1, 1, 1, 1), warmup: true }, bar('c', 1, 1, 1, 1), bar('d', 1, 1, 1, 1)];
    const out = splitWarmup(all, [[1, 2, 3, 4]]);
    expect(out.bars.map((b) => b.ts)).toEqual(['c', 'd']);
    expect(out.averages[0]).toEqual([3, 4]);
  });
  it('keeps everything when there is no warm-up', () => {
    const all = [bar('a', 1, 1, 1, 1), bar('b', 1, 1, 1, 1)];
    expect(splitWarmup(all, [[1, 2]]).bars).toHaveLength(2);
  });
});
