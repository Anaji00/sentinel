import { describe, expect, it } from 'vitest';
import { closesKey, drawable, sparkPoints, trend, trendWords } from '../lib/spark';

describe('drawable', () => {
  it('needs five usable closes', () => {
    expect(drawable([1, 2, 3, 4])).toBeNull();
    expect(drawable([1, 2, 3, 4, 5])).toEqual([1, 2, 3, 4, 5]);
    expect(drawable([1, NaN, 0, -1, 2, 3, 4, 5, 6])).toEqual([1, 2, 3, 4, 5, 6]);
    expect(drawable(undefined)).toBeNull();
  });
});

describe('sparkPoints', () => {
  it('scales into the box with the low at the bottom', () => {
    expect(sparkPoints([1, 2, 3], 10, 10)).toBe('1.0,9.0 5.0,5.0 9.0,1.0');
  });
  it('runs a flat series along the middle', () => {
    expect(sparkPoints([5, 5, 5], 10, 10)).toBe('1.0,5.0 5.0,5.0 9.0,5.0');
  });
});

describe('trend', () => {
  it('reads direction and words from first to last', () => {
    expect(trend([1, 2, 3])).toBe('up');
    expect(trend([3, 2, 1])).toBe('down');
    expect(trend([2, 9, 2])).toBe('flat');
    expect(trendWords([100, 110, 112.3])).toBe('up 12.3% over 3 days');
    expect(trendWords([5, 6, 5])).toBe('flat over 3 days');
  });
});

describe('closesKey', () => {
  it('builds one request for unique upper-case tickers, capped at 100', () => {
    expect(closesKey(['nvda', ' NVDA', 'aapl'])).toBe('/stocks/closes?tickers=NVDA%2CAAPL&days=60');
    expect(closesKey([])).toBeNull();
    const many = Array.from({ length: 150 }, (_, i) => `T${i}`);
    expect(decodeURIComponent(closesKey(many)!).split('tickers=')[1].split('&')[0].split(',')).toHaveLength(100);
  });
});
