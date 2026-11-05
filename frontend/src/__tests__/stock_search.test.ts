import { describe, expect, it } from 'vitest';
import { cleanTicker, looksLikeTicker, stockHref } from '../lib/openStock';
import { drawerTicker } from '../components/StockDrawer';
import { MAX_RECENT, readRecent, suggest, withRecent, writeRecent } from '../lib/stockSearch';

const board = [
  { ticker: 'NVDA', name: 'NVIDIA Corporation' },
  { ticker: 'NVT', name: 'Nvent Electric' },
  { ticker: 'AAPL', name: 'Apple Inc' },
  { ticker: 'AMD', name: 'Advanced Micro Devices' },
  { ticker: 'XOM', name: 'Exxon Mobil' },
];
const base = { query: '', followed: [] as string[], recent: [] as string[], board };

describe('opening a stock', () => {
  it('cleans a ticker the way the drawer does', () => {
    for (const raw of ['nvda', ' brk.b ', 'a/b', '$X', 'TOOLONGTICKERHERE', '', 'a-b']) {
      expect(cleanTicker(raw)).toBe(drawerTicker(raw));
    }
    expect(looksLikeTicker('nvda')).toBe(true);
    expect(looksLikeTicker('.x')).toBe(false);
    expect(looksLikeTicker('')).toBe(false);
  });

  it('keeps the page and its other parameters', () => {
    expect(stockHref('/calls', '?view=signals', 'nvda')).toBe('/calls?view=signals&t=NVDA');
    expect(stockHref('/', '', 'BRK.B')).toBe('/?t=BRK.B');
    expect(stockHref('/calls', '?t=OLD&view=record', 'xom')).toBe('/calls?t=XOM&view=record');
  });
});

describe('suggestions', () => {
  it('shows followed then recent stocks before anything is typed', () => {
    const out = suggest({ ...base, followed: ['GOOGL'], recent: ['XOM', 'GOOGL'] });
    expect(out.map((s) => [s.ticker, s.kind])).toEqual([['GOOGL', 'following'], ['XOM', 'recent']]);
    expect(suggest(base)).toEqual([]);
  });

  it('puts an exact ticker first, then prefixes, then names', () => {
    expect(suggest({ ...base, query: 'nv' }).map((s) => s.ticker)).toEqual(['NVDA', 'NVT', 'NV']);
    expect(suggest({ ...base, query: 'nvda' })[0]).toMatchObject({ ticker: 'NVDA', name: 'NVIDIA Corporation' });
    expect(suggest({ ...base, query: 'exxon' })[0].ticker).toBe('XOM');
    expect(suggest({ ...base, query: 'micro' })[0].ticker).toBe('AMD');
  });

  it('ranks what you follow ahead of an equal match', () => {
    const out = suggest({ ...base, followed: ['NVT'], query: 'nv' });
    expect(out[0]).toMatchObject({ ticker: 'NVT', kind: 'following' });
  });

  it('always offers what was typed as a stock to open, last unless nothing else matches', () => {
    const out = suggest({ ...base, query: 'ZZZZ' });
    expect(out).toEqual([{ ticker: 'ZZZZ', name: null, kind: 'typed' }]);
    expect(suggest({ ...base, query: 'nv' }).some((s) => s.kind === 'typed')).toBe(true);
    expect(suggest({ ...base, query: 'nvda' }).some((s) => s.kind === 'typed')).toBe(false);
  });

  it('offers nothing to open for something that cannot be a ticker', () => {
    expect(suggest({ ...base, query: '...' })).toEqual([]);
    expect(suggest({ ...base, query: '   ' })).toEqual([]);
  });

  it('never returns more than the limit, and never the same stock twice', () => {
    const many = Array.from({ length: 30 }, (_, i) => ({ ticker: `AB${i}`, name: `Alpha ${i}` }));
    const out = suggest({ ...base, board: many, followed: ['AB1'], query: 'ab', limit: 5 });
    expect(out).toHaveLength(5);
    expect(new Set(out.map((s) => s.ticker)).size).toBe(5);
  });
});

describe('recent stocks', () => {
  it('moves the latest to the front without repeats, up to a cap', () => {
    expect(withRecent(['A', 'B', 'C'], 'b')).toEqual(['B', 'A', 'C']);
    const long = Array.from({ length: MAX_RECENT + 3 }, (_, i) => `T${i}`);
    expect(withRecent(long, 'new')).toHaveLength(MAX_RECENT);
    expect(withRecent(['A'], '!!')).toEqual(['A']);
  });

  it('survives missing, broken or nonsense storage', () => {
    const mem = { data: '' as string | null, getItem() { return this.data; }, setItem(_k: string, v: string) { this.data = v; } };
    expect(readRecent(null)).toEqual([]);
    mem.data = 'not json';
    expect(readRecent(mem)).toEqual([]);
    mem.data = '{"a":1}';
    expect(readRecent(mem)).toEqual([]);
    writeRecent(['nvda', 'xom'], mem);
    expect(readRecent(mem)).toEqual(['NVDA', 'XOM']);
    const broken = { getItem: () => { throw new Error('x'); }, setItem: () => { throw new Error('x'); } };
    expect(readRecent(broken)).toEqual([]);
    expect(() => writeRecent(['A'], broken)).not.toThrow();
  });
});
