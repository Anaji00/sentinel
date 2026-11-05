import { describe, expect, it } from 'vitest';
import {
  changesKey,
  dayLabel,
  eventKindLabel,
  flowView,
  macroLine,
  optionsLine,
  priceLine,
  rankLine,
  safeUrl,
  sectionNote,
  shortLine,
} from '../lib/whatChanged';

// Figures from the live /changes response for NVDA and XOM on 5 Nov 2026.
const nvdaBlocks = {
  kind: 'equity_block', buyer_usd: 32411030, seller_usd: 40970137, unknown_usd: 199504377,
  buyer_trades: 78, seller_trades: 92, unknown_trades: 7, note: 'Side inferred from the price.',
};

describe('what changed', () => {
  it('builds the route from a ticker, encoded', () => {
    expect(changesKey('NVDA')).toBe('/stocks/NVDA/changes');
    expect(changesKey('BRK.B')).toBe('/stocks/BRK.B/changes');
  });

  it('writes price moves, closing price and missing pieces plainly', () => {
    expect(priceLine({ as_of: '2026-11-04', close: 200.13, change_1d_pct: -3.17, change_5d_pct: -7.08 })).toBe(
      'Last session: fell 3.2%. Last 5 sessions: fell 7.1%. Closed at $200.13.',
    );
    expect(priceLine({ as_of: '2026-11-04', close: null, change_1d_pct: 1.04, change_5d_pct: null })).toBe('Last session: rose 1.0%.');
  });

  it('says the model rank in everyday words, with its direction', () => {
    expect(rankLine({ as_of: 'x', percentile: 0.8498, percentile_before: 0.7948, note: '' })).toBe(
      'The model ranks it stronger than 85% of stocks, up from 79% five days earlier.',
    );
    expect(rankLine({ as_of: 'x', percentile: 0.5, percentile_before: 0.62, note: '' })).toContain('down from 62%');
    expect(rankLine({ as_of: 'x', percentile: 0.5, percentile_before: null, note: '' })).toBe('The model ranks it stronger than 50% of stocks.');
    expect(rankLine({ as_of: 'x', percentile: null, percentile_before: null, note: '' })).toBeNull();
  });

  it('explains options without jargon', () => {
    const line = optionsLine({ as_of: 'x', atm_iv: 0.5391, atm_iv_before: 0.4314, put_call_ratio: 0.498 });
    expect(line).toContain('yearly swing of about 54%, up from 43%');
    expect(line).toContain('0.50 puts traded for every call: more bets on a rise than a fall.');
    expect(optionsLine({ as_of: 'x', atm_iv: null, atm_iv_before: null, put_call_ratio: null })).toBeNull();
  });

  it('describes short interest, leaving its age to the date beside it', () => {
    expect(shortLine({ as_of: '2026-10-15', short_shares: 301622687, change_pct: -3.14, days_to_cover: 2.9105 })).toBe(
      'Short sellers held 302M shares, 3.1% fewer than the report before. Buying them back would take about 2.9 days of trading.',
    );
    expect(shortLine({ as_of: 'x', short_shares: null, change_pct: 1, days_to_cover: 1 })).toBeNull();
  });
});

describe('large trades', () => {
  it('always shows buyer, seller and unknown, with trade counts', () => {
    const v = flowView(nvdaBlocks);
    expect(v.title).toBe('Large stock trades');
    expect(v.parts.map((p) => p.label)).toEqual(['Buyer-started', 'Seller-started', 'Side not known']);
    expect(v.parts.map((p) => p.trades)).toEqual([78, 92, 7]);
    expect(v.parts[2].usd).toBe('$199.5M');
  });

  it('warns when most of the money has no known side, and never computes a lean', () => {
    const v = flowView(nvdaBlocks);
    expect(v.caution).toContain('Most of the money (73%) has no known side');
    expect(v.caution).toContain('does not show which way large traders leaned');
    expect(JSON.stringify(v)).not.toMatch(/ratio|net buy|net sell/i);
  });

  it('stays quiet when the sides are mostly known', () => {
    expect(flowView({ ...nvdaBlocks, unknown_usd: 1000, buyer_usd: 50000, seller_usd: 40000 }).caution).toBeNull();
    expect(flowView({ ...nvdaBlocks, buyer_usd: 0, seller_usd: 0, unknown_usd: 0 }).caution).toBeNull();
  });
});

describe('missing parts', () => {
  it('tells no data from could-not-read, and says nothing for a good part', () => {
    expect(sectionNote('prices', 'no_data')).toEqual({ text: 'No prices stored for this stock.', tone: 'muted' });
    expect(sectionNote('prices', 'unavailable')).toEqual({ text: 'Could not read prices right now.', tone: 'caution' });
    expect(sectionNote('prices', 'ok')).toBeNull();
  });
});

describe('market links, events and links', () => {
  it('words the strength of a measured link', () => {
    expect(macroLine({ symbol: 'BZ=F', name: 'Brent crude', direction: 'with', t_stat: 10.22, sessions: 252, as_of: null })).toBe(
      'Tends to move with Brent crude (a strong link)',
    );
    expect(macroLine({ symbol: 'DXY', name: 'US dollar', direction: 'against', t_stat: 3.1, sessions: 250, as_of: null })).toBe(
      'Tends to move against US dollar (a weaker link)',
    );
  });

  it('names event kinds for a person, and falls back sensibly', () => {
    expect(eventKindLabel('insider_trade')).toBe('Insider trade');
    expect(eventKindLabel('filing')).toBe('SEC filing');
    expect(eventKindLabel('some_new_kind')).toBe('Some new kind');
    expect(eventKindLabel('')).toBe('Item');
  });

  it('only links to web addresses', () => {
    expect(safeUrl('https://www.sec.gov/ix?doc=/x')).toBe('https://www.sec.gov/ix?doc=/x');
    expect(safeUrl('javascript:alert(1)')).toBeNull();
    expect(safeUrl('data:text/html,<b>x</b>')).toBeNull();
    expect(safeUrl('not a url')).toBeNull();
    expect(safeUrl(null)).toBeNull();
  });

  it('writes a stored day in the app style and nothing for a bad one', () => {
    expect(dayLabel('2026-11-05')).toBe('Nov 5');
    expect(dayLabel('2026-11-05T21:00:00Z')).toBe('Nov 5');
    expect(dayLabel(null)).toBe('');
    expect(dayLabel('nope')).toBe('');
  });
});
