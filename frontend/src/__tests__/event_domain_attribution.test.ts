import { describe, it, expect } from 'vitest';
import { domainMetaFor, getCleanSource } from '../components/IntelligenceFeed';
import type { NormalizedEvent } from '../lib/types';

/**
 * BCHUSDT, DOTUSDT and ADAUSDT rendered as "TRADFI ... via AlphaVantage Feed".
 *
 * The domain came from substring-matching the event type, and "market_anomaly"
 * -- the type the Coinbase candle enricher emits -- contains "market", so it
 * matched the TRADFI branch. The source was then invented to match the wrong
 * domain, crediting Coinbase ticks to a market-data vendor this deployment
 * does not use.
 */

const row = (over: Partial<NormalizedEvent>): NormalizedEvent =>
  ({ event_id: 'e1', type: 'market_anomaly', occurred_at: '2026-08-24T04:28:50Z',
     anomaly_score: 1, ...over } as NormalizedEvent);

describe('event domain attribution', () => {
  it('labels a Coinbase candle anomaly as crypto, not tradfi', () => {
    // The exact shape the gateway now returns for these symbols.
    const e = row({ primary_entity_id: 'BCHUSDT', source: 'coinbase_candles', domain: 'crypto' });
    expect(domainMetaFor(e).label).toBe('CRYPTO');
  });

  it.each(['DOTUSDT', 'ADAUSDT', 'AVAXUSDT', 'DOGEUSDT'])('classifies %s as crypto', (sym) => {
    const e = row({ primary_entity_id: sym, source: 'coinbase_candles', domain: 'crypto' });
    expect(domainMetaFor(e).label).toBe('CRYPTO');
  });

  it('still labels a genuine equity event as tradfi', () => {
    const e = row({ type: 'market_anomaly', primary_entity_id: 'NVDA', domain: 'tradfi' });
    expect(domainMetaFor(e).label).toBe('TRADFI');
  });

  it('falls back to the payload when the server sends no domain', () => {
    const crypto = row({ crypto_data: { pair: 'BTC-USDT-SWAP' } });
    expect(domainMetaFor(crypto).label).toBe('CRYPTO');
    const equity = row({ financial_data: { ticker: 'NVDA' } });
    expect(domainMetaFor(equity).label).toBe('TRADFI');
  });

  it('prefers crypto when a row somehow carries both payloads', () => {
    const e = row({ crypto_data: { pair: 'BTC' }, financial_data: { ticker: 'BTC' } });
    expect(domainMetaFor(e).label).toBe('CRYPTO');
  });

  it('reproduces the original defect without a declared domain or payload', () => {
    // Nothing to go on but the type: this is the case the server now prevents.
    const e = row({ primary_entity_id: 'BCHUSDT' });
    expect(domainMetaFor(e).label).toBe('TRADFI');
  });
});

describe('source attribution', () => {
  it('reports the real collector', () => {
    expect(getCleanSource(row({ source: 'coinbase_candles' }))).toBe('coinbase_candles');
  });

  it('invents nothing when the row carries no source', () => {
    expect(getCleanSource(row({}))).toBe('');
    expect(getCleanSource(row({ source: 'unknown' }))).toBe('');
  });

  it('never names a vendor this deployment does not use', () => {
    const vendors = ['AlphaVantage', 'CoinGecko', 'AISStream', 'PolyMarket API'];
    for (const e of [row({}), row({ source: 'unknown' }), row({ domain: 'tradfi' })]) {
      const out = getCleanSource(e);
      for (const v of vendors) expect(out).not.toContain(v);
    }
  });
});
