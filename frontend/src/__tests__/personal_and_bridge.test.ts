import { describe, it, expect } from 'vitest';
import { toggleTicker } from '../lib/preferences';
import { callsLine } from '../components/MyCorner';
import { companyForTicker, companyForVendor, SURVEILLANCE_COMPANIES } from '../lib/surveillanceCompanies';
import { pickView } from '../components/ui/ViewTabs';

/**
 * A person's followed stocks, the bridge from surveillance vendors to their
 * stock pages, and the merged pages' views.
 */

describe('your stocks', () => {
  it('adds and removes a ticker, keeping order and case', () => {
    expect(toggleTicker(['AAPL'], 'nvda')).toEqual(['AAPL', 'NVDA']);
    expect(toggleTicker(['AAPL', 'NVDA'], 'aapl')).toEqual(['NVDA']);
  });

  it('says how many followed stocks are on the calls', () => {
    expect(callsLine(0, 0)).toBe('Follow stocks from any stock page to see them here.');
    expect(callsLine(0, 3)).toBe("None of your 3 stocks is on today's calls.");
    expect(callsLine(0, 1)).toBe("Your stock isn't on today's calls.");
    expect(callsLine(1, 1)).toBe("1 of your 1 stock is on today's calls.");
    expect(callsLine(2, 5)).toBe("2 of your 5 stocks are on today's calls.");
  });
});

describe('surveillance companies on the market', () => {
  it('finds the listed company behind each name the civic records use', () => {
    expect(companyForVendor('Palantir Technologies')?.ticker).toBe('PLTR');
    expect(companyForVendor('Axon Enterprise')?.ticker).toBe('AXON');
    expect(companyForVendor('Palantir')?.ticker).toBe('PLTR');
    // Subsidiaries and old names.
    expect(companyForVendor('Vigilant Solutions')?.ticker).toBe('MSI');
    expect(companyForVendor('ShotSpotter')?.ticker).toBe('SSTI');
    expect(companyForVendor('Harris Corp.')?.ticker).toBe('LHX');
    // Private vendors have no ticker.
    expect(companyForVendor('Flock Safety')).toBeNull();
    expect(companyForVendor('Clearview AI')).toBeNull();
    expect(companyForVendor(null)).toBeNull();
  });

  it('maps every ticker back to its company, once', () => {
    const tickers = SURVEILLANCE_COMPANIES.map((c) => c.ticker);
    expect(new Set(tickers).size).toBe(tickers.length);
    expect(companyForTicker('axon')?.name).toBe('Axon');
    expect(companyForTicker('AAPL')).toBeNull();
  });
});

describe('merged pages', () => {
  const views = [
    { key: 'prices', label: 'Prices' },
    { key: 'economy', label: 'Economy' },
  ];
  it('opens the named view, or the first when the name is unknown or missing', () => {
    expect(pickView(views, 'economy').key).toBe('economy');
    expect(pickView(views, ['economy', 'prices']).key).toBe('economy');
    expect(pickView(views, 'nope').key).toBe('prices');
    expect(pickView(views, undefined).key).toBe('prices');
  });
});
