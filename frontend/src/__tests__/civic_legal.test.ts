import { describe, it, expect } from 'vitest';
import { auditSummary, brokerFlags, FEED_KIND, lawLine, severityLabel, sharingSummary, tieLine } from '../components/CivicLegal';

/**
 * The legal checks speak as questions with evidence: a possible violation is
 * not a ruling, and sharing is described in counts a reader can verify.
 */
describe('is it legal here', () => {
  it('labels findings without overstating them', () => {
    expect(severityLabel('conflict')).toBe('Possible violation');
    expect(severityLabel('check')).toBe('Ask for records');
  });

  it('describes where an agency sends its plate data', () => {
    const p = { state: 'CA', shared_with_count: 10, shared_by_state: { CA: 5, TX: 2, GA: 1, federal: 1, unknown: 1 } };
    expect(sharingSummary(p)).toBe('Shares its plate data with 10 agencies: 3 of them in 2 other states: 1 federal.');
    expect(sharingSummary({ state: 'WA', shared_with_count: 0, shared_by_state: {} })).toBe(
      'Shares its plate data with no other agency.',
    );
    expect(sharingSummary({ state: 'OR', shared_with_count: 4, shared_by_state: { OR: 4 } })).toBe(
      'Shares its plate data with 4 agencies.',
    );
  });
});

describe('state law line', () => {
  it('states what the law requires, or that there is none', () => {
    const ct = {
      state: 'CT', citation: 'SB 397 (2026)', retention_text: '21 days', immigration_use_banned: true,
      immigration_sharing_banned: true, out_of_state_sharing_banned: true, declaration_required: true,
      federal_sharing_banned: true, note: null, source: 'x',
    };
    expect(lawLine(ct, 'CT')).toBe(
      'CT (SB 397 (2026)): routine data kept at most 21 days; no use for immigration enforcement; sharing outside the state restricted; out-of-state recipients must sign a declaration; federal sharing restricted.',
    );
    expect(lawLine(undefined, 'TX')).toContain('no statute');
  });
});

describe('search log summary', () => {
  it('reports counts and shares, never reasons', () => {
    const a = {
      days: 90, searches: 2000, immigration_hits: 12, immigration_terms: { ICE: 10, HSI: 2 },
      first_amendment_hits: 0, first_amendment_terms: {}, wide_searches: 500, no_reason: 40,
    };
    expect(auditSummary(a)).toBe(
      "2,000 searches in the last 90 days; 25.0% reached 100+ agencies' networks; 2.0% gave no reason; 12 cite immigration terms.",
    );
  });
});

describe('data broker declarations', () => {
  it('lists what a broker declared, worst first, and nothing it did not', () => {
    const labels = { sold_to_law_enforcement: 'sold data to law enforcement (no subpoena)', precise_geolocation: 'collects precise location' };
    expect(brokerFlags({ precise_geolocation: true, sold_to_law_enforcement: true, minors: false }, labels)).toEqual([
      'sold data to law enforcement (no subpoena)',
      'collects precise location',
    ]);
    expect(brokerFlags({ precise_geolocation: null }, labels)).toEqual([]);
  });
});

describe('watch feed', () => {
  it('names every kind the feed serves', () => {
    for (const k of ['news', 'finding', 'portal_dropped', 'portal_new', 'drone_waiver', 'federal_award', 'deployment']) {
      expect(FEED_KIND[k]).toBeTruthy();
    }
  });
});

describe('court cases', () => {
  it('states a judge\'s disclosed holdings with years and value range', () => {
    expect(tieLine([{ vendor: 'Axon', years: [2019, 2017], latest_value: '$15,001–50,000', description: null }])).toBe(
      'Axon (2019, 2017, $15,001–50,000)',
    );
    expect(tieLine([{ vendor: 'Palantir', years: [], latest_value: null, description: null }])).toBe('Palantir (year unknown)');
  });
});
