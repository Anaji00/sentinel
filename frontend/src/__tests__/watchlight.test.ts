import { describe, expect, it } from 'vitest';
import { arc, councilQuestion, trailLines, verdictHeadline, verdictLine } from '../lib/watchlight';

const card = { agency: 'Round Lake Park PD', technologies: [{ technology: 'ALPR' }, { technology: 'Drones' }], nearby_devices: { ALPR: 14 }, oversight: null };

describe('verdictLine', () => {
  it('says what is documented, what is shared, and that no law is on record', () => {
    const v = verdictLine(card, { shared_with: new Array(647).fill('x'), out_of_state_share: 540 });
    expect(v).toContain('documents 2 kinds of surveillance');
    expect(v).toContain('14 plate readers are mapped nearby');
    expect(v).toContain('shares plate data with 647 agencies, 540 in other states');
    expect(v).toContain('No local oversight law is on record.');
  });
  it('leaves out what is missing and never words absence as a fact', () => {
    expect(verdictLine({ agency: 'X PD', oversight: { id: 1 } })).toBe('X PD has no documented surveillance yet. A local oversight law applies.');
    expect(verdictLine({ agency: 'Y PD' }, { shared_with: [] })).not.toContain('shares');
  });
});

describe('councilQuestion', () => {
  it('is a numbered draft that names the agency and the sharing count', () => {
    const q = councilQuestion(card, { shared_with: [1, 2, 3] });
    expect(q).toContain('Round Lake Park PD');
    expect(q).toContain('shares plate data with 3 agencies');
    expect(q).toMatch(/\n4\. /);
  });
});

describe('trailLines and arc', () => {
  it('draws only to known states, biggest first', () => {
    expect(trailLines({ IL: 5, federal: 9, unknown: 2, TX: 30, ZZ: 4 }).map((l) => l.state)).toEqual(['TX', 'IL']);
    expect(trailLines(null)).toEqual([]);
  });
  it('starts and ends where it should', () => {
    const a = arc([40, -89], [31, -99], 10);
    expect(a).toHaveLength(11);
    expect(a[0]).toEqual([40, -89]);
    expect(a[10][0]).toBeCloseTo(31, 5);
  });
});

describe('verdictHeadline', () => {
  it('leads with sharing, then plate readers, then what is documented', () => {
    expect(verdictHeadline(card, { shared_with: new Array(914).fill('x') })).toBe("914 agencies can search this department's plate data.");
    expect(verdictHeadline(card)).toBe('14 plate readers watch this area.');
    expect(verdictHeadline({ agency: 'A', technologies: [{ technology: 'Drones' }] })).toBe('1 kind of surveillance on record here.');
    expect(verdictHeadline({ agency: 'A' })).toBe('No surveillance documented here yet.');
  });
});
