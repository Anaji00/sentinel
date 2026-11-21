import { describe, expect, it } from 'vitest';
import { plainEventText, plainSource, plainTopic, unusualWords, withoutStaleConfidence } from '../lib/plainEvent';

describe('plainEventText', () => {
  it('rewrites a ship-to-ship headline without ship numbers or nautical miles', () => {
    const out = plainEventText('Possible STS: SEA ABUNDANCE alongside MMSI 312248000 for 1.6h at 0.204 nm');
    expect(out).toBe('SEA ABUNDANCE has sat 380 metres from another ship for 1.6 hours, possibly passing cargo');
  });
  it('rewrites the ship detail sentence', () => {
    const out = plainEventText(
      'MMSI 563028450 and MMSI 312248000 have been within 0.204 nm of each other, both under 0.0 knots, for 1.6 hours in Singapore Approach. That geometry is a ship-to-ship transfer or a rendezvous; it is not a berth, because neither hull is alongside a quay, and it is not traffic, because neither is making way.',
    );
    expect(out).toMatch(/^Two ships have stayed within 380 metres of each other for 1.6 hours in Singapore Approach, barely moving\./);
    expect(out).not.toMatch(/MMSI|nm|knots|geometry/);
  });
  it('turns a betting contract into the question and the odds, with no score', () => {
    const out = plainEventText("Prediction market contract 'Will WTI settle above 87.99 on Nov 12?' (Market Odds). Current price/probability: 11.0%. Anomaly score: 0.02.");
    expect(out).toBe('Will WTI settle above 87.99 on Nov 12? The market puts it at 11%.');
  });
  it('rewrites a position report and the sanctions line', () => {
    expect(plainEventText("Ship position for Tanker 'PRIMAVERA' (MMSI: 461000256) in Singapore Approach. Speed: 10.9 knots, Heading: 70°.")).toBe("Tanker 'PRIMAVERA' in Singapore Approach, moving at 10.9 knots.");
    expect(plainEventText("Tanker 'X' under way in Singapore Approach — its MMSI or IMO is on the sanctions list")).toMatch(/the ship is on a sanctions list$/);
  });
  it('leaves a line it does not recognise alone', () => {
    expect(plainEventText('Fed holds rates steady')).toBe('Fed holds rates steady');
    expect(plainEventText('')).toBe('');
  });
});

describe('withoutStaleConfidence', () => {
  it('drops the confidence sentence and keeps the rest', () => {
    expect(withoutStaleConfidence('Tankers are rerouting. Overall confidence 62%. Watch Suez.')).toBe('Tankers are rerouting. Watch Suez.');
    expect(withoutStaleConfidence('Overall confidence: 40.5%')).toBe('');
    expect(withoutStaleConfidence(null)).toBe('');
  });
});

describe('words for scores and sources', () => {
  it('bands the score', () => {
    expect(unusualWords(0.95)).toBe('Very unusual');
    expect(unusualWords(0.78)).toBe('Unusual');
    expect(unusualWords(0.02)).toBe('Routine');
  });
  it('names queues by what they hold', () => {
    expect(plainTopic('events.raw.news')).toBe('News stories waiting to be read');
    expect(plainTopic('some.other.topic')).toBe('some.other.topic');
  });
  it('names known sources and keeps unknown ones', () => {
    expect(plainSource('aisstream')).toBe('ship tracking');
    expect(plainSource('Reuters')).toBe('Reuters');
  });
});
