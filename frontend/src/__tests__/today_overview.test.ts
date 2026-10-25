import { describe, it, expect } from 'vitest';
import { greeting, signedPct, sparkPoints } from '../components/TodayOverview';
import { decodeEntities } from '../lib/format';

describe('today', () => {
  it('greets by the reader\'s hour', () => {
    expect(greeting(8)).toBe('Good morning');
    expect(greeting(14)).toBe('Good afternoon');
    expect(greeting(21)).toBe('Good evening');
    expect(greeting(2)).toBe('Working late');
  });

  it('signs percentages and keeps a missing figure missing', () => {
    expect(signedPct(1.234)).toBe('+1.23%');
    expect(signedPct(-0.4)).toBe('-0.40%');
    expect(signedPct(null)).toBe('—');
    expect(signedPct(Number.NaN)).toBe('—');
  });

  it('scales a sparkline into its box, top is high', () => {
    expect(sparkPoints([1, 2], 1, 2, 100, 10)).toBe('0.0,10.0 100.0,0.0');
    expect(sparkPoints([5, 5], 5, 5, 10, 10)).toBe('0.0,10.0 10.0,10.0');
  });
});

describe('feed text', () => {
  it('decodes escaped headlines to plain text without parsing HTML', () => {
    expect(decodeEntities('don&#39;t')).toBe("don't");
    expect(decodeEntities('AT&amp;T &quot;x&quot; &#x2014; ok')).toBe('AT&T "x" — ok');
    expect(decodeEntities('&lt;script&gt;')).toBe('<script>');
    expect(decodeEntities('&bogus; &#0;')).toBe('&bogus; &#0;');
  });
});
