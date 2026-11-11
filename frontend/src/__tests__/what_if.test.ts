import { describe, expect, it } from 'vitest';
import { isFinished, MAX_TICKERS, parseRequest, problem, statusWords } from '../lib/whatIf';

describe('what-if request', () => {
  it('tidies the topic and reads symbols however they are separated', () => {
    const p = parseRequest('  a   ceasefire in\nUkraine ', 'xom, $lmt;  RTX  xom');
    expect(p.topic).toBe('a ceasefire in Ukraine');
    expect(p.tickers).toEqual(['XOM', 'LMT', 'RTX']);
    expect(p.invalid).toEqual([]);
  });

  it('shows back what is not a symbol instead of dropping it', () => {
    const p = parseRequest('a strike at a port', 'xom, 123abc!!, ');
    expect(p.invalid).toEqual(['123abc!!']);
    expect(problem(p)).toBe('Not a stock symbol: 123abc!!.');
  });

  it('wants a few words of topic, and caps symbols', () => {
    expect(problem(parseRequest('hi', ''))).toBe('Describe the development in a few words.');
    expect(problem(parseRequest('a ceasefire', ''))).toBeNull();
    const many = Array.from({ length: 15 }, (_, i) => `AB${String.fromCharCode(65 + i)}`).join(' ');
    expect(parseRequest('a ceasefire', many).tickers).toHaveLength(MAX_TICKERS);
  });
});

describe('request status', () => {
  it('knows when to stop polling', () => {
    expect(isFinished({ status: 'queued' })).toBe(false);
    expect(isFinished({ status: 'running' })).toBe(false);
    expect(isFinished({ status: 'done' })).toBe(true);
    expect(isFinished({ status: 'failed' })).toBe(true);
    expect(isFinished(undefined)).toBe(false);
  });

  it('says each state in words, and a failure with its reason when there is one', () => {
    expect(statusWords(undefined)).toBe('Sending…');
    expect(statusWords({ status: 'running' })).toContain('a minute or two');
    expect(statusWords({ status: 'failed', error: 'timeout' })).toBe('It could not be written: timeout');
    expect(statusWords({ status: 'failed' })).toContain('Try again');
  });
});
