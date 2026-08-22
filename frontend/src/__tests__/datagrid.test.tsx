import React from 'react';
import { describe, it, expect } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { DataGrid } from '../components/ui/DataGrid';
import { ABSENT } from '../lib/format';

const render = (el: React.ReactElement) => renderToStaticMarkup(el);

describe('DataGrid replaces raw JSON rendering', () => {
  it('humanizes snake_case keys into visible labels', () => {
    const html = render(<DataGrid data={{ open_interest: 1200, z_score: 2.7 }} />);

    // The visible label is humanized...
    expect(html).toContain('>Open Interest</dt>');
    expect(html).toContain('>Z Score</dt>');

    // ...while the raw path stays available as a tooltip, so an operator can
    // still cross-reference the field against the API response.
    expect(html).toContain('title="open_interest"');
  });

  it('bounds float precision so 17-digit values cannot reach the panel', () => {
    // The exact value the radar endpoint used to emit.
    const html = render(<DataGrid data={{ z_score: 2.6999999999999997 }} />);
    expect(html).toContain('2.70');
    expect(html).not.toContain('2.6999999999999997');
  });

  it('renders sub-unit values with more resolution than large ones', () => {
    const html = render(<DataGrid data={{ confidence: 0.87654321, notional: 1234.5678 }} />);
    expect(html).toContain('0.8765');
    expect(html).toContain('1,234.57');
  });

  it('formats epoch numbers on temporal keys instead of printing the integer', () => {
    const seconds = Math.floor(Date.UTC(2026, 7, 21, 14, 32, 5) / 1000);
    const html = render(<DataGrid data={{ occurred_at: seconds }} />);
    expect(html).toContain('2026-08-21');
    expect(html).not.toContain(String(seconds));
  });

  it('handles millisecond epochs as well as seconds', () => {
    const ms = Date.UTC(2026, 7, 21, 14, 32, 5);
    const html = render(<DataGrid data={{ updated_at: ms }} />);
    expect(html).toContain('2026-08-21');
  });

  it('leaves identifiers verbatim rather than formatting them as quantities', () => {
    // A CIK rendered as a number would gain thousands separators.
    const html = render(<DataGrid data={{ cik: 1067983, event_id: 90210 }} />);
    expect(html).toContain('1067983');
    expect(html).not.toContain('1,067,983');
  });

  it('renders absence as the em dash, never as zero', () => {
    const html = render(<DataGrid data={{ sharpe: null, missing: undefined, empty: '' }} />);
    expect(html.match(new RegExp(ABSENT, 'g'))?.length).toBe(3);
    expect(html).not.toContain('0.00');
  });

  it('distinguishes a measured zero from an absent value', () => {
    const html = render(<DataGrid data={{ measured: 0 }} />);
    expect(html).toContain('0');
    expect(html).not.toContain(ABSENT);
  });

  it('renders booleans as badges, not as the strings true/false', () => {
    const html = render(<DataGrid data={{ is_synthetic: false, active: true }} />);
    expect(html).toContain('TRUE');
    expect(html).toContain('FALSE');
  });

  it('flattens nested records into dotted paths', () => {
    const html = render(<DataGrid data={{ funding: { rate: 0.0125, venue: 'BINANCE' } }} />);
    expect(html).toContain('Rate');
    expect(html).toContain('Venue');
    expect(html).toContain('funding.rate');
  });

  it('summarizes arrays of records rather than dumping them', () => {
    const html = render(<DataGrid data={{ legs: [{ a: 1 }, { a: 2 }, { a: 3 }] }} />);
    expect(html).toContain('3 records');
    expect(html).not.toContain('{');
  });

  it('renders scalar arrays as compact chips with an overflow count', () => {
    const html = render(<DataGrid data={{ tags: ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'] }} />);
    expect(html).toContain('+2');
  });

  it('omits requested fields', () => {
    const html = render(<DataGrid data={{ keep: 1, raw_payload: { huge: true } }} omit={['raw_payload']} />);
    expect(html).toContain('Keep');
    expect(html).not.toContain('Raw Payload');
  });

  it('shows an explicit empty state instead of a blank panel', () => {
    expect(render(<DataGrid data={{}} />)).toContain('No structured detail available');
    expect(render(<DataGrid data={null} emptyLabel="Nothing reported" />)).toContain('Nothing reported');
  });

  it('never emits a serialized object literal', () => {
    const html = render(
      <DataGrid data={{ nested: { deep: { deeper: { deepest: 1 } } } }} maxDepth={1} />,
    );
    expect(html).not.toContain('[object Object]');
    expect(html).not.toMatch(/\{"/);
  });
});
