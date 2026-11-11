import { describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { DivergenceBrief, StageBadge } from '../components/DivergenceBrief';

const brief = {
  what_happened: 'X +4.00% since the previous close while its industry group +0.50%: a gap of +3.50%.',
  catalyst: 'No news names X alone this session: not measured as news-driven',
  peers: { reference: 'industry', members: ['A +1.0%'], returns_known: true },
  evidence: 'The gap is 3.2 times the usual variation.',
  positioning: 'not measured: no signed options flow is joined to this alert',
  watch: ['Escalating if the gap widens to 4.7 sigma or beyond'],
  limits: ['A price gap is a measurement, not a forecast', 'Stage: new'],
};

describe('divergence rendering', () => {
  it('renders the badge with its meaning', () => {
    const html = renderToStaticMarkup(<StageBadge situation={{ stage: 'ESCALATING' }} />);
    expect(html).toContain('Escalating');
    expect(html).toContain('title="The gap has widened');
    expect(renderToStaticMarkup(<StageBadge situation={null} />)).toBe('');
  });

  it('offers the brief with the situation line as its hint', () => {
    const html = renderToStaticMarkup(<DivergenceBrief id="c1" situation={{ stage: 'CONFIRMED', sessions: 2 }} brief={brief} />);
    // The disclosure loads its body only when opened, so the closed markup carries the label and hint.
    expect(html).toContain('Intelligence brief');
    expect(html).toContain('flagged on 2 sessions');
  });

  it('renders nothing without a brief', () => {
    expect(renderToStaticMarkup(<DivergenceBrief id="c1" brief={null} />)).toBe('');
  });
});
