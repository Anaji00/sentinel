import { describe, expect, it } from 'vitest';
import { gradeFor, agentHeadline, agentLabel, agentVerdict, betaAgentRows, datesNote, familyRows, familyVerdict, horizonHoursWords, type Clustered, type ClusteredAgent, type ClusteredFamily } from '../lib/grades';

const fam = (family: string, k: number, verdict: string, excess: number | null = 0): ClusteredFamily => ({
  family, k, units: 100, dates: 11, mean_ratio_by_date: 1, excess_over_control: excess, t_over_dates: 1, top3_date_share: 0.54, verdict,
});
const agent = (over: Partial<ClusteredAgent>): ClusteredAgent => ({ agent: 'quant_engine', horizon_h: 720, basis: 'excess_beta', calls: 80, dates: 30, mean_signed_bps: -20, t_over_dates: -1.4, top3_date_share: 0.2, verdict: 'no clear edge', ...over });

describe('familyVerdict', () => {
  it('words each verdict against a random stock the same day', () => {
    expect(familyVerdict('moves more than the same-day control')).toEqual({ text: 'Moves more than a random stock the same day', tone: 'good' });
    expect(familyVerdict('moves less than the same-day control').tone).toBe('warn');
    expect(familyVerdict('no difference from the same-day control').tone).toBe('plain');
    expect(familyVerdict('too few dates')).toEqual({ text: 'Too few days to judge', tone: 'quiet' });
  });
  it('shows unknown text as it came', () => {
    expect(familyVerdict('something new')).toEqual({ text: 'something new', tone: 'plain' });
  });
});

describe('datesNote', () => {
  it('says how few days a result rests on', () => {
    expect(datesNote(11, 0.54)).toBe('11 days, 54% of cases on the three busiest');
    expect(datesNote(1, null)).toBe('1 day');
    expect(datesNote(null, 0.5)).toBe('');
  });
});

describe('familyRows', () => {
  const c: Clustered = { rows: [fam('B', 5, 'no difference from the same-day control'), fam('A', 5, 'moves more than the same-day control', 0.43), fam('C', 1, 'moves more than the same-day control'), fam('D', 5, 'too few dates')] };
  it('keeps one horizon, findings first', () => {
    expect(familyRows(c, 5).map((r) => r.family)).toEqual(['A', 'B', 'D']);
  });
  it('handles nothing', () => {
    expect(familyRows(null, 5)).toEqual([]);
  });
});

describe('gradeFor', () => {
  const c: Clustered = { rows: [fam('Peer Cointegration', 5, 'moves more than the same-day control', 0.43), fam('Peer Cointegration', 1, 'no difference from the same-day control')] };
  it('matches the family exactly, at five sessions', () => {
    expect(gradeFor(c, 'Peer Cointegration')?.k).toBe(5);
    expect(gradeFor(c, 'peer cointegration')).toBeNull();
    expect(gradeFor(c, null)).toBeNull();
    expect(gradeFor(undefined, 'x')).toBeNull();
  });
});

describe('agent grades', () => {
  it('keeps only the beta-adjusted basis', () => {
    const c: Clustered = { agents: { rows: [agent({ basis: 'excess_spy' }), agent({ basis: 'excess_beta' })] } };
    expect(betaAgentRows(c)).toHaveLength(1);
    expect(betaAgentRows(null)).toEqual([]);
  });
  it('rewrites only "not judged"', () => {
    expect(agentVerdict('not judged: too few dates').text).toBe('Not judged: too few days');
    expect(agentVerdict('no clear edge').text).toBe('no clear edge');
  });
  it('labels an agent with its variant, and a horizon in words', () => {
    expect(agentLabel(agent({ agent: 'quant_engine' }))).toBe('quant engine');
    expect(agentLabel(agent({ agent: 'stock_correlation_agent.baseline' }))).toBe('stock correlation agent (baseline)');
    expect(horizonHoursWords(24)).toBe('1 day');
    expect(horizonHoursWords(120)).toBe('5 days');
    expect(horizonHoursWords(6)).toBe('6 hours');
    expect(horizonHoursWords(null)).toBe('all horizons');
  });
  it('writes an honest headline', () => {
    expect(agentHeadline([])).toBeNull();
    expect(agentHeadline([agent({ verdict: 'not judged: too few dates' })])).toBe('No agent has been on enough different days to judge yet.');
    expect(agentHeadline([agent({})])).toContain('none shows a measured edge');
    expect(agentHeadline([agent({ mean_signed_bps: 40, t_over_dates: 2.5 })])).toContain('show a measured edge counted by day');
  });
});
