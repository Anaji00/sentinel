import { describe, expect, it } from 'vitest';
import { NOT_PROVIDED, briefBlocks, horizonWords, isNotMeasured, measuresLines, situationLine, stageView } from '../lib/divergence';

const brief = {
  what_happened: 'X +4.00% while its industry group +0.50%: a gap of +3.50%.',
  catalyst: 'No news names X alone this session: not measured as news-driven',
  peers: { reference: 'industry', members: ['A +1.0%', 'B +0.2%'], returns_known: true },
  evidence: 'The gap is 3.2 times the usual variation.',
  positioning: 'not measured: no signed options flow is joined to this alert',
  watch: ['Escalating if the gap widens', 'Resolved if it closes'],
  limits: ['A price gap is a measurement, not a forecast', 'Stage: new'],
};

describe('stage', () => {
  it('names each stage with a tone and a plain sentence', () => {
    // A new, unconfirmed situation is the common case, so it stays quiet; colour is for confirmed and escalating ones.
    expect(stageView('DEVELOPING')).toMatchObject({ word: 'Developing', tone: 'muted' });
    expect(stageView('CONFIRMED')?.word).toBe('Confirmed');
    expect(stageView('ESCALATING')?.tone).toBe('negative');
    expect(stageView('RESOLVED')?.tone).toBe('muted');
  });

  it('badges nothing for an unknown or absent stage', () => {
    expect(stageView('MADE_UP')).toBeNull();
    expect(stageView(undefined)).toBeNull();
    expect(stageView(null)).toBeNull();
  });

  it('says how long it has run from the measurements', () => {
    expect(situationLine({ stage: 'CONFIRMED', sessions: 2, peak_z: 3.2, last_z: 3, first_session: '2026-11-09', last_session: '2026-11-10' })).toBe(
      'flagged on 2 sessions, peak gap 3.2 standard deviations, latest 3, 2026-11-09 to 2026-11-10',
    );
    expect(situationLine({ stage: 'DEVELOPING', sessions: 1 })).toBe('flagged on 1 session');
    expect(situationLine({ stage: 'DEVELOPING' })).toBeNull();
    expect(situationLine(null)).toBeNull();
  });
});

describe('brief', () => {
  it('is seven labelled blocks in the fixed order', () => {
    const blocks = briefBlocks(brief);
    expect(blocks.map((b) => b.key)).toEqual(['what_happened', 'catalyst', 'peers', 'evidence', 'positioning', 'watch', 'limits']);
    expect(blocks).toHaveLength(7);
    expect(blocks[0].label).toBe('What happened');
  });

  it('keeps "not measured" text exactly as sent', () => {
    const blocks = briefBlocks(brief);
    expect(blocks[4].lines).toEqual(['not measured: no signed options flow is joined to this alert']);
    expect(blocks[1].lines[0]).toContain('not measured as news-driven');
    expect(isNotMeasured(blocks[4].lines[0])).toBe(true);
    expect(isNotMeasured(blocks[0].lines[0])).toBe(false);
  });

  it('shows lists as items and peers with their reference', () => {
    const blocks = briefBlocks(brief);
    expect(blocks[5].lines).toEqual(['Escalating if the gap widens', 'Resolved if it closes']);
    expect(blocks[2].lines).toEqual(['Compared with its industry group.', 'A +1.0%, B +0.2%']);
    expect(briefBlocks({ ...brief, peers: { reference: 'peers', members: ['C'], returns_known: false } })[2].lines.at(-1)).toContain('not available');
  });

  it('says a missing section is not provided, never leaves it out or invents it', () => {
    const blocks = briefBlocks({ what_happened: 'x' });
    expect(blocks).toHaveLength(7);
    expect(blocks[1].lines).toEqual([NOT_PROVIDED]);
    expect(blocks[2].lines).toEqual([NOT_PROVIDED]);
    expect(blocks[5].lines).toEqual([NOT_PROVIDED]);
    expect(briefBlocks(null)).toEqual([]);
  });
});

describe('horizon and measurements', () => {
  it('says the window in words and nothing for an unknown one', () => {
    expect(horizonWords('session')).toBe('today so far');
    expect(horizonWords('5d')).toBe('over 5 sessions');
    expect(horizonWords('decade')).toBeNull();
    expect(horizonWords(undefined)).toBeNull();
  });

  it('turns each measurement into a plain sentence and leaves absent ones out', () => {
    const lines = measuresLines({ horizon: 'session', breadth: 0.62, beta: 1.1, relative_volume: 1.84 });
    expect(lines).toEqual([
      '62% of its group moved the other way, so most of the group disagrees with this stock.',
      'Beta 1.10: for each 1% its group moves, this stock has usually moved about 1.10%.',
      'Trading 1.8 times its usual volume for this time of day.',
    ]);
    expect(measuresLines({ breadth: 0.2 })[0]).toContain('mostly one stock');
    expect(measuresLines({ beta: 0.9, relative_volume: null })).toHaveLength(1);
    expect(measuresLines(null)).toEqual([]);
    expect(measuresLines({ breadth: Number.NaN })).toEqual([]);
  });
});

import { ALERT_KINDS, filterAlerts, isDivergence } from '../lib/divergence';

describe('alert kinds', () => {
  const alerts = [
    { id: 1, tags: ['divergence', 'horizon:5d'] },
    { id: 2, tags: ['MACRO'] },
    { id: 3, tags: null, situation: { stage: 'CONFIRMED' } },
    { id: 4 },
  ];

  it('knows a divergence by its tag or its situation', () => {
    expect(alerts.map(isDivergence)).toEqual([true, false, true, false]);
    expect(isDivergence({ tags: ['Divergence'] })).toBe(true);
  });

  it('filters into everything, stock-vs-peers and the rest, without losing any', () => {
    expect(filterAlerts(alerts, 'all')).toHaveLength(4);
    expect(filterAlerts(alerts, 'divergence').map((a) => a.id)).toEqual([1, 3]);
    expect(filterAlerts(alerts, 'other').map((a) => a.id)).toEqual([2, 4]);
    expect(ALERT_KINDS.map((k) => k.id)).toEqual(['all', 'divergence', 'other']);
  });
});
