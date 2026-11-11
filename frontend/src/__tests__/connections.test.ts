import { describe, expect, it } from 'vitest';
import {
  displayName,
  groupHubs,
  layoutRings,
  matchHubs,
  parseEntityGraph,
  parsePath,
  pluralKind,
  relationshipWords,
  strengthWords,
  type RawConnection,
} from '../lib/connections';

const link = (over: Partial<RawConnection>): RawConnection => ({
  source_name: 'Maersk',
  source_type: 'Company',
  relationship: 'OPERATES',
  target_id: 'v1',
  target_name: 'Vessel One',
  target_type: 'Vessel',
  decayed_weight: 0.8,
  last_updated: 1_700_000_000,
  hop_level: 1,
  ...over,
});

describe('names', () => {
  it('tidies shouting identifiers and leaves proper names alone', () => {
    expect(displayName('MORGAN_STANLEY')).toBe('Morgan Stanley');
    expect(displayName('SOUTH CHINA SEA')).toBe('South China Sea');
    expect(displayName('Maersk Line')).toBe('Maersk Line');
    expect(displayName('VSAT')).toBe('VSAT');
    expect(displayName('CACI')).toBe('CACI');
    expect(displayName('0x00ab…12ef')).toBe('0x00ab…12ef');
  });
  it('says a kind in the plural, in words', () => {
    expect(pluralKind('InstitutionalFiler')).toBe('institutional filers');
    expect(pluralKind('Country')).toBe('countries');
    expect(pluralKind('Vessel')).toBe('vessels');
    expect(pluralKind('Index')).toBe('indexes');
  });
});

describe('words', () => {
  it('turns relationship codes into plain words', () => {
    expect(relationshipWords('CORRELATED_WITH')).toBe('correlated with');
    expect(relationshipWords('')).toBe('related to');
    expect(relationshipWords(null)).toBe('related to');
  });
  it('names how firm a link is', () => {
    expect(strengthWords(0.9)).toBe('strong');
    expect(strengthWords(0.3)).toBe('moderate');
    expect(strengthWords(0.01)).toBe('faint');
  });
});

describe('entity view', () => {
  it('is empty when nothing is stored', () => {
    expect(parseEntityGraph(null).nodes).toEqual([]);
    expect(parseEntityGraph({ connections: [] }).center).toBeNull();
    expect(parseEntityGraph({ connections: [link({ target_name: '' })] }).edges).toEqual([]);
  });

  it('builds a centre, its neighbours and typed links', () => {
    const v = parseEntityGraph({ connections: [link({}), link({ target_name: 'Panama', target_type: 'Flag', relationship: 'FLAGGED_AS' })] });
    expect(v.center).toBe('Maersk');
    expect(v.nodes.map((n) => n.name).sort()).toEqual(['Maersk', 'Panama', 'Vessel One']);
    expect(v.nodes.find((n) => n.name === 'Panama')).toMatchObject({ type: 'Flag', hop: 1 });
    expect(v.edges[1].relationship).toBe('flagged as');
    expect(v.fromEvents).toBe(false);
  });

  it('drops repeats and self links, and puts second-hop nodes on the second ring', () => {
    const v = parseEntityGraph({
      connections: [link({}), link({}), link({ target_name: 'Maersk' }), link({ source_name: 'Vessel One', source_type: 'Vessel', target_name: 'Rotterdam', target_type: 'Port', hop_level: 2 })],
    });
    expect(v.edges).toHaveLength(2);
    expect(v.nodes.find((n) => n.name === 'Rotterdam')?.hop).toBe(2);
  });

  it('marks links built from recent events, which carry no source type', () => {
    const v = parseEntityGraph({ connections: [link({ source_type: undefined })] });
    expect(v.fromEvents).toBe(true);
    expect(v.edges[0].stored).toBe(false);
  });
});

describe('layout', () => {
  const view = parseEntityGraph({
    connections: [
      link({}),
      link({ target_name: 'Panama', target_type: 'Flag' }),
      link({ target_name: 'Suez', target_type: 'Region' }),
      link({ source_name: 'Vessel One', target_name: 'Rotterdam', target_type: 'Port', hop_level: 2 }),
    ],
  });

  it('puts the centre in the middle and every node inside the box', () => {
    const pos = layoutRings(view, 1);
    expect(pos.get('Maersk')).toEqual({ x: 0.5, y: 0.5 });
    expect(pos.size).toBe(view.nodes.length);
    for (const p of pos.values()) {
      expect(Number.isFinite(p.x) && Number.isFinite(p.y)).toBe(true);
      expect(p.x).toBeGreaterThan(0.02);
      expect(p.x).toBeLessThan(0.98);
      expect(p.y).toBeGreaterThan(0.02);
      expect(p.y).toBeLessThan(0.98);
    }
  });

  it('is the same every time and puts a second-hop node outside the first ring', () => {
    expect(layoutRings(view)).toEqual(layoutRings(view));
    const pos = layoutRings(view);
    const d = (n: string) => Math.hypot(pos.get(n)!.x - 0.5, pos.get(n)!.y - 0.5);
    expect(d('Rotterdam')).toBeGreaterThan(d('Vessel One'));
  });

  it('has nothing to place without a centre', () => {
    expect(layoutRings(parseEntityGraph(null)).size).toBe(0);
  });
});

describe('paths', () => {
  it('reads a found path with the link between each pair', () => {
    const v = parsePath({
      path: [{ entities: [{ name: 'A' }, { id: 'b' }, { name: 'C' }], relations: [[{}, 'OWNS', {}], { type: 'FLAGGED_AS' }] }],
    });
    expect(v.found).toBe(true);
    expect(v.steps).toEqual([
      { name: 'A', link: 'owns' },
      { name: 'b', link: 'flagged as' },
      { name: 'C', link: null },
    ]);
  });

  it('never invents a route', () => {
    expect(parsePath({ path: [] }).found).toBe(false);
    expect(parsePath(null).found).toBe(false);
    expect(parsePath({ path: [{ entities: [{ name: 'A' }] }] }).found).toBe(false);
  });
});

describe('hubs', () => {
  const hubs = [
    { id: '1', name: 'Maersk', type: 'Company', degree: 40 },
    { id: '2', name: 'Maersk Line', type: 'Company', degree: 12 },
    { id: '3', name: 'Panama', type: 'Flag', degree: 90 },
    { id: '4', name: 'Evergreen', type: 'Company', degree: 5 },
  ];
  it('groups by kind, busiest kind first', () => {
    const g = groupHubs(hubs);
    expect(g.map((x) => x.type)).toEqual(['Flag', 'Company']);
    expect(g[1].hubs.map((h) => h.name)).toEqual(['Maersk', 'Maersk Line', 'Evergreen']);
    expect(groupHubs(hubs, 1)[1].hubs).toHaveLength(1);
  });
  it('matches the start of a name before the middle, busiest first', () => {
    expect(matchHubs(hubs, 'maersk').map((h) => h.name)).toEqual(['Maersk', 'Maersk Line']);
    expect(matchHubs(hubs, 'green').map((h) => h.name)).toEqual(['Evergreen']);
    expect(matchHubs(hubs, '  ')).toEqual([]);
    expect(matchHubs(hubs, 'zzz')).toEqual([]);
  });
});
