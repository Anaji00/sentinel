import { describe, expect, it } from 'vitest';
import { groupFeed, splitTitle, type FeedLike } from '../lib/civicFeed';

const f = (kind: string, title: string, ref: string | null = null): FeedLike => ({ kind, title, ref, at: '2026-11-15' });

describe('groupFeed', () => {
  const dec = 'Out-of-state sharing needs signed declarations';
  const items = [f('finding', `${dec} — Round Lake Park PD`, 'a'), f('news', 'EFF to Congress: a headline'), f('finding', `${dec} — West Dundee PD`, 'b'), f('finding', `${dec} — Dekalb PD`, 'c'), f('finding', 'Has a 287(g) agreement — Roman Forest PD', 'd')];
  it('turns three or more of the same finding into one group, keeping the order of first appearance', () => {
    const g = groupFeed(items);
    expect(g.map((e) => e.type)).toEqual(['group', 'single', 'single']);
    const first = g[0];
    expect(first.type === 'group' && first.places).toEqual(['Round Lake Park PD', 'West Dundee PD', 'Dekalb PD']);
  });
  it('leaves pairs and titles without a place alone', () => {
    expect(groupFeed(items.slice(0, 3).filter((i) => i.kind === 'finding'), 3).every((e) => e.type === 'single')).toBe(true);
    expect(splitTitle('No dash here')).toEqual({ lead: 'No dash here', place: null });
  });
  it('never drops an item', () => {
    const count = groupFeed(items).reduce((n, e) => n + (e.type === 'group' ? e.items.length : 1), 0);
    expect(count).toBe(items.length);
  });
});
