import { describe, it, expect } from 'vitest';
import { badgeText, newUnread, popups, type NotificationItem } from '../lib/notifications';

const item = (id: number, read = false): NotificationItem => ({
  id, read, kind: 'call', ticker: 'A', title: `t${id}`, body: 'b', url: '/calls?t=A', created_at: '2026-11-04T09:00:00Z',
});

describe('the bell', () => {
  it('announces nothing on the first check, however much is unread', () => {
    expect(newUnread([item(1), item(2)], null)).toEqual([]);
  });

  it('announces only unread rows not seen before', () => {
    const fresh = newUnread([item(1), item(2), item(3, true)], new Set([1]));
    expect(fresh.map((n) => n.id)).toEqual([2]);
  });

  it('turns a burst into one summary pop-up', () => {
    expect(popups([item(1), item(2)]).map((p) => p.title)).toEqual(['t1', 't2']);
    const many = popups([item(1), item(2), item(3), item(4)]);
    expect(many).toHaveLength(1);
    expect(many[0].title).toBe('4 new updates on stocks you follow');
  });

  it('caps the badge', () => {
    expect(badgeText(7)).toBe('7');
    expect(badgeText(120)).toBe('99+');
  });
});
