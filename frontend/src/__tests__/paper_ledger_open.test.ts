import { describe, it, expect } from 'vitest';
import { onlyStopsSoFar } from '../components/TrackRecord';

const s = (n: number) => ({ n, hit_rate: null, mean_bps: null, t: null });
const row = (n: number, stopped: number, open: number) => ({
  ...s(n),
  gross: s(n),
  pnl_usd: 0,
  pnl_scaled_usd: 0,
  mean_cost_bps: null,
  stopped,
  open_marked: s(open),
});

describe('paper ledger while a horizon is young', () => {
  it('flags a horizon whose only exits so far are stops, with trades still open', () => {
    expect(onlyStopsSoFar(row(107, 107, 49))).toBe(true);
  });

  it('does not flag a horizon that has closed trades at their end, or has nothing open', () => {
    expect(onlyStopsSoFar(row(171, 98, 47))).toBe(false);
    expect(onlyStopsSoFar(row(5, 5, 0))).toBe(false);
    expect(onlyStopsSoFar(undefined)).toBe(false);
  });
});

import { autoTradeLine, type AutoTrade } from '../components/AutoTradePanel';

const auto = (over: Partial<AutoTrade>): AutoTrade => ({
  enabled: true,
  venue: 'Alpaca paper',
  account: null,
  positions: [],
  orders: [],
  days: [],
  ...over,
});

describe('auto-trader status in words', () => {
  it('says when it is off, waiting, or has traded', () => {
    expect(autoTradeLine(auto({ enabled: false }))).toMatch(/^Off/);
    expect(autoTradeLine(auto({}))).toMatch(/next market open/);
    expect(autoTradeLine(auto({ days: [{ day: '2026-10-23', sent: 1, rejected: 2 }] }))).toBe(
      'On: last traded 2026-10-23, 1 order sent, 2 refused by the broker.',
    );
  });
});
