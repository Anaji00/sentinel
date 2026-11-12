import { describe, expect, it } from 'vitest';
import { ARM_SECONDS, armedSecondsLeft, describeOrder, orderWarnings, stepAfterEdit, type OrderDraft } from '../lib/orderSteps';

const base: OrderDraft = { side: 'BUY', type: 'MARKET', symbol: 'AAPL', quantity: 10, limitPrice: 0, mark: 200, isLive: false };
const money = (v: number) => `$${v}`;

describe('describeOrder', () => {
  it('names a market order back in words with its cost', () => {
    expect(describeOrder(base, 2000, money)).toBe('Buy 10 AAPL at the market price, about $2000.');
  });
  it('names a limit sell as a floor', () => {
    expect(describeOrder({ ...base, side: 'SELL', type: 'LIMIT', limitPrice: 210 }, null, money)).toBe('Sell 10 AAPL at no worse than $210.');
  });
});

describe('orderWarnings', () => {
  it('is quiet for an ordinary paper market order', () => {
    expect(orderWarnings(base)).toEqual([]);
  });
  it('says the cost is a guess without a price', () => {
    expect(orderWarnings({ ...base, mark: null })[0]).toMatch(/no recent price/);
  });
  it('warns that a buy limit well above the price fills at once', () => {
    const w = orderWarnings({ ...base, type: 'LIMIT', limitPrice: 230 });
    expect(w.join(' ')).toMatch(/15\.0% above.*fill straight away/);
  });
  it('warns that a buy limit well below the price may never fill', () => {
    expect(orderWarnings({ ...base, type: 'LIMIT', limitPrice: 150 }).join(' ')).toMatch(/25\.0% below.*never fill/);
  });
  it('stays quiet when the limit is near the price', () => {
    expect(orderWarnings({ ...base, type: 'LIMIT', limitPrice: 201 })).toEqual([]);
  });
  it('warns on fractions and on the live book', () => {
    const w = orderWarnings({ ...base, quantity: 1.5, isLive: true });
    expect(w).toHaveLength(2);
    expect(w[1]).toMatch(/live book/);
  });
});

describe('the armed window', () => {
  it('counts down and stops at zero', () => {
    expect(armedSecondsLeft(0, 0)).toBe(ARM_SECONDS);
    expect(armedSecondsLeft(0, 4500)).toBe(ARM_SECONDS - 4);
    expect(armedSecondsLeft(0, 60000)).toBe(0);
  });
  it('sends any edit back to the start', () => {
    expect(stepAfterEdit()).toBe('enter');
  });
});
