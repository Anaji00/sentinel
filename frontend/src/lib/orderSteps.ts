/**
 * The order ticket's three steps, as plain functions: what to name back to the person, what to warn about, and when the
 * armed state runs out. Nothing here sizes, caps or sends an order; the gateway still checks the kill switch and records
 * intent before the venue is reached.
 */

export type OrderStep = 'enter' | 'review' | 'armed';

/** Seconds the Confirm button stays live after arming, so a walked-away ticket cannot be fired later by habit. */
export const ARM_SECONDS = 10;

/** How far a limit price may sit from the last mark before the review says so. */
export const LIMIT_WARN_FRACTION = 0.05;

export interface OrderDraft {
  side: 'BUY' | 'SELL';
  type: 'MARKET' | 'LIMIT';
  symbol: string;
  quantity: number;
  limitPrice: number;
  mark: number | null;
  isLive: boolean;
}

/** The order read back as one sentence, in words. */
export function describeOrder(d: OrderDraft, notional: number | null, money: (v: number) => string): string {
  const what = d.side === 'BUY' ? 'Buy' : 'Sell';
  const how = d.type === 'LIMIT' ? `at no worse than ${money(d.limitPrice)}` : 'at the market price';
  const worth = notional !== null ? `, about ${money(notional)}` : '';
  return `${what} ${d.quantity} ${d.symbol} ${how}${worth}.`;
}

/** Things worth a second look before arming. Advice only: none of them blocks the order. */
export function orderWarnings(d: OrderDraft): string[] {
  const out: string[] = [];
  if (d.mark === null) {
    out.push('There is no recent price for this symbol here, so the cost is a guess.');
  } else if (d.type === 'LIMIT') {
    const gap = (d.limitPrice - d.mark) / d.mark;
    if (Math.abs(gap) > LIMIT_WARN_FRACTION) {
      const pct = Math.abs(gap * 100).toFixed(1);
      const above = gap > 0;
      const fillsNow = (d.side === 'BUY') === above;
      out.push(
        `The limit is ${pct}% ${above ? 'above' : 'below'} the last price` +
          (fillsNow ? ', so it will most likely fill straight away at a worse price than you may expect.' : ', so it may never fill.'),
      );
    }
  }
  if (!Number.isInteger(d.quantity)) out.push('The quantity is a fraction of a share; not every symbol accepts that.');
  if (d.isLive) out.push('This is the live book. The order reaches a real venue and cannot be recalled once filled.');
  return out;
}

/** The step to be on after the person changes a field: any edit puts the ticket back at the start. */
export function stepAfterEdit(): OrderStep {
  return 'enter';
}

/** Whole seconds left in the armed window, never below zero. */
export function armedSecondsLeft(armedAtMs: number, nowMs: number): number {
  return Math.max(0, ARM_SECONDS - Math.floor((nowMs - armedAtMs) / 1000));
}
