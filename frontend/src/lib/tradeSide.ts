/**
 * Who initiated a large trade, from the tags enrichment sets. `financial_data.side`
 * is not this: it is empty on blocks and holds CALL or PUT on option prints.
 * Always an inference: the quote rule for options, the tick test for blocks.
 */
export type TradeSide = 'BUY' | 'SELL' | 'UNKNOWN';

export function tradeSide(tags: readonly string[] | null | undefined): TradeSide {
  const t = tags ?? [];
  if (t.includes('buyer_initiated') || t.includes('aggressor_buy')) return 'BUY';
  if (t.includes('seller_initiated') || t.includes('aggressor_sell')) return 'SELL';
  return 'UNKNOWN';
}

export const TRADE_SIDE_LABEL: Record<TradeSide, string> = {
  BUY: 'Buyer',
  SELL: 'Seller',
  UNKNOWN: 'Not known',
};

export function tradeSideMethod(isOption: boolean): string {
  return isOption
    ? 'Inferred from the print against the quote at the time. Most prints cannot be read.'
    : 'Inferred from the trade price against the previous trade.';
}
