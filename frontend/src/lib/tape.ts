/**
 * The live price tape across the top of Markets: the latest reading of each
 * symbol and its change across the readings fetched. Pure, so it is tested
 * without a network.
 */

export interface TapePoint {
  timestamp: string;
  price: number;
}

export interface TapeRow {
  symbol: string;
  price: number;
  changePct: number | null;
}

/** The same request the price charts make, so SWR shares one fetch between them. */
export const TAPE_KEY =
  '/radar/market-series?symbols=SPY,QQQ,DJI,VIX,WTI,BRENT,BTCUSD,ETHUSD,TLT,US30Y,US10Y,US02Y,GLD&limit=60';

/** Symbols in the order the tape shows them. */
export const TAPE_ORDER = ['SPY', 'QQQ', 'DJI', 'VIX', 'WTI', 'BRENT', 'GLD', 'BTCUSD', 'ETHUSD', 'US10Y', 'US02Y'];

export function tapeRows(series: Record<string, TapePoint[]> | undefined, order: string[] = TAPE_ORDER): TapeRow[] {
  if (!series) return [];
  const rows: TapeRow[] = [];
  for (const symbol of order) {
    const pts = (series[symbol] ?? []).filter((p) => Number.isFinite(p.price) && p.price > 0);
    if (!pts.length) continue;
    const first = pts[0].price;
    const last = pts[pts.length - 1].price;
    rows.push({ symbol, price: last, changePct: pts.length > 1 ? ((last - first) / first) * 100 : null });
  }
  return rows;
}
