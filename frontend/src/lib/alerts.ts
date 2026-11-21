/**
 * Price alerts (`/alerts`): the shapes, the plain sentence for each alert, and the checks done before asking the server.
 * Alerts are evaluated on the server each time they are read from stored minute bars, so they fire only while someone
 * has Sentinel open; the panel says so.
 */

export interface PriceAlert {
  id: number;
  ticker: string;
  direction: 'above' | 'below';
  level: number;
  note: string | null;
  status: 'active' | 'triggered';
  created_at: string;
  triggered_at: string | null;
  triggered_price: number | null;
  seen_at: string | null;
}

export const MAX_NOTE = 140;

/** A level typed by a person: "170", "170.5" or "$1,170.50". Null when it is not a positive number. */
export function parseLevel(text: string): number | null {
  const cleaned = text.replace(/[$,\s]/g, '');
  if (!/^\d+(\.\d+)?$/.test(cleaned)) return null;
  const v = Number(cleaned);
  return Number.isFinite(v) && v > 0 ? v : null;
}

/** The sentence for an alert, with the price that crossed when it has fired. */
export function alertLine(a: PriceAlert, money: (v: number) => string): string {
  const base = `${a.ticker} ${a.direction === 'above' ? 'rises above' : 'falls below'} ${money(a.level)}`;
  if (a.status === 'triggered') return `${base}: reached ${a.triggered_price != null ? money(a.triggered_price) : 'that level'}.`;
  return `${base}.`;
}

/** A warning when the level has already been passed by the current price: the alert would fire on the next new bar. */
export function alreadyPassed(direction: 'above' | 'below', level: number, price: number | null): string | null {
  if (price == null) return null;
  if (direction === 'above' && price >= level) return 'The price is already above this level, so this will fire on the next new bar.';
  if (direction === 'below' && price <= level) return 'The price is already below this level, so this will fire on the next new bar.';
  return null;
}

/** Alerts that fired and have not been shown yet, oldest first, without any already announced in this browser. */
export function newlyTriggered(list: PriceAlert[] | undefined, announced: ReadonlySet<number>): PriceAlert[] {
  return (list ?? []).filter((a) => a.status === 'triggered' && !a.seen_at && !announced.has(a.id)).sort((x, y) => (x.triggered_at ?? '').localeCompare(y.triggered_at ?? ''));
}
