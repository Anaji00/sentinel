/**
 * Wording for two reports the platform already computes: how the paper book would move in a market
 * shock (/portfolio/exposure) and which oil-route measures are unusual against their own history
 * (/chokepoints/flows). Nothing is derived here beyond turning codes into plain words.
 */

export interface ExposureRow {
  instrument: string;
  shock: string;
  book_move: number;
  names: number | null;
  flag: boolean;
}

export interface ExposureReport {
  generated_at?: string | null;
  rebalanced_on?: string | null;
  names?: number;
  market_beta?: number | null;
  limit?: number;
  flagged?: string[];
  rows?: ExposureRow[];
}

const INSTRUMENT_WORDS: Record<string, string> = {
  'SPY (market)': 'The stock market (S&P 500)',
  US10Y: 'The 10-year Treasury yield',
  US02Y: 'The 2-year Treasury yield',
  US30Y: 'The 30-year Treasury yield',
  WTI: 'Oil (WTI crude)',
  BRENT: 'Oil (Brent crude)',
  GLD: 'Gold',
  DXY: 'The US dollar',
  'GC=F': 'Gold',
  'SI=F': 'Silver',
  'HG=F': 'Copper',
  'CL=F': 'Oil (WTI crude)',
  'BZ=F': 'Oil (Brent crude)',
  'NG=F': 'Natural gas',
  'ZS=F': 'Soybeans',
  'ZC=F': 'Corn',
  'ZW=F': 'Wheat',
  'SB=F': 'Sugar',
};

export const instrumentWords = (i: string): string => INSTRUMENT_WORDS[i] ?? i;

/** "If the stock market falls 5%, the book moves -1.6%", from a row. */
export function shockSentence(r: ExposureRow): string {
  const move = r.book_move * 100;
  const dir = move === 0 ? 'does not move' : `${move > 0 ? 'gains' : 'loses'} ${Math.abs(move).toFixed(2)}%`;
  return `${instrumentWords(r.instrument)} ${r.shock}: the book ${dir}`;
}

/** Rows ordered by how big the book's move is, flagged ones first. */
export function sortExposure(rows: readonly ExposureRow[]): ExposureRow[] {
  return [...rows].sort((a, b) => Number(b.flag) - Number(a.flag) || Math.abs(b.book_move) - Math.abs(a.book_move));
}

export interface OilSeries {
  site: string;
  metric: string;
  day: string;
  value: number;
  n?: number;
  source?: string;
  history_days: number;
  coverage_gap: boolean;
  z: number | null;
}

export interface OilReport {
  generated_at?: string | null;
  series?: OilSeries[];
  episodes?: OilSeries[];
}

const METRIC_WORDS: Record<string, string> = {
  radar_density: 'ships seen by satellite radar',
  dark_events: 'ships switching off their transponder',
  tanker_transits: 'tanker crossings',
  ais_positions: 'ship positions received',
  laden_tanker_transits_west: 'loaded tankers heading west',
};

export const metricWords = (m: string): string => METRIC_WORDS[m] ?? m.replace(/_/g, ' ');

/** `bab_el_mandeb` -> `Bab El Mandeb`. */
export const siteWords = (s: string): string =>
  s
    .replace(/_/g, ' ')
    .trim()
    .replace(/(^|\s)([a-z])/g, (_m, sp: string, ch: string) => `${sp}${ch.toUpperCase()}`);

/** How unusual a reading is, in words, from its z-score against its own history. */
export function unusualWords(z: number | null): string {
  if (z == null) return 'not enough history to compare';
  const a = Math.abs(z);
  const dir = z >= 0 ? 'above' : 'below';
  if (a >= 4) return `far ${dir} its usual level`;
  if (a >= 2) return `well ${dir} its usual level`;
  return `near its usual level`;
}

/** The unusual readings (|z| >= 2), biggest first, with gaps in coverage left out because they are not readings. */
export function unusualOil(report: OilReport | null | undefined, minZ = 2): OilSeries[] {
  return [...(report?.episodes ?? report?.series ?? [])]
    .filter((s) => !s.coverage_gap && s.z != null && Math.abs(s.z) >= minZ)
    .sort((a, b) => Math.abs(b.z as number) - Math.abs(a.z as number));
}
