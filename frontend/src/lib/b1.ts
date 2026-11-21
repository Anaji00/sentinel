/**
 * The traded paper book's make-up (`/portfolio/b1-exposure`): how many names on each side, how much of that was
 * measured, the book's exposure to the stock market itself, and the sectors it leans on. A figure with no measured
 * names behind it is not shown.
 */

export interface Side {
  names: number | null;
  measured: number | null;
  mean_beta: number | null;
  median_vol: number | null;
}

export interface B1Exposure {
  rebalanced_on: string | null;
  scores_as_of: string | null;
  long: Side;
  short: Side;
  net_beta: number | null;
  top_sectors: { long: [string, number][]; short: [string, number][] };
}

/** "Net exposure to the stock market: +0.23, a bit of market exposure". Null when unknown. */
export function netBetaWords(b: number | null | undefined): string | null {
  if (b == null || !Number.isFinite(b)) return null;
  const size = Math.abs(b) < 0.1 ? 'almost none' : Math.abs(b) < 0.4 ? 'a little' : 'a lot';
  return `${b >= 0 ? '+' : '−'}${Math.abs(b).toFixed(2)}: ${size}`;
}

/** One side as a sentence, with the measured count beside any average; a side with nothing measured says so. */
export function sideWords(label: string, s: Side): string {
  const names = s.names ?? 0;
  if (s.measured == null || s.measured <= 0 || s.mean_beta == null) return `${label}: ${names} names, none measured yet.`;
  const vol = s.median_vol != null ? `, typical yearly swing ${(s.median_vol * 100).toFixed(0)}%` : '';
  return `${label}: ${names} names; for the ${s.measured} measured, their average market sensitivity is ${s.mean_beta.toFixed(2)}${vol}.`;
}

/** "Information Technology 12, Financials 9", the largest first, at most `n`. */
export function sectorWords(pairs: [string, number][] | undefined, n = 3): string {
  return [...(pairs ?? [])].sort((a, b) => b[1] - a[1]).slice(0, n).map(([s, k]) => `${s} ${k}`).join(', ');
}
