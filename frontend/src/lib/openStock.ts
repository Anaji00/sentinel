/**
 * Opening a stock: the address that puts its drawer over the current page.
 *
 * `?t=TICKER` is read by the drawer on every dashboard page. The page's other
 * parameters (a view, a window) are kept, so opening a stock never loses your place.
 */

/** A ticker as the drawer accepts it: letters, digits, dot and dash, at most ten. */
export function cleanTicker(raw: string | null | undefined): string {
  return (raw || '').toUpperCase().replace(/[^A-Z0-9.-]/g, '').slice(0, 10);
}

/** True for something that could be a ticker: it starts with a letter or digit. */
export function looksLikeTicker(raw: string | null | undefined): boolean {
  return /^[A-Z0-9][A-Z0-9.-]*$/.test(cleanTicker(raw));
}

/** `/calls?view=signals&t=NVDA` for `("/calls", "?view=signals", "nvda")`. */
export function stockHref(pathname: string, search: string, ticker: string): string {
  const params = new URLSearchParams(search.startsWith('?') ? search.slice(1) : search);
  params.set('t', cleanTicker(ticker));
  return `${pathname || '/'}?${params.toString()}`;
}
