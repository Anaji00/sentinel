/**
 * Surveillance companies that trade on a US exchange, and the names each goes
 * by in the civic data: federal contracts (`money`), mapped devices (`maker`)
 * and lobbying filings (`lobby`). The bridge between the platform's two halves:
 * the companies paid to watch the public are also stocks a reader can look up.
 *
 * Privately held vendors (Flock Safety, Dataminr, Clearview AI, Skydio, Babel
 * Street, Genetec...) have no ticker and are deliberately absent.
 */

export interface SurveillanceCompany {
  ticker: string;
  name: string;
  /** What it sells to police and government, in a phrase. */
  sells: string;
  money?: string;
  maker?: string;
  lobby?: string;
}

export const SURVEILLANCE_COMPANIES: SurveillanceCompany[] = [
  {
    ticker: 'PLTR',
    name: 'Palantir',
    sells: 'data platforms for police, immigration enforcement and the military',
    money: 'Palantir Technologies',
    lobby: 'Palantir',
  },
  {
    ticker: 'AXON',
    name: 'Axon',
    sells: 'Tasers, body cameras, and the evidence and drone systems behind them',
    money: 'Axon Enterprise',
    maker: 'Axon Enterprise',
    lobby: 'Axon',
  },
  {
    ticker: 'MSI',
    name: 'Motorola Solutions',
    sells: 'plate readers (Vigilant), in-car cameras (WatchGuard) and police radio and software',
    maker: 'Motorola Solutions',
    lobby: 'Motorola Solutions',
  },
  {
    ticker: 'CLBT',
    name: 'Cellebrite',
    sells: 'tools that break into and copy phones',
    money: 'Cellebrite',
    lobby: 'Cellebrite',
  },
  {
    ticker: 'SSTI',
    name: 'SoundThinking',
    sells: 'ShotSpotter gunshot microphones and police analytics',
    money: 'SoundThinking',
    lobby: 'SoundThinking',
  },
  { ticker: 'REKR', name: 'Rekor', sells: 'plate readers and roadway analytics', maker: 'Rekor', lobby: 'Rekor' },
  { ticker: 'RELX', name: 'RELX (LexisNexis Risk)', sells: 'people-search and data-broker records sold to police', lobby: 'LexisNexis Risk' },
  { ticker: 'LHX', name: 'L3Harris', sells: 'cell-site simulators ("Stingrays") that pose as phone towers' },
];

const BY_TICKER = new Map(SURVEILLANCE_COMPANIES.map((c) => [c.ticker, c]));

/** The listed company behind a name as the civic data spells it, if any. */
export function companyForVendor(vendor: string | null | undefined): SurveillanceCompany | null {
  if (!vendor) return null;
  const v = vendor.trim().toLowerCase();
  for (const c of SURVEILLANCE_COMPANIES) {
    const names = [c.name, c.money, c.maker, c.lobby].filter(Boolean).map((x) => String(x).toLowerCase());
    if (names.includes(v)) return c;
  }
  // Subsidiaries and old names the agency records still use.
  if (/vigilant|watchguard/.test(v)) return BY_TICKER.get('MSI') ?? null;
  if (/shotspotter/.test(v)) return BY_TICKER.get('SSTI') ?? null;
  if (/^harris\b|l3harris/.test(v)) return BY_TICKER.get('LHX') ?? null;
  if (/lexisnexis/.test(v)) return BY_TICKER.get('RELX') ?? null;
  return null;
}

export function companyForTicker(ticker: string | null | undefined): SurveillanceCompany | null {
  return ticker ? (BY_TICKER.get(ticker.trim().toUpperCase()) ?? null) : null;
}
