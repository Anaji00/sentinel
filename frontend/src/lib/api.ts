/**
 * @file Centralized API Client & Universal SWR Fetcher
 * Provides full-stack data resilience with CORS-proxied public API fetching
 * (Binance Futures, Yahoo Finance, PolyMarket, CISA KEV) when the backend is offline.
 * STRICT POLICY: 100% authentic live external data or clean "AWAITING LIVE DATA STREAM..." state.
 */

import axios from 'axios';

/** Where the client sends requests.
 *
 * In the browser this must be the BFF at /api/proxy: it is the only path that
 * verifies the session cookie and declines to attach the operator's master key.
 * On the server (SSR, tests) there is no cookie to forward and the gateway is
 * addressed directly.
 *
 * Exported so a test can assert both branches instead of restating the
 * function in its own body, which is what the previous test did.
 */
export const getBaseUrl = () => {
  if (typeof window !== 'undefined') {
    return '/api/proxy/api/v1';
  }
  return process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1';
};

export const apiClient = axios.create({
  baseURL: getBaseUrl(),
  withCredentials: true,
  // A request that cannot finish should fail, not hold a connection open.
  //
  // axios defaults to no timeout, and two endpoints this client polls were
  // measured at 39 and 37 seconds -- on an SWR refresh interval that means
  // overlapping in-flight requests for the same panel. Thirty seconds is long
  // enough for the slow-but-working ones and short enough that a wedged one
  // surfaces as an error rather than as a permanently spinning panel.
  timeout: 30_000,
  headers: {
    'Content-Type': 'application/json',
  },
});

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    const status = error.response?.status;
    const endpoint = error.config?.url || 'API';
    console.warn(
      `[Sentinel API] Endpoint '${endpoint}' returned HTTP status ${status || 'Network/Connection Unavailable'}`,
    );
    return Promise.reject(error);
  },
);

const SYMBOL_YAF_MAP: Record<string, string> = {
  SPX: '^GSPC',
  NDX: '^NDX',
  SPY: '^GSPC',
  QQQ: '^NDX',
  DJI: '^DJI',
  VIX: '^VIX',
  WTI: 'CL=F',
  BRENT: 'BZ=F',
  GLD: 'GC=F',
  US30: '^TYX',
  US30Y: '^TYX',
  US10Y: '^TNX',
  US2Y: '2YY=F',
  US02Y: '2YY=F',
  '2Y': '2YY=F',
  '2YR': '2YY=F',
  TLT: 'TLT',
  SHY: 'SHY',
};

/**
 * Helper to fetch 100% authentic live series directly from public APIs in browser when backend is offline.
 * Uses Coinbase Exchange Candles API for Crypto and Yahoo Finance via AllOrigins proxy for Equities/Yields.
 */

/**
 * Direct Binance Futures REST API fetcher for Crypto Derivatives, Funding Rates, & OI
 */

/**
 * Direct CISA Known Exploited Vulnerabilities (KEV) Catalog fetcher for Cyber Intelligence
 */

/**
 * Direct PolyMarket API fetcher for Prediction Market Probability Radar
 */

/**
 * Universal data fetcher used by SWR.
 * Automatically handles duplicate /api/v1 prefixes and falls back to authentic public APIs.
 */
/** A request that did not succeed, with what the server said about it.
 *
 *  The fetcher used to collapse every failure to `data = null` and then throw
 *  `new Error("API fetch failed for /scenarios")`. A panel could tell that
 *  something went wrong and nothing else: an expired session, a route that
 *  does not exist, a 500 from a bad query and an unreachable gateway were one
 *  indistinguishable value. `/scenarios?status=CONFIRMED` was returning a 500
 *  on every request -- see the Cypher-in-PostgreSQL defect -- and the feed
 *  rendered "NO ACTIVE SCENARIOS FOUND", which is what it also renders when
 *  the platform is healthy and has nothing to say. */
export class ApiError extends Error {
  readonly status: number | null;
  readonly url: string;
  constructor(url: string, status: number | null, detail?: string) {
    super(
      status === null
        ? `Could not reach the API for ${url}`
        : `API returned ${status} for ${url}${detail ? `: ${detail}` : ''}`,
    );
    this.name = 'ApiError';
    this.status = status;
    this.url = url;
  }
  /** The server answered, and its answer was "there is no such thing". */
  get isMissing(): boolean {
    return this.status === 404;
  }
  /** Nobody is signed in, or the session expired. */
  get isUnauthenticated(): boolean {
    return this.status === 401 || this.status === 403;
  }
  /** The request never got an answer at all. */
  get isUnreachable(): boolean {
    return this.status === null;
  }
}

/** A short phrase for why a panel has nothing, or null if it simply has nothing.
 *
 *  Panels across this app render one empty state -- "AWAITING LIVE DATA
 *  STREAM..." -- for both "the platform is healthy and quiet" and "the request
 *  failed". Those are opposite facts about the system, and on an intelligence
 *  platform the second one is the alarming one: a dashboard that looks calm
 *  during an outage is the failure mode this whole codebase keeps repeating.
 */
export function describeApiError(err: unknown): string | null {
  if (!err) return null;
  if (err instanceof ApiError) {
    if (err.isUnreachable) return 'Feed unreachable';
    if (err.isUnauthenticated) return 'Session expired';
    if (err.isMissing) return 'Endpoint not found';
    return `Feed error ${err.status}`;
  }
  return 'Feed error';
}

export const fetcher = async (url: string) => {
  // Prevent duplicate /api/v1/api/v1 prefix errors if component passed full /api/v1 path
  const normalizedUrl = url.startsWith('/api/v1/') ? url.replace('/api/v1/', '/') : url;

  let data: any = null;
  try {
    const res = await apiClient.get(normalizedUrl);
    data = res.data;
  } catch (err: any) {
    // Thrown, not swallowed, and carrying the status.
    //
    // Every caller of this fetcher is an SWR hook, and SWR's `error` is the
    // only channel a component has for "this did not work". Returning null
    // here meant that channel was fed a generic Error with the status
    // discarded -- and for /radar/market-series, which returns below before
    // reaching the throw, it was never fed anything at all.
    const status = err?.response?.status ?? null;
    const detail = err?.response?.data?.detail;
    throw new ApiError(normalizedUrl, status, typeof detail === 'string' ? detail : undefined);
  }

  // Handle market-series endpoint
  if (normalizedUrl.includes('/radar/market-series')) {
    const dummyUrl = new URL(normalizedUrl, 'http://localhost:8000/api/v1');
    const symbolsParam = dummyUrl.searchParams.get('symbols');
    if (!symbolsParam) return { symbols: [], series: {} };
    const targetSymbols = symbolsParam
      .split(',')
      .map((s: string) => s.trim().toUpperCase())
      .filter(Boolean);
    const limit = parseInt(dummyUrl.searchParams.get('limit') || '60', 10);

    // `data` is a real response by the time control reaches here: a failed
    // request threw above. An empty `series` therefore means the platform has
    // no history for these symbols, which is a different statement from "the
    // request did not work" -- and this branch used to make them the same, by
    // returning this shape from a caught exception.
    const seriesData: Record<string, any[]> = data?.series || {};

    // No external substitution.
    //
    // Symbols the platform has no series for are left absent. This used to
    // fetch Coinbase and Yahoo (the latter through api.allorigins.win and
    // corsproxy.io, two unaffiliated CORS proxies) directly from the browser
    // and render the result in the same components, with no badge and
    // anomaly_score 0.0 -- so rows that never passed through enrichment,
    // scoring, correlation or the audit ledger sat beside rows that did, and a
    // backend outage looked like a working dashboard.

    // Mirror ticks across all symbol aliases so every chart component finds its requested ticker key
    const aliasMap: Record<string, string[]> = {
      BTCUSD: ['BTC', 'BTCUSDT'],
      BTC: ['BTCUSD', 'BTCUSDT'],
      ETHUSD: ['ETH', 'ETHUSDT'],
      ETH: ['ETHUSD', 'ETHUSDT'],
      US30Y: ['US30', '30YR', '30Y'],
      US30: ['US30Y', '30YR', '30Y'],
      '30YR': ['US30Y', 'US30', '30Y'],
      US02Y: ['US2Y', '2YR', '2Y'],
      US2Y: ['US02Y', '2YR', '2Y'],
      '2YR': ['US02Y', 'US2Y', '2Y'],
    };

    Object.keys(seriesData).forEach((key) => {
      const aliases = aliasMap[key] || [];
      aliases.forEach((aliasKey) => {
        if (!seriesData[aliasKey] || seriesData[aliasKey].length === 0) {
          seriesData[aliasKey] = seriesData[key];
        }
      });
    });

    return {
      symbols: targetSymbols,
      series: seriesData,
    };
  }

  // A 200 carrying a literal null body. Rare, and not a success.
  if (data === null) {
    throw new ApiError(normalizedUrl, 200, 'empty response body');
  }

  return data;
};
