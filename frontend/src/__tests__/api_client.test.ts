import { describe, it, expect, vi, afterEach } from 'vitest';
import { ApiError, apiClient, describeApiError, fetcher, getBaseUrl } from '../lib/api';

/**
 * These tests import `api.ts`.
 *
 * They previously did not. Both declared a local copy of the logic inside the
 * test body and asserted against the copy -- `getBaseUrl` returning
 * `http://${host}/api/v1` when the real one returns `/api/proxy/api/v1` in the
 * browser, and a `createHeaders` that attached a Bearer token from
 * localStorage, which `api.ts` has never read. Both passed unconditionally and
 * would have kept passing if the module were deleted.
 */
describe('Frontend API Client (api.ts)', () => {
  it('routes browser requests through the session-checking proxy', () => {
    // The BFF at /api/proxy is the only path that verifies the session cookie
    // and refuses to attach the operator's master key. A component fetching
    // /api/v1/... directly reaches a Next.js origin with no such route.
    const realWindow = (globalThis as any).window;
    (globalThis as any).window = { location: { host: 'sentinel.local' } };
    try {
      expect(getBaseUrl()).toBe('/api/proxy/api/v1');
    } finally {
      if (realWindow === undefined) delete (globalThis as any).window;
      else (globalThis as any).window = realWindow;
    }
  });

  it('addresses the gateway directly when there is no browser', () => {
    // SSR and tests have no cookie to forward, so the proxy has nothing to
    // check and the gateway is addressed directly.
    expect(getBaseUrl()).toContain('/api/v1');
    expect(getBaseUrl()).not.toContain('/api/proxy');
  });

  it('sends credentials, because the session lives in a cookie', () => {
    expect(apiClient.defaults.withCredentials).toBe(true);
  });

  it('carries a request timeout', () => {
    // Two endpoints this client polls were measured at 39 and 37 seconds. With
    // no timeout an SWR refresh holds the connection open across refreshes.
    expect(apiClient.defaults.timeout).toBeGreaterThan(0);
  });

  it('does not attach an Authorization header of its own', () => {
    // The old test asserted the opposite, against a function it had written
    // itself. Authentication is the cookie; a Bearer header here would be a
    // second credential path nobody maintains.
    const headers = apiClient.defaults.headers as Record<string, unknown>;
    expect(JSON.stringify(headers)).not.toContain('Authorization');
  });

  it('exports the SWR fetcher the panels bind to', () => {
    expect(typeof fetcher).toBe('function');
  });
});

/**
 * What a panel is told when a request does not work.
 *
 * SWR's `error` is the only channel a component has for "this did not work",
 * and the fetcher fed it a generic `Error` with the status discarded -- so an
 * expired session, a route that does not exist, a 500 from a bad query and an
 * unreachable gateway arrived as one indistinguishable value. That is not
 * hypothetical: `/scenarios?status=CONFIRMED` returned a 500 on every request
 * for as long as the filter existed, and the feed rendered "NO ACTIVE
 * SCENARIOS FOUND" -- which is also what it renders when the platform is
 * healthy and has nothing to say.
 */
describe('fetcher failure reporting', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  const failWith = (status: number | null, detail?: string) => {
    vi.spyOn(apiClient, 'get').mockRejectedValue(
      status === null ? new Error('Network Error') : { response: { status, data: { detail } } },
    );
  };

  it('reports the status the server returned', async () => {
    failWith(500, 'Database query failed');
    await expect(fetcher('/scenarios?status=CONFIRMED')).rejects.toMatchObject({
      name: 'ApiError',
      status: 500,
    });
  });

  it('separates "no such thing" from "could not ask"', async () => {
    failWith(404);
    const missing = await fetcher('/events/detail/nope').catch((e) => e as ApiError);
    expect(missing.isMissing).toBe(true);
    expect(missing.isUnreachable).toBe(false);

    failWith(null);
    const unreachable = await fetcher('/events/detail/nope').catch((e) => e as ApiError);
    expect(unreachable.isUnreachable).toBe(true);
    expect(unreachable.isMissing).toBe(false);
  });

  it('names an expired session as one', async () => {
    failWith(401);
    const err = await fetcher('/portfolio/positions').catch((e) => e as ApiError);
    expect(err.isUnauthenticated).toBe(true);
  });

  it('lets a failed market-series request fail', async () => {
    // This branch returns before the throw at the end of the fetcher, so it
    // was the one endpoint whose failures could never reach a component at
    // all: a caught exception produced `{symbols: [...], series: {}}`, which
    // is a well-formed answer meaning "the platform has no history for these
    // symbols". Four chart components read it and showed "AWAITING LIVE DATA
    // STREAM..." through an outage.
    failWith(503);
    await expect(fetcher('/radar/market-series?symbols=BTCUSD&limit=60')).rejects.toBeInstanceOf(
      ApiError,
    );
  });

  it('still answers an empty market series, which is a different thing', async () => {
    vi.spyOn(apiClient, 'get').mockResolvedValue({ data: { series: {} } } as never);
    const out = await fetcher('/radar/market-series?symbols=BTCUSD&limit=60');
    expect(out).toEqual({ symbols: ['BTCUSD'], series: {} });
  });

  it('mirrors a series across its aliases when the server did answer', async () => {
    const ticks = [{ timestamp: 't', price: 1, volume: null, anomaly_score: 0 }];
    vi.spyOn(apiClient, 'get').mockResolvedValue({
      data: { series: { BTCUSD: ticks } },
    } as never);
    const out = await fetcher('/radar/market-series?symbols=BTC&limit=60');
    expect(out.series.BTC).toEqual(ticks);
    expect(out.series.BTCUSDT).toEqual(ticks);
  });
});

/**
 * The panels that had one empty state for two opposite facts.
 *
 * "AWAITING LIVE DATA STREAM..." was rendered both when the platform was
 * healthy and quiet and when the request had failed. On an intelligence
 * platform the second is the alarming one, and a dashboard that looks calm
 * during an outage is the failure this codebase keeps repeating.
 */
describe('describeApiError', () => {
  it('says nothing when nothing went wrong', () => {
    expect(describeApiError(null)).toBeNull();
    expect(describeApiError(undefined)).toBeNull();
  });

  // Sentence case, not caps: these strings are shown to a reader at the
  // moment something has gone wrong, and the app no longer shouts. What is
  // being asserted is that the four states stay distinguishable.
  it('distinguishes the four things a panel would act on differently', () => {
    expect(describeApiError(new ApiError('/x', null))).toBe('Feed unreachable');
    expect(describeApiError(new ApiError('/x', 401))).toBe('Session expired');
    expect(describeApiError(new ApiError('/x', 404))).toBe('Endpoint not found');
    expect(describeApiError(new ApiError('/x', 500))).toBe('Feed error 500');
  });

  it('still says something for an error it did not throw itself', () => {
    expect(describeApiError(new Error('boom'))).toBe('Feed error');
  });
});
