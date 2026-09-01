import { NextRequest, NextResponse } from 'next/server';
import { signSessionToken } from '../../login/route';
import { SSO_STATE_COOKIE } from '../start/route';

const API_GATEWAY_URL = process.env.API_GATEWAY_URL || 'http://api-gateway:8000';
const SESSION_SECRET = process.env.SESSION_SECRET;

/**
 * Where the identity provider sends the browser back to.
 *
 * Redeems the authorization code through the gateway and, on success, mints the
 * same `sentinel_session` cookie a password login produces -- so every
 * downstream RBAC check stays unaware of how someone signed in.
 *
 * A GET, because this is a browser navigation from the provider's domain, not a
 * fetch from our own page. It therefore ends in a redirect rather than JSON:
 * the person is looking at a page, and JSON in the address bar is not a
 * sign-in experience.
 */
export async function GET(req: NextRequest) {
  const url = new URL(req.url);
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');
  const providerError = url.searchParams.get('error');

  const fail = (reason: string) =>
    NextResponse.redirect(new URL(`/login?sso_error=${encodeURIComponent(reason)}`, req.url));

  // The provider can decline before we ever see a code -- consent refused,
  // account disabled, policy block. That is an answer, not a fault.
  if (providerError) return fail(providerError);

  if (!SESSION_SECRET) return fail('not_configured');
  if (!code || !state) return fail('incomplete_response');

  // The state must match the one this browser was given when it started. The
  // cookie is the second half of the CSRF check: the gateway proves the state
  // is one it issued, and this proves it was issued to this browser.
  const cookieState = req.cookies.get(SSO_STATE_COOKIE)?.value;
  if (!cookieState || cookieState !== state) return fail('state_mismatch');

  let account: Record<string, unknown> | null = null;
  try {
    const upstream = await fetch(`${API_GATEWAY_URL}/api/v1/auth/oidc/callback`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code, state }),
      cache: 'no-store',
    });

    if (!upstream.ok) {
      const detail = await upstream.json().catch(() => ({}));
      // The gateway's messages here are written to be shown to a person --
      // "an account already exists for this address", "this attempt has
      // expired" -- so they are carried through rather than flattened.
      return fail(String(detail?.detail || 'sign_in_failed'));
    }

    const payload = await upstream.json();
    if (payload?.success !== true || !payload?.user?.email) return fail('sign_in_failed');
    account = payload.user;
  } catch {
    return fail('service_unreachable');
  }

  const expiresAt = Date.now() + 24 * 60 * 60 * 1000;
  const sessionToken = signSessionToken(
    String(account!.email),
    String(account!.role || 'VIEWER'),
    expiresAt,
  );

  const response = NextResponse.redirect(new URL('/', req.url));
  const isProduction =
    process.env.NODE_ENV === 'production' || process.env.SENTINEL_ENV === 'production';
  const isSecureCookie = isProduction || process.env.COOKIE_SECURE !== 'false';

  response.cookies.set({
    name: 'sentinel_session',
    value: sessionToken,
    httpOnly: true,
    secure: isSecureCookie,
    sameSite: 'strict',
    path: '/',
    maxAge: 24 * 60 * 60,
  });

  // The attempt is finished; the state cookie has nothing left to authorise.
  response.cookies.set({
    name: SSO_STATE_COOKIE,
    value: '',
    httpOnly: true,
    secure: isSecureCookie,
    sameSite: 'lax',
    path: '/',
    maxAge: 0,
  });

  return response;
}
