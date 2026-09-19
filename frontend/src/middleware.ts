import { NextRequest, NextResponse } from 'next/server';

/**
 * Where an unauthenticated request is allowed to go.
 *
 * There was no middleware at all. Every route under `(dashboard)` rendered for
 * anyone who typed the URL: an anonymous visitor got the full command centre,
 * sixteen panels each failing its own 401, a header reading "Not signed in",
 * and no way offered to sign in. The application looked broken rather than
 * closed, which is the worse of the two, because a person cannot tell whether
 * to report an outage or to log in.
 *
 * THIS IS ROUTING, NOT THE SECURITY BOUNDARY.
 *
 * It reads the cookie's payload without verifying the signature, because
 * verifying it needs `node:crypto` and this runs on the edge for every
 * request. A forged cookie gets past this check and reaches a dashboard whose
 * every request is then refused by the BFF proxy, which does verify the
 * signature, and by the gateway behind it. Nothing here grants access to
 * anything; it decides which screen a person is sent to, and the expiry check
 * exists so an obviously-dead session lands on the login page instead of on a
 * dashboard that fails sixteen times and then redirects anyway.
 */

const SESSION_COOKIE = 'sentinel_session';

/** Reachable signed out. Everything else redirects to the sign-in screen. */
const PUBLIC_PATHS = ['/login', '/signup', '/forgot', '/reset', '/verify'];

/** Signed in, these send you on: there is nothing to do on them any more. */
const AUTH_PATHS = ['/login', '/signup'];

function isPublic(pathname: string): boolean {
  return PUBLIC_PATHS.some((p) => pathname === p || pathname.startsWith(`${p}/`));
}

/**
 * Whether the cookie could plausibly still be valid.
 *
 * Payload only: `email:role:expiresAt`, base64url. An unparseable or expired
 * one is treated as absent, which is the useful half of the check -- the
 * common case by far is a cookie that simply ran out overnight.
 */
function looksLive(token: string | undefined): boolean {
  if (!token) return false;
  const [encoded] = token.split('.');
  if (!encoded) return false;
  try {
    const payload = Buffer.from(encoded, 'base64url').toString('utf8');
    const segments = payload.split(':');
    const expiresAt = Number(segments[segments.length - 1]);
    return Number.isFinite(expiresAt) && Date.now() < expiresAt;
  } catch {
    return false;
  }
}

export function middleware(req: NextRequest) {
  const { pathname, search } = req.nextUrl;
  const signedIn = looksLive(req.cookies.get(SESSION_COOKIE)?.value);

  if (signedIn && AUTH_PATHS.some((p) => pathname === p)) {
    // `next` is honoured only as a same-origin path. An absolute URL here
    // would turn the sign-in flow into an open redirect.
    const next = req.nextUrl.searchParams.get('next');
    const safe = next && next.startsWith('/') && !next.startsWith('//') ? next : '/';
    return NextResponse.redirect(new URL(safe, req.url));
  }

  if (!signedIn && !isPublic(pathname)) {
    const url = new URL('/login', req.url);
    // Carry where they were going, so signing in finishes the journey rather
    // than dropping them at the command centre to navigate again.
    url.searchParams.set('next', `${pathname}${search}`);
    url.searchParams.set('reason', 'signin');
    return NextResponse.redirect(url);
  }

  return NextResponse.next();
}

export const config = {
  /**
   * Everything except the API, Next's own assets, and files with an extension.
   *
   * `/api/*` is excluded deliberately: those routes answer with a 401 that the
   * client is built to handle, and redirecting a fetch to an HTML login page
   * would give every panel a parse error instead of a status code.
   */
  matcher: ['/((?!api|_next/static|_next/image|favicon.ico|.*\\.[^/]+$).*)'],
};
