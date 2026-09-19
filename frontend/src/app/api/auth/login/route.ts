import { NextRequest, NextResponse } from 'next/server';
import { signSessionToken } from '@/lib/session';

// Fail closed: no hardcoded secret defaults. SESSION_SECRET must match the API
// gateway's SESSION_SECRET, because the gateway verifies the cookie this route
// mints. Passwords are no longer compared here -- the gateway checks them against
// scrypt hashes in the users table -- so no password secret lives in this process.
const SESSION_SECRET = process.env.SESSION_SECRET;
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'vance@sentinel-quant.io';
const API_GATEWAY_URL = process.env.API_GATEWAY_URL || 'http://api-gateway:8000';

export async function POST(req: NextRequest) {
  try {
    // Fail closed if the signing secret isn't configured — never mint a cookie
    // signed with an undefined/placeholder secret.
    if (!SESSION_SECRET) {
      return NextResponse.json(
        { success: false, error: 'Authentication is not configured' },
        { status: 500 },
      );
    }

    const body = await req.json();
    const { email, password } = body;

    // The master gateway key is deliberately NOT accepted here. It exists for
    // service-to-service calls, and letting it mint a browser session meant
    // anyone who learned or guessed it held an ADMIN account -- with a form on
    // the public sign-in page inviting them to try.
    let isAuthenticated = false;
    let sessionEmail = ADMIN_EMAIL;
    let account: Record<string, unknown> | null = null;

    if (email && password) {
      // Credentials are verified by the gateway against scrypt hashes in the
      // users table. This route used to compare a plaintext ADMIN_PASSWORD held
      // in the environment of a public-facing web process, which allowed exactly
      // one account, could not be rotated per user, and carried no role or tier.
      // The BFF's remaining job is to mint the session cookie on success.
      try {
        const upstream = await fetch(`${API_GATEWAY_URL}/api/v1/auth/login`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password }),
          cache: 'no-store',
        });
        if (upstream.ok) {
          const payload = await upstream.json();
          isAuthenticated = payload?.success === true;
          account = payload?.user ?? null;
          if (account?.email) sessionEmail = String(account.email);
        } else if (upstream.status === 429) {
          // Surface throttling rather than reporting it as a bad password.
          return NextResponse.json(
            { success: false, error: 'Too many sign-in attempts. Try again in a few minutes.' },
            { status: 429 },
          );
        }
      } catch (e) {
        return NextResponse.json(
          { success: false, error: 'Authentication service unreachable' },
          { status: 503 },
        );
      }
    }

    if (!isAuthenticated) {
      return NextResponse.json(
        { success: false, error: 'Invalid corporate credentials or API key' },
        { status: 401 },
      );
    }

    // Issue a 24-hour signed session cookie
    const expiresAt = Date.now() + 24 * 60 * 60 * 1000;
    const sessionRole = String((account?.role as string) || 'ANALYST');
    const sessionToken = signSessionToken(sessionEmail, sessionRole, expiresAt);

    const response = NextResponse.json({
      success: true,
      user: account ?? { email: sessionEmail, role: 'admin' },
    });

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
      maxAge: 24 * 60 * 60, // 24 hours
    });

    return response;
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: 'Authentication service error' },
      { status: 500 },
    );
  }
}
