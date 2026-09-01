import { NextRequest, NextResponse } from 'next/server';

const API_GATEWAY_URL = process.env.API_GATEWAY_URL || 'http://api-gateway:8000';

// The `state` the gateway minted, parked in a cookie so the callback can prove
// the response it receives belongs to the browser that started the flow.
//
// httpOnly, because nothing in the page needs to read it and a readable state
// cookie is one XSS away from being forgeable. sameSite 'lax' rather than
// 'strict': the browser arrives back at the callback from the identity
// provider's domain, and a strict cookie is not sent on that navigation --
// which would break every sign-in.
export const SSO_STATE_COOKIE = 'sentinel_sso_state';

/**
 * Whether SSO is available, and where to send the browser to use it.
 *
 * GET reports availability so the sign-in page can decide whether to render a
 * button; POST begins an attempt. Both proxy the gateway, because the issuer
 * URL and client credentials belong on the server side of this boundary.
 */
export async function GET() {
  try {
    const upstream = await fetch(`${API_GATEWAY_URL}/api/v1/auth/oidc/status`, {
      cache: 'no-store',
    });
    if (!upstream.ok) return NextResponse.json({ enabled: false });
    return NextResponse.json(await upstream.json());
  } catch {
    // A gateway that cannot be reached is not a gateway with SSO configured.
    // Reporting `enabled: false` renders a sign-in page with a password form,
    // which is the correct fallback; reporting an error renders nothing.
    return NextResponse.json({ enabled: false });
  }
}

export async function POST(req: NextRequest) {
  try {
    const upstream = await fetch(`${API_GATEWAY_URL}/api/v1/auth/oidc/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
      cache: 'no-store',
    });

    if (!upstream.ok) {
      const detail = await upstream.json().catch(() => ({}));
      return NextResponse.json(
        { success: false, error: detail?.detail || 'Single sign-on is unavailable.' },
        { status: upstream.status },
      );
    }

    const { authorization_url, state } = await upstream.json();
    if (!authorization_url || !state) {
      return NextResponse.json(
        { success: false, error: 'Single sign-on is misconfigured.' },
        { status: 503 },
      );
    }

    const response = NextResponse.json({ success: true, authorization_url });
    const isProduction =
      process.env.NODE_ENV === 'production' || process.env.SENTINEL_ENV === 'production';

    response.cookies.set({
      name: SSO_STATE_COOKIE,
      value: state,
      httpOnly: true,
      secure: isProduction || process.env.COOKIE_SECURE !== 'false',
      sameSite: 'lax',
      path: '/',
      // Matches the gateway's own attempt TTL. A state cookie that outlives the
      // attempt it refers to is just a stale value the callback has to reject.
      maxAge: 600,
    });
    return response;
  } catch {
    return NextResponse.json(
      { success: false, error: 'Authentication service unreachable' },
      { status: 503 },
    );
  }
}
