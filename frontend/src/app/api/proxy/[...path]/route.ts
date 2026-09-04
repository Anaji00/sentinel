import { NextRequest, NextResponse } from 'next/server';
import { verifySessionToken } from '../../auth/login/route';

// Its three sibling handlers (auth/login, auth/sso/start, auth/sso/callback)
// all default to the gateway; this one defaulted to localhost, which inside the
// frontend container is the frontend. With API_GATEWAY_URL unset, sign-in would
// keep working through the three that name the gateway correctly and every data
// request would fail against this container -- an authentication success
// followed by an empty product, which is the hardest shape to diagnose.
const BACKEND_URL = process.env.API_GATEWAY_URL || 'http://api-gateway:8000';
const API_GATEWAY_KEY = process.env.API_GATEWAY_KEY || process.env.NEXT_PUBLIC_API_KEY || '';

// Endpoints a person reaches before they have a session: creating an account,
// confirming an address, recovering a password, registering interest in the paid
// tier. Requiring a session here would make signup impossible. Each of these is
// public and per-source throttled on the gateway, so forwarding them
// unauthenticated is exactly what should happen.
const PUBLIC_PATHS = [
  'api/v1/auth/signup',
  'api/v1/auth/login',
  'api/v1/auth/verify',
  'api/v1/auth/resend-verification',
  'api/v1/auth/forgot-password',
  'api/v1/auth/reset-password',
  'api/v1/billing/waitlist',
];

function isPublicPath(pathStr: string): boolean {
  const clean = pathStr.replace(/^\/+|\/+$/g, '');
  return PUBLIC_PATHS.includes(clean);
}

async function handleProxy(req: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  // Check auth session
  const cookie = req.cookies.get('sentinel_session');
  const isDev = process.env.NODE_ENV !== 'production';

  const { path } = await context.params;
  const pathStr = (path || []).join('/');
  const publicPath = isPublicPath(pathStr);

  if (!publicPath) {
    if (!cookie || !cookie.value) {
      if (!isDev && !req.nextUrl.pathname.includes('/health')) {
        return NextResponse.json({ error: 'Unauthorized session' }, { status: 401 });
      }
    } else {
      const { valid } = verifySessionToken(cookie.value);
      if (!valid && !isDev && !req.nextUrl.pathname.includes('/health')) {
        return NextResponse.json({ error: 'Invalid or expired session' }, { status: 401 });
      }
    }
  }
  const search = req.nextUrl.search;
  const targetUrl = `${BACKEND_URL.replace(/\/+$/, '')}/${pathStr}${search}`;

  const headers = new Headers(req.headers);
  headers.delete('host');

  // Forward the caller's own session rather than the operator's master key.
  // Attaching X-API-KEY unconditionally made every signed-in visitor
  // indistinguishable from the operator at the gateway -- ADMIN role, and
  // exempt from any subscription gate, because the gateway checks the API key
  // before the session cookie. The cookie is already a credential the gateway
  // accepts, so it is what should identify the user. The master key is used
  // only where there is no session to forward (health checks and similar).
  // A public path is forwarded with no credential at all: it must be handled as
  // an anonymous caller, never as the operator.
  const hasSession = Boolean(cookie?.value && verifySessionToken(cookie.value).valid);
  if (hasSession || publicPath) {
    headers.delete('X-API-KEY');
    headers.delete('x-api-key');
  } else {
    headers.set('X-API-KEY', API_GATEWAY_KEY);
  }

  try {
    const body = ['GET', 'HEAD'].includes(req.method) ? undefined : await req.arrayBuffer();
    const res = await fetch(targetUrl, {
      method: req.method,
      headers,
      body,
      cache: 'no-store',
    });

    const responseHeaders = new Headers(res.headers);
    responseHeaders.delete('content-encoding');

    const data = await res.arrayBuffer();
    return new NextResponse(data, {
      status: res.status,
      statusText: res.statusText,
      headers: responseHeaders,
    });
  } catch (err: any) {
    return NextResponse.json(
      { error: 'Backend gateway proxy error', message: err.message },
      { status: 502 }
    );
  }
}

export const GET = handleProxy;
export const POST = handleProxy;
export const PUT = handleProxy;
export const DELETE = handleProxy;
export const PATCH = handleProxy;
