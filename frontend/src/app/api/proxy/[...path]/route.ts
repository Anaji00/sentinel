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

// Probes an uptime checker reaches before anyone has signed in. Matched
// exactly, against the whole path.
//
// This was `pathname.includes('/health')` -- a substring test standing in for a
// route test -- and every path containing those seven characters inherited the
// exemption. Reproduced against the running stack with no cookie:
// /api/v1/events/crypto returned 401 and /api/v1/events/health returned 500
// events across every domain, because `health` is not a known domain and fell
// into the unfiltered branch. /api/v1/health/secrets returned the credential
// audit with previews. And it was not read-only: POST /api/v1/cases/health/notes
// returned 422 where /api/v1/cases/abc/notes returned 401 -- FastAPI resolves
// require_role() before it validates a body, so a 422 means the role gate had
// already been passed.
//
// The escalation came from the block below: with no session, the proxy attaches
// the operator's master key, which the gateway resolves to Role.ADMIN. So the
// exemption did not merely skip a check, it upgraded the caller.
//
// Only liveness and readiness belong here. The rest of /api/v1/health --
// /data, /sources, /secrets -- is operational detail about the deployment and
// requires a session like anything else.
const PROBE_PATHS = ['health', 'api/v1/health/liveness', 'api/v1/health/readiness'];

function isProbePath(pathStr: string): boolean {
  return PROBE_PATHS.includes(pathStr.replace(/^\/+|\/+$/g, ''));
}

async function handleProxy(req: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  // Check auth session
  const cookie = req.cookies.get('sentinel_session');
  const isDev = process.env.NODE_ENV !== 'production';

  const { path } = await context.params;
  const pathStr = (path || []).join('/');
  const publicPath = isPublicPath(pathStr);
  const probePath = isProbePath(pathStr);

  if (!publicPath && !probePath) {
    if (!cookie || !cookie.value) {
      if (!isDev) {
        return NextResponse.json({ error: 'Unauthorized session' }, { status: 401 });
      }
    } else {
      const { valid } = verifySessionToken(cookie.value);
      if (!valid && !isDev) {
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
  // A probe is forwarded with no credential, for the same reason a public path
  // is: it must be handled as an anonymous caller. Attaching the master key to
  // an unauthenticated request is what turned the exemption above from a
  // skipped check into a privilege escalation.
  const hasSession = Boolean(cookie?.value && verifySessionToken(cookie.value).valid);
  if (hasSession || publicPath || probePath) {
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
