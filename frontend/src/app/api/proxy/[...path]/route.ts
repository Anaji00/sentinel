import { NextRequest, NextResponse } from 'next/server';
import { verifySessionToken } from '../../auth/login/route';

const BACKEND_URL = process.env.API_GATEWAY_URL || 'http://localhost:8000';
const API_GATEWAY_KEY = process.env.API_GATEWAY_KEY || process.env.NEXT_PUBLIC_API_KEY || '';

async function handleProxy(req: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  // Check auth session
  const cookie = req.cookies.get('sentinel_session');
  const isDev = process.env.NODE_ENV !== 'production';

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

  const { path } = await context.params;
  const pathStr = (path || []).join('/');
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
  const hasSession = Boolean(cookie?.value && verifySessionToken(cookie.value).valid);
  if (hasSession) {
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
