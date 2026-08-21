import { NextRequest, NextResponse } from 'next/server';
import crypto from 'crypto';

// Admin authentication secret and password configuration.
// Fail closed: no hardcoded secret defaults. SESSION_SECRET must match the API
// gateway's SESSION_SECRET (the gateway verifies the cookie this route mints), and
// ADMIN_PASSWORD must be injected from a real secret store. If either is unset the
// login endpoint refuses to authenticate rather than trusting a public default.
const SESSION_SECRET = process.env.SESSION_SECRET;
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'vance@sentinel-quant.io';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;

function signSessionToken(email: string, expiresAt: number): string {
  const payload = `${email}:${expiresAt}`;
  const hmac = crypto.createHmac('sha256', SESSION_SECRET as string).update(payload).digest('hex');
  return `${Buffer.from(payload).toString('base64url')}.${hmac}`;
}

export function verifySessionToken(token: string): { valid: boolean; email?: string } {
  try {
    if (!SESSION_SECRET) return { valid: false };
    const [encodedPayload, signature] = token.split('.');
    if (!encodedPayload || !signature) return { valid: false };

    const payload = Buffer.from(encodedPayload, 'base64url').toString('utf8');
    const [email, expiresAtStr] = payload.split(':');
    const expiresAt = parseInt(expiresAtStr, 10);

    if (isNaN(expiresAt) || Date.now() > expiresAt) return { valid: false };

    const expectedHmac = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
    if (!crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expectedHmac))) {
      return { valid: false };
    }

    return { valid: true, email };
  } catch (e) {
    return { valid: false };
  }
}

export async function POST(req: NextRequest) {
  try {
    // Fail closed if the signing secret isn't configured — never mint a cookie
    // signed with an undefined/placeholder secret.
    if (!SESSION_SECRET) {
      return NextResponse.json(
        { success: false, error: 'Authentication is not configured' },
        { status: 500 }
      );
    }

    const body = await req.json();
    const { email, password, apiKey } = body;

    const gatewayKey = process.env.API_GATEWAY_KEY || process.env.NEXT_PUBLIC_API_KEY || '';

    let isAuthenticated = false;

    if (apiKey) {
      // Only accept an API-key login when a gateway key is actually configured,
      // so an unset key can never match an empty-string submission.
      if (gatewayKey && apiKey === gatewayKey) {
        isAuthenticated = true;
      }
    } else if (email && password && ADMIN_PASSWORD) {
      const emailMatch = email.trim().toLowerCase() === ADMIN_EMAIL.toLowerCase();
      const passwordMatch = password === ADMIN_PASSWORD;
      if (emailMatch && passwordMatch) {
        isAuthenticated = true;
      }
    }

    if (!isAuthenticated) {
      return NextResponse.json(
        { success: false, error: 'Invalid corporate credentials or API key' },
        { status: 401 }
      );
    }

    // Issue a 24-hour signed session cookie
    const expiresAt = Date.now() + 24 * 60 * 60 * 1000;
    const sessionToken = signSessionToken(ADMIN_EMAIL, expiresAt);

    const response = NextResponse.json({
      success: true,
      user: { email: ADMIN_EMAIL, role: 'admin' },
    });

    const isProduction = process.env.NODE_ENV === 'production' || process.env.SENTINEL_ENV === 'production';
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
      { status: 500 }
    );
  }
}
