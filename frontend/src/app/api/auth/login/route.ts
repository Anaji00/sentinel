import { NextRequest, NextResponse } from 'next/server';
import crypto from 'crypto';

// Admin authentication secret and password hash configuration
const SESSION_SECRET = process.env.SESSION_SECRET || 'sentinel-secure-session-secret-key-2026';
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'vance@sentinel-quant.io';

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'sentinel2026!';

function signSessionToken(email: string, expiresAt: number): string {
  const payload = `${email}:${expiresAt}`;
  const hmac = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
  return `${Buffer.from(payload).toString('base64url')}.${hmac}`;
}

export function verifySessionToken(token: string): { valid: boolean; email?: string } {
  try {
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
    const body = await req.json();
    const { email, password, apiKey } = body;

    const gatewayKey = process.env.API_GATEWAY_KEY || process.env.NEXT_PUBLIC_API_KEY || 'sentinel-dev-key-2026';

    let isAuthenticated = false;

    if (apiKey) {
      if (apiKey === gatewayKey) {
        isAuthenticated = true;
      }
    } else if (email && password) {
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

    response.cookies.set({
      name: 'sentinel_session',
      value: sessionToken,
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
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
