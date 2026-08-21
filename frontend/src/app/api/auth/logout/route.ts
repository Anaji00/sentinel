import { NextResponse } from 'next/server';

export async function POST() {
  const response = NextResponse.json({ success: true });
  const isProduction = process.env.NODE_ENV === 'production' || process.env.SENTINEL_ENV === 'production';
  const isSecureCookie = isProduction || process.env.COOKIE_SECURE !== 'false';

  response.cookies.set({
    name: 'sentinel_session',
    value: '',
    httpOnly: true,
    secure: isSecureCookie,
    sameSite: 'strict',
    path: '/',
    maxAge: 0,
  });
  return response;
}
