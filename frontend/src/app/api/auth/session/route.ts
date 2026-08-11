import { NextRequest, NextResponse } from 'next/server';
import { verifySessionToken } from '../login/route';

export async function GET(req: NextRequest) {
  const cookie = req.cookies.get('sentinel_session');
  if (!cookie || !cookie.value) {
    return NextResponse.json({ authenticated: false }, { status: 401 });
  }

  const { valid, email } = verifySessionToken(cookie.value);
  if (!valid) {
    return NextResponse.json({ authenticated: false }, { status: 401 });
  }

  return NextResponse.json({
    authenticated: true,
    user: { email, role: 'admin' },
  });
}
