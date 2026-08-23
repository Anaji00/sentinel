import { NextRequest, NextResponse } from 'next/server';
import { verifySessionToken } from '../login/route';

export async function GET(req: NextRequest) {
  const cookie = req.cookies.get('sentinel_session');
  if (!cookie || !cookie.value) {
    return NextResponse.json({ authenticated: false }, { status: 401 });
  }

  const { valid, email, role } = verifySessionToken(cookie.value);
  if (!valid) {
    return NextResponse.json({ authenticated: false }, { status: 401 });
  }

  // The role comes from the signed token, not a constant. This reported every
  // signed-in user as 'admin', which was harmless while the platform had one
  // account and is not once anyone can sign up: the client uses this to decide
  // what to show, so operator-only panels were rendered for every visitor.
  return NextResponse.json({
    authenticated: true,
    user: { email, role: (role || 'VIEWER').toUpperCase() },
  });
}
