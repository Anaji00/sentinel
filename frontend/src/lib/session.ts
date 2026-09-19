/**
 * The session cookie format: how it is signed, and how it is checked.
 *
 * This lived in `app/api/auth/login/route.ts` and was imported from there by
 * the SSO callback, the session probe and the proxy. A Next route module may
 * only export route handlers -- the generated route type asserts exactly that
 * -- so the shared helper had to leave. Nothing about it was route-specific:
 * it is an HMAC over a string.
 */

import crypto from 'crypto';

// Fail closed: no hardcoded secret defaults. SESSION_SECRET must match the API
// gateway's SESSION_SECRET, because the gateway verifies the cookie these
// helpers mint.
const SESSION_SECRET = process.env.SESSION_SECRET;

/** The cookie the BFF mints and the gateway verifies. */
export const SESSION_COOKIE = 'sentinel_session';

/**
 * The one-shot CSRF state for the SSO round trip.
 *
 * This was exported from `app/api/auth/sso/start/route.ts` and imported by
 * the callback beside it -- the same shape that made the frontend
 * unbuildable once, since a route module may export nothing but handlers.
 * It only surfaced after the first instance was fixed, because tsc stops
 * at the first offending route type.
 */
export const SSO_STATE_COOKIE = 'sentinel_sso_state';

// The token carries the account's role. Without it the gateway falls back to
// ANALYST for every session, which is why the BFF used to attach the operator's
// master API key to proxied calls instead -- making every signed-in visitor an
// admin and rendering any subscription gate decorative.
// Exported so the SSO callback mints byte-identical cookies. A second
// implementation there would be a second place for the session format to
// drift, and a cookie signed slightly differently fails verification in a
// way that looks like an expired login.
export function signSessionToken(email: string, role: string, expiresAt: number): string {
  const payload = `${email}:${role}:${expiresAt}`;
  const hmac = crypto
    .createHmac('sha256', SESSION_SECRET as string)
    .update(payload)
    .digest('hex');
  return `${Buffer.from(payload).toString('base64url')}.${hmac}`;
}

export function verifySessionToken(token: string): {
  valid: boolean;
  email?: string;
  role?: string;
} {
  try {
    if (!SESSION_SECRET) return { valid: false };
    const [encodedPayload, signature] = token.split('.');
    if (!encodedPayload || !signature) return { valid: false };

    const payload = Buffer.from(encodedPayload, 'base64url').toString('utf8');
    // `email:role:expiresAt`, falling back to the older `email:expiresAt` so
    // cookies minted before roles were carried keep working until they expire.
    const segments = payload.split(':');
    let email: string, role: string, expiresAtStr: string;
    if (segments.length >= 3) {
      [email, role, expiresAtStr] = segments;
    } else {
      // VIEWER, not ANALYST -- the same rule the gateway applies.
      //
      // ANALYST carries write access to cases, watchlists and reports. A cookie
      // minted before roles were encoded says nothing about what its holder may
      // do, and the least privilege is the only safe reading of silence.
      [email, expiresAtStr] = segments;
      role = 'VIEWER';
    }
    const expiresAt = parseInt(expiresAtStr, 10);

    if (isNaN(expiresAt) || Date.now() > expiresAt) return { valid: false };

    const expectedHmac = crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest('hex');
    // Length-mismatched buffers make timingSafeEqual throw, which the catch
    // below would turn into a plain `invalid` -- check first so the comparison
    // is reached only when it can be constant-time.
    const sigBuf = Buffer.from(signature);
    const expBuf = Buffer.from(expectedHmac);
    if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) {
      return { valid: false };
    }

    return { valid: true, email, role };
  } catch (e) {
    return { valid: false };
  }
}
