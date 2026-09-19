'use client';

import React, { useEffect, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useSession } from '@/components/ui/SessionContext';

export default function LoginPage() {
  const router = useRouter();
  // Empty by default. Prefilling the configured admin address discloses a valid
  // account name to anyone who loads the login page, and hardcodes one
  // deployment's operator into the build.
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isSuccess, setIsSuccess] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  // Distinct from an error: an expired session is not a failure the
  // operator caused, and colouring it like a rejected password says it was.
  const [notice, setNotice] = useState<string | null>(null);
  const session = useSession();
  // Rendered only when a provider is actually configured. An SSO button that
  // leads to a 404 is worse than no button: the person cannot tell whether
  // they are meant to use it.
  const [sso, setSso] = useState<{ enabled: boolean; label: string } | null>(null);
  const [ssoBusy, setSsoBusy] = useState(false);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/auth/sso/start')
      .then((r) => r.json())
      .then((d) => {
        if (!cancelled) setSso({ enabled: !!d?.enabled, label: d?.label || 'Single sign-on' });
      })
      .catch(() => {
        if (!cancelled) setSso({ enabled: false, label: '' });
      });

    const params = new URLSearchParams(window.location.search);

    // The callback redirects here with ?sso_error=... when the provider or the
    // gateway declined. Surfaced in the same place as a password failure,
    // because to the person signing in it is the same event.
    const ssoError = params.get('sso_error');
    if (ssoError) setErrorMsg(ssoError);

    // `?reason=expired` is DataProvider redirecting an operator whose cookie
    // ran out mid-session. Worth saying: arriving at a login screen with no
    // explanation reads as "I was signed out", which invites the suspicion
    // that something is wrong with the account rather than with the clock.
    const reason = params.get('reason');
    if (!ssoError) {
      if (reason === 'expired') {
        setNotice('Your session expired. Sign in to pick up where you were.');
      } else if (reason === 'signin' && params.get('next')) {
        // The middleware turned an anonymous request away. Saying which page
        // is the difference between "sign in" and "sign in to see /audit" --
        // the second one tells a person whether they are in the right place.
        setNotice(`Sign in to continue to ${params.get('next')}.`);
      }
    }
    return () => {
      cancelled = true;
    };
  }, []);

  const startSso = async () => {
    setSsoBusy(true);
    setErrorMsg(null);
    try {
      const res = await fetch('/api/auth/sso/start', { method: 'POST' });
      const data = await res.json();
      if (res.ok && data?.authorization_url) {
        window.location.href = data.authorization_url;
        return;
      }
      setErrorMsg(data?.error || 'Single sign-on is unavailable.');
    } catch {
      setErrorMsg('Could not reach the sign-on provider.');
    }
    setSsoBusy(false);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setErrorMsg(null);

    try {
      const payload = { email, password };
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      if (res.ok && data.success) {
        setIsSuccess(true);
        // Re-ask before navigating. The provider polls on an interval, so
        // without this the destination page renders its header from a cached
        // "anonymous" and shows a Sign in button to someone who just did.
        session.refresh();
        setTimeout(() => {
          // Back to the panel they were reading, not to the command
          // centre. `next` is taken only as a same-origin path: an absolute
          // URL here would make the login form an open redirect.
          const next = new URLSearchParams(window.location.search).get('next');
          const safe = next && next.startsWith('/') && !next.startsWith('//') ? next : '/';
          router.push(safe);
        }, 500);
      } else {
        setErrorMsg(data.error || 'Authentication failed. Please check credentials.');
      }
    } catch (err: any) {
      setErrorMsg('Connection error during authentication.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen w-full bg-page text-ink flex flex-col items-center justify-center relative overflow-hidden select-none p-4">
      {/* Dynamic Cyber Grid Background & Glow Orbs */}
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-cyan-950/30 via-page to-[#030407]" />
      <div className="absolute -top-40 -left-40 w-96 h-96 bg-cyan-500/15 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute -bottom-40 -right-40 w-96 h-96 bg-purple-500/15 rounded-full blur-3xl pointer-events-none" />

      {/* Main Glass Card */}
      <div className="relative w-full max-w-md bg-raised/90 border border-accent/40 rounded-3xl p-8 z-10 space-y-6">
        {/* Brand Header */}
        <div className="text-center space-y-2">
          <div className="inline-flex h-14 w-14 rounded-2xl bg-gradient-to-br from-cyan-950 to-slate-950 border border-accent/60 items-center justify-center mb-2">
            <span className="text-accent font-black text-2xl tracking-tighter drop-">S</span>
          </div>
          <h1 className="text-xl font-semibold text-white">
            SENTINEL <span className="text-accent">INTELLIGENCE</span>
          </h1>
          <p className="text-micro text-ink-dim tracking-wide">
            Multi-domain market and geopolitical intelligence
          </p>
        </div>

        {/* Authentication Form */}
        <form onSubmit={handleSubmit} className="space-y-4 text-xs">
          <div>
            <label className="text-micro text-ink-dim font-bold uppercase tracking-wider">
              Corporate Email
            </label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full mt-1.5 bg-page/90 border border-line focus:border-accent rounded-xl px-4 py-3 text-white outline-none transition-colors"
              placeholder="name@firm.com"
              required
            />
          </div>
          <div>
            <div className="flex justify-between items-center">
              <label className="text-micro text-ink-dim font-bold uppercase tracking-wider">
                Password
              </label>
              <span className="text-micro text-cyan-400 hover:underline cursor-pointer">
                SAML SSO
              </span>
            </div>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full mt-1.5 bg-page/90 border border-line focus:border-accent rounded-xl px-4 py-3 text-white outline-none transition-colors"
              required
            />
          </div>

          {notice && !errorMsg && (
            <div
              role="status"
              className="rounded-xl border border-line-strong bg-raised p-3 text-micro text-ink-dim"
            >
              {notice}
            </div>
          )}

          {errorMsg && (
            <div
              role="alert"
              className="rounded-xl border border-negative/40 bg-negative/10 p-3 text-micro font-medium text-negative"
            >
              {errorMsg}
            </div>
          )}

          {/* Submit Button */}
          <button
            type="submit"
            disabled={isLoading || isSuccess}
            className="w-full py-3.5 mt-2 rounded-xl bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-white font-extrabold tracking-widest uppercase transition-all disabled:opacity-50 flex items-center justify-center gap-2"
          >
            {isLoading ? (
              <span className="flex items-center gap-2">
                <span className="h-2 w-2 rounded-full bg-white animate-ping" />
                Signing in…
              </span>
            ) : isSuccess ? (
              <span className="text-emerald-300 font-bold"> AUTHENTICATED</span>
            ) : (
              'ENTER COMMAND HUD'
            )}
          </button>
        </form>

        {/* Single sign-on, when the deployment has a provider. Below the
            password form rather than above it: passwords still work, and
            demoting the path most people use to a secondary position would be
            a regression for every deployment that never configures an IdP. */}
        {sso?.enabled && (
          <div className="pt-3">
            <div className="flex items-center gap-3 pb-3">
              <span className="h-px flex-1 bg-overlay" />
              <span className="text-micro uppercase tracking-widest text-ink-mute">or</span>
              <span className="h-px flex-1 bg-overlay" />
            </div>
            <button
              type="button"
              onClick={startSso}
              disabled={ssoBusy || isLoading || isSuccess}
              className="w-full py-3 rounded-xl border border-line-strong hover:border-cyan-500/60 text-ink font-bold tracking-wide uppercase text-xs transition-all disabled:opacity-50"
            >
              {ssoBusy ? 'REDIRECTING...' : sso.label}
            </button>
          </div>
        )}

        {/* Open signup: the platform is free to join, so the way in belongs on
            the sign-in page rather than behind it. */}
        <div className="flex items-center justify-between text-xs pt-1">
          <Link href="/forgot" className="text-ink-dim hover:text-cyan-400">
            Forgot password?
          </Link>
          <Link href="/signup" className="text-cyan-400 hover:text-cyan-300 font-bold">
            Create a free account
          </Link>
        </div>

        {/* A "DEV MODE ACTIVE — Proceed to Dashboards" link stood here, and it
            was not gated on anything: no NODE_ENV check, no flag, nothing. The
            label was decoration, so it shipped to production, where it invited
            every visitor to skip the form. It never granted access -- there was
            no gate to skip -- but a sign-in screen offering a way around itself
            is the wrong message, and with routing in place it now leads
            straight back here, which reads as the login being broken. */}
      </div>

      {/* Footer Security Badges */}
      <div className="mt-8 text-center text-micro text-ink-mute space-y-1 z-10">
        <p>SENTINEL ENTERPRISE SECURITY • RSA 4096 / TLS 1.3 ENCRYPTED</p>
        <p className="text-ink-mute">CONFIDENTIAL & PROPRIETARY SYSTEM</p>
      </div>
    </div>
  );
}
