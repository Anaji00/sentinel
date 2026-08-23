'use client';

import React, { Suspense, useCallback, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';
import AuthShell, { buttonClass, fieldClass } from '@/components/AuthShell';

type State = 'working' | 'confirmed' | 'failed' | 'missing';

function VerifyInner() {
  const params = useSearchParams();
  const token = params.get('token');
  const [state, setState] = useState<State>(token ? 'working' : 'missing');
  const [resendEmail, setResendEmail] = useState('');
  const [resent, setResent] = useState(false);
  // The token is single-use. React runs effects twice in development, and a
  // second submission would consume an already-burned token and report failure
  // for a link that actually worked.
  const attempted = useRef(false);

  const confirm = useCallback(async (value: string) => {
    try {
      const res = await fetch('/api/proxy/api/v1/auth/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: value }),
      });
      setState(res.ok ? 'confirmed' : 'failed');
    } catch {
      setState('failed');
    }
  }, []);

  useEffect(() => {
    if (!token || attempted.current) return;
    attempted.current = true;
    void confirm(token);
  }, [token, confirm]);

  const resend = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await fetch('/api/proxy/api/v1/auth/resend-verification', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: resendEmail }),
      });
    } finally {
      // Deliberately unconditional: the server answers the same way whether or
      // not the address needs confirming, and the client must not leak more.
      setResent(true);
    }
  };

  if (state === 'working') {
    return (
      <AuthShell title="Confirming" subtitle="ONE MOMENT">
        <p className="text-sm text-slate-400 text-center">Checking your link…</p>
      </AuthShell>
    );
  }

  if (state === 'confirmed') {
    return (
      <AuthShell title="Email confirmed" subtitle="YOU ARE ALL SET">
        <div className="space-y-4 text-sm text-slate-300">
          <p>Your address is confirmed. Nothing else to do.</p>
          <Link href="/login" className="block text-center text-cyan-400 hover:text-cyan-300">
            Sign in
          </Link>
        </div>
      </AuthShell>
    );
  }

  return (
    <AuthShell title="Link not valid" subtitle="IT MAY HAVE EXPIRED">
      <div className="space-y-4 text-sm text-slate-300">
        <p className="text-slate-400">
          Confirmation links last 48 hours and can be used once. Request a fresh one below — your
          account still works on the free plan in the meantime.
        </p>

        {resent ? (
          <p className="text-cyan-400">
            If that address still needs confirming, a new link is on its way.
          </p>
        ) : (
          <form onSubmit={resend} className="space-y-3">
            <input
              type="email" required value={resendEmail} onChange={(e) => setResendEmail(e.target.value)}
              className={fieldClass} placeholder="you@example.com" aria-label="Email address"
            />
            <button type="submit" className={buttonClass}>Send a new link</button>
          </form>
        )}

        <Link href="/login" className="block text-center text-xs text-slate-500 hover:text-slate-300">
          Back to sign in
        </Link>
      </div>
    </AuthShell>
  );
}

export default function VerifyPage() {
  // useSearchParams needs a Suspense boundary, or the whole route opts out of
  // static rendering and the build warns.
  return (
    <Suspense fallback={<AuthShell title="Confirming" subtitle="ONE MOMENT"><p className="text-sm text-slate-400 text-center">Loading…</p></AuthShell>}>
      <VerifyInner />
    </Suspense>
  );
}
