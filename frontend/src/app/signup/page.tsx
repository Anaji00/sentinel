'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import AuthShell, { buttonClass, fieldClass } from '@/components/AuthShell';

// Mirrors the server's floor. Enforced there too — this only spares the user a
// round trip, it is not the check that matters.
const MIN_PASSWORD_LENGTH = 12;

export default function SignupPage() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [busy, setBusy] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (password.length < MIN_PASSWORD_LENGTH) {
      setError(`Choose a password of at least ${MIN_PASSWORD_LENGTH} characters.`);
      return;
    }

    setBusy(true);
    try {
      const res = await fetch('/api/proxy/api/v1/auth/signup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password, display_name: displayName || null }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.status === 429) {
        setError('Too many sign-up attempts from this network. Try again in a few minutes.');
      } else if (!res.ok) {
        setError(body?.detail || 'Could not create the account. Try again.');
      } else {
        setDone(true);
      }
    } catch {
      setError('Could not reach the server. Check your connection and try again.');
    } finally {
      setBusy(false);
    }
  };

  if (done) {
    return (
      <AuthShell title="Check your email" subtitle="ONE STEP LEFT">
        <div className="space-y-4 text-sm text-slate-300 leading-relaxed">
          <p>
            We sent a confirmation link to <span className="text-cyan-400">{email}</span>. It works
            for 48 hours.
          </p>
          <p className="text-slate-400">
            Your account is already active on the free plan — the whole analyst platform, every
            domain, the knowledge graph and all dashboards. Confirming just proves the address is
            yours.
          </p>
          <Link href="/login" className="block text-center text-cyan-400 hover:text-cyan-300 text-sm">
            Go to sign in
          </Link>
        </div>
      </AuthShell>
    );
  }

  return (
    <AuthShell title="Create your account" subtitle="FREE — NO CARD REQUIRED">
      <form onSubmit={submit} className="space-y-4">
        <div className="space-y-1">
          <label htmlFor="email" className="text-[10px] uppercase tracking-widest text-slate-400">Email</label>
          <input
            id="email" type="email" required autoComplete="email" value={email}
            onChange={(e) => setEmail(e.target.value)} className={fieldClass} placeholder="you@example.com"
          />
        </div>

        <div className="space-y-1">
          <label htmlFor="name" className="text-[10px] uppercase tracking-widest text-slate-400">
            Name <span className="text-slate-600">(optional)</span>
          </label>
          <input
            id="name" type="text" autoComplete="name" value={displayName}
            onChange={(e) => setDisplayName(e.target.value)} className={fieldClass} placeholder="How we address you"
          />
        </div>

        <div className="space-y-1">
          <label htmlFor="password" className="text-[10px] uppercase tracking-widest text-slate-400">Password</label>
          <input
            id="password" type="password" required autoComplete="new-password" value={password}
            onChange={(e) => setPassword(e.target.value)} className={fieldClass}
            placeholder={`At least ${MIN_PASSWORD_LENGTH} characters`}
          />
          <p className="text-[10px] text-slate-500 pt-1">
            {password.length > 0 && password.length < MIN_PASSWORD_LENGTH
              ? `${MIN_PASSWORD_LENGTH - password.length} more character${MIN_PASSWORD_LENGTH - password.length === 1 ? '' : 's'} needed`
              : `Minimum ${MIN_PASSWORD_LENGTH} characters`}
          </p>
        </div>

        {error && <p className="text-sm text-rose-400">{error}</p>}

        <button type="submit" disabled={busy} className={buttonClass}>
          {busy ? 'Creating…' : 'Create account'}
        </button>

        <p className="text-center text-xs text-slate-500">
          Already have one?{' '}
          <Link href="/login" className="text-cyan-400 hover:text-cyan-300">Sign in</Link>
        </p>
      </form>
    </AuthShell>
  );
}
