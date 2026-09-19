'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import AuthShell, { buttonClass, fieldClass } from '@/components/AuthShell';

export default function ForgotPasswordPage() {
  const [email, setEmail] = useState('');
  const [busy, setBusy] = useState(false);
  const [sent, setSent] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      await fetch('/api/proxy/api/v1/auth/forgot-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email }),
      });
    } finally {
      // Always the same outcome. Reporting "no such account" here would let
      // anyone test which addresses are registered.
      setSent(true);
      setBusy(false);
    }
  };

  if (sent) {
    return (
      <AuthShell title="Check your email" subtitle="IF THAT ADDRESS HAS AN ACCOUNT">
        <div className="space-y-4 text-sm text-ink-dim">
          <p>
            If an account exists for that address and it has been confirmed, a reset link is on its
            way.
          </p>
          <p className="text-ink-mute text-xs">
            The link works for 30 minutes and can be used once.
          </p>
          <Link href="/login" className="block text-center text-cyan-400 hover:text-cyan-300">
            Back to sign in
          </Link>
        </div>
      </AuthShell>
    );
  }

  return (
    <AuthShell title="Reset your password" subtitle="WE WILL EMAIL YOU A LINK">
      <form onSubmit={submit} className="space-y-4">
        <div className="space-y-1">
          <label htmlFor="email" className="text-micro uppercase tracking-widest text-ink-dim">
            Email
          </label>
          <input
            id="email"
            type="email"
            required
            autoComplete="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className={fieldClass}
            placeholder="you@example.com"
          />
        </div>
        <button type="submit" disabled={busy} className={buttonClass}>
          {busy ? 'Sending…' : 'Send reset link'}
        </button>
        <p className="text-center text-xs text-ink-mute">
          Remembered it?{' '}
          <Link href="/login" className="text-cyan-400 hover:text-cyan-300">
            Sign in
          </Link>
        </p>
      </form>
    </AuthShell>
  );
}
