'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import AuthShell, { buttonClass, fieldClass } from '@/components/AuthShell';

export default function ForgotPasswordPage() {
  const [email, setEmail] = useState('');
  const [busy, setBusy] = useState(false);
  const [sent, setSent] = useState(false);
  // False when the deployment has no SMTP configured: no email will arrive.
  const [emailDelivery, setEmailDelivery] = useState(true);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      const res = await fetch('/api/proxy/api/v1/auth/forgot-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email }),
      });
      const body: { email_delivery?: boolean } = await res.json().catch(() => ({}));
      setEmailDelivery(body.email_delivery !== false);
    } finally {
      // Always the same outcome. Reporting "no such account" here would let
      // anyone test which addresses are registered.
      setSent(true);
      setBusy(false);
    }
  };

  if (sent) {
    return (
      <AuthShell title="Check your email" subtitle="If that address has an account">
        <div className="space-y-4 text-sm text-ink-dim">
          <p>
            If an account exists for that address and it has been confirmed, a reset link is on its
            way.
          </p>
          <p className="text-ink-mute text-xs">
            The link works for 30 minutes and can be used once.
          </p>
          {!emailDelivery && (
            <p className="text-caution text-xs">
              This deployment has no email server configured, so no email will arrive. The
              operator can find the link in the API gateway log, or set a new password with{' '}
              <code>scripts/reset_password.py</code>.
            </p>
          )}
          <Link href="/login" className="block text-center text-accent hover:text-accent">
            Back to sign in
          </Link>
        </div>
      </AuthShell>
    );
  }

  return (
    <AuthShell title="Reset your password" subtitle="We'll email you a link">
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
          <Link href="/login" className="text-accent hover:text-accent">
            Sign in
          </Link>
        </p>
      </form>
    </AuthShell>
  );
}
