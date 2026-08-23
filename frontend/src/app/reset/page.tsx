'use client';

import React, { Suspense, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';
import AuthShell, { buttonClass, fieldClass } from '@/components/AuthShell';

const MIN_PASSWORD_LENGTH = 12;

function ResetInner() {
  const token = useSearchParams().get('token');
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
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
    if (password !== confirm) {
      setError('Those two passwords do not match.');
      return;
    }

    setBusy(true);
    try {
      const res = await fetch('/api/proxy/api/v1/auth/reset-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token, password }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok) setDone(true);
      else setError(body?.detail || 'Could not reset the password. Request a new link.');
    } catch {
      setError('Could not reach the server. Try again.');
    } finally {
      setBusy(false);
    }
  };

  if (!token) {
    return (
      <AuthShell title="Link not valid" subtitle="NO TOKEN">
        <div className="space-y-4 text-sm text-slate-300">
          <p className="text-slate-400">This page needs a reset link from your email.</p>
          <Link href="/forgot" className="block text-center text-cyan-400 hover:text-cyan-300">
            Request a reset link
          </Link>
        </div>
      </AuthShell>
    );
  }

  if (done) {
    return (
      <AuthShell title="Password changed" subtitle="YOU CAN SIGN IN NOW">
        <div className="space-y-4 text-sm text-slate-300">
          <p>Your password has been changed. Any other reset links have stopped working.</p>
          <Link href="/login" className="block text-center text-cyan-400 hover:text-cyan-300">
            Sign in
          </Link>
        </div>
      </AuthShell>
    );
  }

  return (
    <AuthShell title="Choose a new password" subtitle="LINK VALID FOR 30 MINUTES">
      <form onSubmit={submit} className="space-y-4">
        <div className="space-y-1">
          <label htmlFor="pw" className="text-[10px] uppercase tracking-widest text-slate-400">New password</label>
          <input
            id="pw" type="password" required autoComplete="new-password" value={password}
            onChange={(e) => setPassword(e.target.value)} className={fieldClass}
            placeholder={`At least ${MIN_PASSWORD_LENGTH} characters`}
          />
        </div>
        <div className="space-y-1">
          <label htmlFor="pw2" className="text-[10px] uppercase tracking-widest text-slate-400">Confirm</label>
          <input
            id="pw2" type="password" required autoComplete="new-password" value={confirm}
            onChange={(e) => setConfirm(e.target.value)} className={fieldClass} placeholder="Type it again"
          />
        </div>

        {error && <p className="text-sm text-rose-400">{error}</p>}

        <button type="submit" disabled={busy} className={buttonClass}>
          {busy ? 'Saving…' : 'Change password'}
        </button>
      </form>
    </AuthShell>
  );
}

export default function ResetPage() {
  return (
    <Suspense fallback={<AuthShell title="Loading" subtitle="ONE MOMENT"><p className="text-sm text-slate-400 text-center">Loading…</p></AuthShell>}>
      <ResetInner />
    </Suspense>
  );
}
