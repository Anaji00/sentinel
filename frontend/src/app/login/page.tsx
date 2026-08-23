'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';

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
        setTimeout(() => {
          router.push('/');
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
    <div className="min-h-screen w-full bg-[#05070c] text-slate-100 font-mono flex flex-col items-center justify-center relative overflow-hidden select-none p-4">
      {/* Dynamic Cyber Grid Background & Glow Orbs */}
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-cyan-950/30 via-[#05070c] to-[#030407]" />
      <div className="absolute -top-40 -left-40 w-96 h-96 bg-cyan-500/15 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute -bottom-40 -right-40 w-96 h-96 bg-purple-500/15 rounded-full blur-3xl pointer-events-none" />

      {/* Main Glass Card */}
      <div className="relative w-full max-w-md bg-[#090d16]/90 border border-[#00f2fe]/40 rounded-3xl p-8 backdrop-blur-2xl shadow-[0_0_60px_rgba(0,242,254,0.15)] z-10 space-y-6">
        
        {/* Brand Header */}
        <div className="text-center space-y-2">
          <div className="inline-flex h-14 w-14 rounded-2xl bg-gradient-to-br from-cyan-950 to-slate-950 border border-[#00f2fe]/60 items-center justify-center shadow-[0_0_30px_rgba(0,242,254,0.5)] mb-2">
            <span className="text-[#00f2fe] font-black text-2xl tracking-tighter drop-shadow-[0_0_12px_rgba(0,242,254,0.9)]">S</span>
          </div>
          <h1 className="text-xl font-black text-white tracking-widest uppercase">
            SENTINEL <span className="text-[#00f2fe]">INTELLIGENCE</span>
          </h1>
          <p className="text-[11px] text-slate-400 font-mono tracking-wide">
            INSTITUTIONAL MULTI-DOMAIN QUANT RADAR & SWARM PLATFORM
          </p>
        </div>

        {/* Authentication Form */}
        <form onSubmit={handleSubmit} className="space-y-4 text-xs">
              <div>
                <label className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">Corporate Email</label>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full mt-1.5 bg-slate-950/90 border border-slate-800 focus:border-[#00f2fe] rounded-xl px-4 py-3 text-white outline-none transition-colors"
                  placeholder="name@firm.com"
                  required
                />
              </div>
              <div>
                <div className="flex justify-between items-center">
                  <label className="text-[10px] text-slate-400 font-bold uppercase tracking-wider">Password</label>
                  <span className="text-[10px] text-cyan-400 hover:underline cursor-pointer">SAML SSO</span>
                </div>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full mt-1.5 bg-slate-950/90 border border-slate-800 focus:border-[#00f2fe] rounded-xl px-4 py-3 text-white outline-none transition-colors"
                  required
                />
              </div>

          {errorMsg && (
            <div className="p-3 rounded-xl bg-red-950/80 border border-red-500/40 text-red-300 text-[11px] font-semibold">
              ⚠️ {errorMsg}
            </div>
          )}

          {/* Submit Button */}
          <button
            type="submit"
            disabled={isLoading || isSuccess}
            className="w-full py-3.5 mt-2 rounded-xl bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-white font-extrabold tracking-widest uppercase transition-all shadow-[0_0_25px_rgba(0,242,254,0.4)] disabled:opacity-50 flex items-center justify-center gap-2"
          >
            {isLoading ? (
              <span className="flex items-center gap-2">
                <span className="h-2 w-2 rounded-full bg-white animate-ping" />
                AUTHENTICATING...
              </span>
            ) : isSuccess ? (
              <span className="text-emerald-300 font-bold">✓ AUTHENTICATED</span>
            ) : (
              'ENTER COMMAND HUD'
            )}
          </button>
        </form>

        {/* Open signup: the platform is free to join, so the way in belongs on
            the sign-in page rather than behind it. */}
        <div className="flex items-center justify-between text-xs pt-1">
          <Link href="/forgot" className="text-slate-400 hover:text-cyan-400">
            Forgot password?
          </Link>
          <Link href="/signup" className="text-cyan-400 hover:text-cyan-300 font-bold">
            Create a free account
          </Link>
        </div>

        {/* Development Quick Navigation Bypass */}
        <div className="pt-2 border-t border-slate-800/80 flex items-center justify-between text-[11px]">
          <span className="text-slate-500">DEV MODE ACTIVE</span>
          <Link
            href="/"
            className="text-cyan-400 hover:text-cyan-300 font-bold underline flex items-center gap-1"
          >
            <span>Proceed to Dashboards</span> →
          </Link>
        </div>

      </div>

      {/* Footer Security Badges */}
      <div className="mt-8 text-center text-[10px] text-slate-500 space-y-1 z-10">
        <p>SENTINEL ENTERPRISE SECURITY • RSA 4096 / TLS 1.3 ENCRYPTED</p>
        <p className="text-slate-600">CONFIDENTIAL & PROPRIETARY SYSTEM</p>
      </div>
    </div>
  );
}
