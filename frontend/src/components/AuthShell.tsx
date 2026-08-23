'use client';

import React from 'react';

/**
 * The framing every pre-authentication page shares: sign in, sign up, confirm an
 * address, reset a password. Extracted so those four pages differ only by their
 * form, and so a change to the brand does not have to be made four times.
 */
export default function AuthShell({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="min-h-screen w-full bg-[#05070c] text-slate-100 font-mono flex flex-col items-center justify-center relative overflow-hidden p-4">
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-cyan-950/30 via-[#05070c] to-[#030407]" />
      <div className="absolute -top-40 -left-40 w-96 h-96 bg-cyan-500/15 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute -bottom-40 -right-40 w-96 h-96 bg-purple-500/15 rounded-full blur-3xl pointer-events-none" />

      <div className="relative w-full max-w-md bg-[#090d16]/90 border border-[#00f2fe]/40 rounded-3xl p-8 backdrop-blur-2xl shadow-[0_0_60px_rgba(0,242,254,0.15)] z-10 space-y-6">
        <div className="text-center space-y-2">
          <div className="inline-flex h-14 w-14 rounded-2xl bg-gradient-to-br from-cyan-950 to-slate-950 border border-[#00f2fe]/60 items-center justify-center shadow-[0_0_30px_rgba(0,242,254,0.5)] mb-2">
            <span className="text-[#00f2fe] font-black text-2xl tracking-tighter drop-shadow-[0_0_12px_rgba(0,242,254,0.9)]">S</span>
          </div>
          <h1 className="text-xl font-black text-white tracking-widest uppercase">{title}</h1>
          {subtitle && <p className="text-[11px] text-slate-400 font-mono tracking-wide">{subtitle}</p>}
        </div>
        {children}
      </div>
    </div>
  );
}

/** Shared input styling, so the four forms stay visually identical. */
export const fieldClass =
  'w-full bg-slate-950/80 border border-slate-700 rounded-xl px-4 py-3 text-sm text-slate-100 ' +
  'placeholder:text-slate-600 focus:border-cyan-500/60 focus:outline-none focus:ring-1 focus:ring-cyan-500/40';

export const buttonClass =
  'w-full py-3 rounded-xl bg-gradient-to-r from-cyan-600 to-cyan-500 text-slate-950 text-sm font-black ' +
  'tracking-widest uppercase hover:from-cyan-500 hover:to-cyan-400 disabled:opacity-50 disabled:cursor-not-allowed';
