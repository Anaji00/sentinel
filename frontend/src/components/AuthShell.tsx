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
    <div className="min-h-screen w-full bg-page text-ink flex flex-col items-center justify-center relative overflow-hidden p-4">
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-cyan-950/30 via-page to-[#030407]" />
      <div className="absolute -top-40 -left-40 w-96 h-96 bg-cyan-500/15 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute -bottom-40 -right-40 w-96 h-96 bg-purple-500/15 rounded-full blur-3xl pointer-events-none" />

      <div className="relative w-full max-w-md bg-raised/90 border border-accent/40 rounded-3xl p-8 z-10 space-y-6">
        <div className="text-center space-y-2">
          <div className="inline-flex h-14 w-14 rounded-2xl bg-gradient-to-br from-cyan-950 to-slate-950 border border-accent/60 items-center justify-center mb-2">
            <span className="text-accent font-black text-2xl tracking-tighter drop-">S</span>
          </div>
          <h1 className="text-xl font-semibold text-white">{title}</h1>
          {subtitle && <p className="text-micro text-ink-dim tracking-wide">{subtitle}</p>}
        </div>
        {children}
      </div>
    </div>
  );
}

/** Shared input styling, so the four forms stay visually identical. */
export const fieldClass =
  'w-full bg-page/80 border border-line-strong rounded-xl px-4 py-3 text-sm text-ink' +
  'placeholder:text-ink-mute focus:border-cyan-500/60 focus:outline-none focus:ring-1 focus:ring-cyan-500/40';

export const buttonClass =
  'w-full py-3 rounded-xl bg-gradient-to-r from-cyan-600 to-cyan-500 text-slate-950 text-sm font-black' +
  'tracking-widest uppercase hover:from-cyan-500 hover:to-cyan-400 disabled:opacity-50 disabled:cursor-not-allowed';
