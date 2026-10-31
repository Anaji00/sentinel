'use client';

/**
 * The first visit: what is this, and where do I start? A new account landed on
 * a dashboard of numbers with no word on what the platform is for. Three doors,
 * remembered on the account, so home leads with what this person came for.
 */

import React, { useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { usePreferences, type Focus } from '../lib/preferences';
import { useDialog } from './ui/useDialog';

const CHOICES: { focus: Focus; title: string; text: string; go?: string; tone: string }[] = [
  {
    focus: 'civic',
    title: 'Who is watching my town?',
    text: 'Plate readers, face recognition and drones near you, whether police follow the law, and who profits.',
    go: '/watch',
    tone: 'border-civic/40 hover:border-civic',
  },
  {
    focus: 'stocks',
    title: 'Today’s stock calls',
    text: 'What to buy and what to bet against, how likely each is to work, and how much to put in.',
    go: '/calls',
    tone: 'border-accent/40 hover:border-accent',
  },
  {
    focus: 'both',
    title: 'Show me both',
    text: 'Home leads with today’s calls and who’s watching, side by side.',
    tone: 'border-line-strong hover:border-ink-dim',
  },
];

export function Welcome() {
  const { prefs, save } = usePreferences();
  const router = useRouter();
  const [closed, setClosed] = useState(false);
  const open = Boolean(prefs && !prefs.welcomed && !closed);
  const dismiss = () => {
    setClosed(true);
    void save({ welcomed: true }).catch(() => undefined);
  };
  const dialog = useDialog(open, dismiss, 'Welcome to Sentinel');
  if (!open) return null;

  const choose = (focus: Focus, go?: string) => {
    setClosed(true);
    void save({ welcomed: true, focus }).catch(() => undefined);
    if (go) router.push(go);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4" {...dialog.overlayProps}>
      <div className="w-full max-w-2xl space-y-5 rounded-2xl border border-line bg-raised p-6 shadow-overlay" {...dialog.panelProps}>
        <div className="space-y-1.5">
          <h2 className="text-lead font-semibold text-ink">Welcome to Sentinel</h2>
          <p className="text-sm text-ink-dim">
            Two things in one place: the stock market, read honestly, and the surveillance around you, read from public
            records. Where would you like to start? You can change this any time.
          </p>
        </div>
        <div className="grid gap-3 sm:grid-cols-3">
          {CHOICES.map((c) => (
            <button
              key={c.focus}
              type="button"
              onClick={() => choose(c.focus, c.go)}
              className={`rounded-xl border bg-inset p-4 text-left transition-colors ${c.tone}`}
            >
              <div className="text-sm font-semibold text-ink">{c.title}</div>
              <p className="mt-1 text-xs text-ink-dim">{c.text}</p>
            </button>
          ))}
        </div>
        <div className="flex items-center justify-between text-xs">
          <Link href="/methodology" onClick={dismiss} className="text-accent hover:underline">
            How does this work?
          </Link>
          <button type="button" onClick={dismiss} className="text-ink-mute hover:text-ink">
            Skip
          </button>
        </div>
      </div>
    </div>
  );
}

export default Welcome;
