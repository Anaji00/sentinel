'use client';

/**
 * A short, dismissible "how to read this" card at the top of the stock drawer, shown the first time and then never
 * again. Four lines, in the order the drawer lays out its sections, so a newcomer learns where to look.
 */

import React from 'react';
import { FIRST_LOOK_STEPS, hasSeenFirstLook, markFirstLookSeen } from '../lib/firstLook';
import { Button } from './ui/Button';

export default function FirstLook() {
  // Decided after mount: the server cannot read this browser's storage, so it renders nothing.
  const [show, setShow] = React.useState(false);
  React.useEffect(() => {
    setShow(!hasSeenFirstLook());
  }, []);
  if (!show) return null;
  const close = () => {
    markFirstLookSeen();
    setShow(false);
  };
  return (
    <section aria-label="How to read this page" className="space-y-2 rounded-xl border border-line-accent bg-accent-dim p-4">
      <div className="flex items-start justify-between gap-3">
        <h3 className="text-sm font-semibold text-ink">How to read this</h3>
        <Button variant="outline" size="sm" onClick={close}>
          Got it
        </Button>
      </div>
      <ol className="space-y-1.5 text-xs text-ink-dim">
        {FIRST_LOOK_STEPS.map((s, i) => (
          <li key={s.title} className="grid grid-cols-[1.25rem_minmax(0,1fr)] gap-x-1">
            <span className="font-mono text-ink-mute">{i + 1}</span>
            <span>
              <span className="font-medium text-ink">{s.title}.</span> {s.text}
            </span>
          </li>
        ))}
      </ol>
    </section>
  );
}
