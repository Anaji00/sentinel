'use client';

/**
 * A folded section. The content is not rendered, and so not fetched, until it is
 * opened, and the choice is remembered. Built on the native <details> element, so
 * it works from the keyboard and is announced as expandable without extra code.
 *
 *   variant "link": an inline "Show more ..." line, for detail inside a section
 *   variant "row":  a full-width row with a chevron, for whole sections
 */

import React from 'react';
import { readOpen, writeOpen } from '../../lib/disclosure';
import { IconChevronDown } from './icons';

export function Disclosure({
  label,
  hint,
  variant = 'link',
  defaultOpen = false,
  storageKey,
  children,
}: {
  label: string;
  hint?: string;
  variant?: 'link' | 'row';
  defaultOpen?: boolean;
  storageKey?: string;
  children: React.ReactNode;
}) {
  const [open, setOpen] = React.useState(defaultOpen);

  // After mount, not in the initial state: the server cannot know what this browser remembered.
  React.useEffect(() => {
    setOpen(readOpen(storageKey, defaultOpen));
  }, [storageKey, defaultOpen]);

  const toggle = (e: React.SyntheticEvent<HTMLDetailsElement>) => {
    const next = e.currentTarget.open;
    if (next === open) return;
    setOpen(next);
    writeOpen(storageKey, next);
  };

  return (
    <details open={open} onToggle={toggle} className={variant === 'row' ? 'rounded-xl border border-line bg-inset' : ''}>
      {variant === 'row' ? (
        <summary className="flex cursor-pointer list-none select-none items-center justify-between gap-3 px-4 py-3 [&::-webkit-details-marker]:hidden">
          <span className="text-sm font-semibold text-ink">{label}</span>
          <span className="flex items-center gap-2 text-micro text-ink-mute">
            {hint}
            <IconChevronDown className={`h-4 w-4 transition-transform ${open ? 'rotate-180' : ''}`} />
          </span>
        </summary>
      ) : (
        <summary className="cursor-pointer list-none select-none text-xs text-accent hover:underline [&::-webkit-details-marker]:hidden">
          {open ? 'Hide' : 'Show'} {label}
        </summary>
      )}
      {open && <div className={variant === 'row' ? 'space-y-4 px-3 pb-3' : 'space-y-3.5 pt-3.5'}>{children}</div>}
    </details>
  );
}

export default Disclosure;
