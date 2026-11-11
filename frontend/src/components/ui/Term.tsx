'use client';

/**
 * A technical word with its plain-English meaning one hover, focus or tap away.
 *
 * `<Term word="sharpe">Net Sharpe</Term>` draws the text with a dotted underline; the meaning appears in a
 * bubble on hover or keyboard focus and is linked with aria-describedby, so a screen reader reads it too.
 * With no definition on file it draws the text plain, never an empty bubble.
 */

import React from 'react';
import { defineTerm } from '../../lib/glossary';

export function Term({ word, children }: { word: string; children: React.ReactNode }) {
  const id = React.useId();
  const meaning = defineTerm(word);
  if (!meaning) return <>{children}</>;
  return (
    <span className="group relative inline-block">
      <span
        tabIndex={0}
        aria-describedby={id}
        className="cursor-help underline decoration-dotted decoration-ink-mute underline-offset-2 outline-none focus-visible:ring-1 focus-visible:ring-accent/60"
      >
        {children}
      </span>
      <span
        id={id}
        role="tooltip"
        className="pointer-events-none invisible absolute left-0 top-full z-30 mt-1 w-60 rounded-lg border border-line-strong bg-inset px-3 py-2 text-left text-micro font-normal normal-case leading-snug tracking-normal text-ink-dim opacity-0 shadow-overlay transition-opacity group-focus-within:visible group-focus-within:opacity-100 group-hover:visible group-hover:opacity-100"
      >
        {meaning}
      </span>
    </span>
  );
}

export default Term;
