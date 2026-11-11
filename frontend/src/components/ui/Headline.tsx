/**
 * The page's headline: one big number, what it is, and a line of context. It sits under the page title so the
 * first thing a reader sees is the answer, and everything below is the detail. The number is the only large
 * type on the line, and its colour is the direction it points, never decoration.
 */

import React from 'react';
import type { HeadlineTone, HeadlineView } from '../../lib/headlines';

const TONE: Record<HeadlineTone, string> = { positive: 'text-positive', negative: 'text-negative', neutral: 'text-ink' };

export function Headline({ view }: { view: HeadlineView }) {
  return (
    <section aria-label="Headline" className="flex flex-wrap items-end gap-x-5 gap-y-1 rounded-xl border border-line bg-raised px-5 py-4">
      <div className={`font-mono text-display font-semibold leading-none ${TONE[view.tone]}`}>{view.value}</div>
      <div className="min-w-0">
        <div className="text-sm font-medium text-ink">{view.label}</div>
        <div className="text-xs text-ink-mute">{view.detail}</div>
      </div>
    </section>
  );
}

export default Headline;
