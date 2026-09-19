'use client';

/**
 * What a panel shows when it has nothing to show.
 *
 * Audited across the twelve dashboard routes with the backend unreachable:
 * `/intelligence` rendered 37 characters — a heading and an empty box.
 * `/operations` rendered 66 — three headings and a subtitle. No spinner, no
 * message, no error. A reader cannot tell a loading page from a broken one
 * from a genuinely quiet one, and all three are different things to know.
 *
 * Three states, deliberately distinct, because the backend audit spent four
 * hundred entries on exactly this distinction:
 *
 *   loading   we have not heard back yet
 *   empty     we heard back, and the answer is nothing
 *   error     we asked and could not be told
 */

import React from 'react';

type Kind = 'loading' | 'empty' | 'error';

const TONE: Record<Kind, string> = {
  loading: 'text-ink-mute',
  empty: 'text-ink-mute',
  error: 'text-amber-400/90',
};

export function EmptyState({
  kind,
  title,
  detail,
}: {
  kind: Kind;
  title: string;
  /** Why, in a sentence. Worth writing: it is what turns a blank box into an answer. */
  detail?: string;
}) {
  return (
    <div className="flex h-full min-h-[8rem] w-full flex-col items-center justify-center gap-1.5 p-6 text-center">
      {kind === 'loading' && (
        <span
          aria-hidden="true"
          className="h-4 w-4 animate-spin rounded-full border-2 border-line-strong border-t-cyan-400"
        />
      )}
      <p
        className={`text-micro font-medium ${TONE[kind]}`}
        role={kind === 'error' ? 'alert' : undefined}
      >
        {title}
      </p>
      {detail && <p className="max-w-sm text-micro leading-relaxed text-ink-mute">{detail}</p>}
    </div>
  );
}
