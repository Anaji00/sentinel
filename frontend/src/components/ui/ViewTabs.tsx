/**
 * Tabs that are links: each view of a merged page has its own URL (`?view=`),
 * so a view can be bookmarked, shared and reached from an old route's redirect.
 * No client state, so it renders in a server component.
 */

import React from 'react';
import Link from 'next/link';

export interface View {
  key: string;
  label: string;
  hint?: string;
}

/** The view a `?view=` value names, or the first when it names none. */
export function pickView(views: View[], requested: string | string[] | undefined): View {
  const want = Array.isArray(requested) ? requested[0] : requested;
  return views.find((v) => v.key === want) ?? views[0];
}

export function ViewTabs({ base, views, active }: { base: string; views: View[]; active: string }) {
  return (
    <nav aria-label="Views" className="flex flex-wrap rounded-lg border border-line bg-inset p-0.5">
      {views.map((v, i) => {
        const on = v.key === active;
        return (
          <Link
            key={v.key}
            href={i === 0 ? base : `${base}?view=${v.key}`}
            aria-current={on ? 'page' : undefined}
            title={v.hint}
            className={`rounded-md px-3 py-1.5 text-xs ${on ? 'bg-overlay text-ink font-semibold' : 'text-ink-dim hover:text-ink'}`}
          >
            {v.label}
          </Link>
        );
      })}
    </nav>
  );
}

export default ViewTabs;
