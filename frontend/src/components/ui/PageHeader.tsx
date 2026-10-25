/**
 * One header for every page: a plain-English title, a sentence on what the
 * page answers, and the pages a reader usually goes to next.
 *
 * Most pages had only an `sr-only` <h1> and let their first panel's title
 * stand in, so nothing on screen said what the page was for or where to go
 * from it. Related links make the market pages one connected place: from a
 * signal to the stock behind it, from the board to the portfolio built on it.
 */

import React from 'react';
import Link from 'next/link';

export interface RelatedLink {
  href: string;
  label: string;
}

interface PageHeaderProps {
  title: string;
  description: React.ReactNode;
  related?: RelatedLink[];
  /** Controls on the right (tabs, a live badge). */
  children?: React.ReactNode;
}

export function PageHeader({ title, description, related = [], children }: PageHeaderProps) {
  return (
    <header className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
      <div className="min-w-0 space-y-1">
        <h1 className="text-lead font-semibold text-ink leading-tight">{title}</h1>
        <p className="text-sm text-ink-dim max-w-3xl">{description}</p>
        {related.length > 0 && (
          <nav aria-label="Related pages" className="flex flex-wrap items-center gap-1.5 pt-1">
            <span className="text-micro text-ink-mute">Related:</span>
            {related.map((r) => (
              <Link
                key={r.href}
                href={r.href}
                className="rounded-full border border-line px-2.5 py-0.5 text-micro text-ink-dim hover:border-line-accent hover:text-ink"
              >
                {r.label}
              </Link>
            ))}
          </nav>
        )}
      </div>
      {children && <div className="shrink-0">{children}</div>}
    </header>
  );
}

export default PageHeader;
