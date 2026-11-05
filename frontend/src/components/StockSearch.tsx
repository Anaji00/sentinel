'use client';

/**
 * Look up any stock from Calls: type a ticker and the stock drawer opens over
 * the page, with the stocks you follow one click away. This replaces the
 * separate Look Up a Stock page; "Full detail" in the drawer holds what that
 * page had beyond the drawer's first screen.
 */

import React from 'react';
import { usePathname, useRouter } from 'next/navigation';
import { usePreferences } from '../lib/preferences';
import { drawerTicker, StockLink } from './StockDrawer';
import { Button } from './ui/Button';
import { Input } from './ui/Input';

export default function StockSearch() {
  const router = useRouter();
  const pathname = usePathname() || '/calls';
  const { prefs } = usePreferences();
  const [draft, setDraft] = React.useState('');
  const [problem, setProblem] = React.useState<string | null>(null);
  const followed = (prefs?.tickers ?? []).slice(0, 8);

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    const ticker = drawerTicker(draft);
    if (!ticker) {
      setProblem('Type a ticker such as NVDA or BRK.B.');
      return;
    }
    setProblem(null);
    const next = new URLSearchParams(window.location.search);
    next.set('t', ticker);
    router.push(`${pathname}?${next.toString()}`, { scroll: false });
    setDraft('');
  };

  return (
    <section
      aria-label="Look up a stock"
      className="flex flex-wrap items-end gap-x-5 gap-y-2 rounded-xl border border-line bg-raised p-3"
    >
      <form onSubmit={submit} noValidate className="flex flex-wrap items-start gap-2">
        <Input
          label="Look up any stock"
          value={draft}
          onChange={(e) => {
            setDraft(e.target.value);
            if (problem) setProblem(null);
          }}
          maxLength={10}
          placeholder="NVDA"
          autoCapitalize="characters"
          autoComplete="off"
          spellCheck={false}
          error={problem}
          className="w-32 font-mono uppercase"
        />
        <Button type="submit" variant="primary" className="mt-[1.125rem]">
          Look up
        </Button>
      </form>
      {followed.length > 0 && (
        <div className="flex flex-wrap items-center gap-1.5 text-micro text-ink-mute">
          <span>Following</span>
          {followed.map((t) => (
            <StockLink
              key={t}
              ticker={t}
              className="rounded-full border border-line bg-inset px-2.5 py-0.5 font-mono text-ink-dim hover:text-ink"
            />
          ))}
        </div>
      )}
    </section>
  );
}
