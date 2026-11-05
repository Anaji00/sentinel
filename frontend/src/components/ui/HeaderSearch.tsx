'use client';

/**
 * The search box in the header, on every page.
 *
 * Type a ticker or a company name; Enter (or a click on a suggestion) opens that stock's
 * drawer over the page you are on, keeping the page's other parameters. `/` focuses the box
 * from anywhere that is not a text field. Ctrl/Cmd+K still opens the full palette, which also
 * finds pages. Below `sm` the box becomes an icon that opens the palette, since the header has no room.
 */

import React, { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { Search } from 'lucide-react';
import { usePathname, useRouter } from 'next/navigation';
import { fetcher } from '../../lib/api';
import { usePreferences } from '../../lib/preferences';
import { stockHref } from '../../lib/openStock';
import { suggest, readRecent, withRecent, writeRecent, type NamedStock, type Suggestion } from '../../lib/stockSearch';
import { Button } from './Button';
import { Input } from './Input';

const KIND_LABEL: Record<Suggestion['kind'], string> = {
  following: 'Following',
  recent: 'Recent',
  match: '',
  typed: 'Open this ticker',
};

/** True when a key press is going into a text field, where `/` must stay a plain character. */
export function isTypingTarget(el: EventTarget | null): boolean {
  if (!(el instanceof HTMLElement)) return false;
  return el.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName);
}

/** Names for the suggestions: loaded once, the first time the box is used. */
function useStockNames(wanted: boolean): NamedStock[] {
  const [names, setNames] = useState<NamedStock[]>([]);
  const started = useRef(false);
  useEffect(() => {
    if (!wanted || started.current) return;
    started.current = true;
    (async () => {
      try {
        const events = await fetcher('/events/tradfi?limit=120');
        const seen = new Set<string>();
        const found: NamedStock[] = [];
        for (const ev of Array.isArray(events) ? events : []) {
          const d = ev?.domain_data || ev?.financial_data || {};
          const ticker = String(d.ticker || ev?.primary_entity_id || '').toUpperCase().trim();
          if (!ticker || seen.has(ticker)) continue;
          seen.add(ticker);
          const name = ev?.primary_entity_name && ev.primary_entity_name !== ticker ? String(ev.primary_entity_name) : null;
          found.push({ ticker, name });
        }
        setNames(found);
      } catch {
        // The box still opens any ticker you type; only the name matching is missing.
      }
    })();
  }, [wanted]);
  return names;
}

export function HeaderSearch(): React.ReactElement {
  const router = useRouter();
  const pathname = usePathname();
  const { prefs } = usePreferences();
  const listId = useId();
  const inputRef = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [cursor, setCursor] = useState(0);
  const [recent, setRecent] = useState<string[]>([]);
  const [touched, setTouched] = useState(false);
  const names = useStockNames(touched);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== '/' || e.ctrlKey || e.metaKey || e.altKey || isTypingTarget(e.target)) return;
      e.preventDefault();
      inputRef.current?.focus();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const results = useMemo(
    () => suggest({ query, followed: prefs?.tickers ?? [], recent, board: names }),
    [query, prefs?.tickers, recent, names],
  );

  const choose = useCallback(
    (ticker: string) => {
      const next = withRecent(recent, ticker);
      setRecent(next);
      writeRecent(next);
      setQuery('');
      setOpen(false);
      inputRef.current?.blur();
      router.push(stockHref(pathname || '/', window.location.search, ticker), { scroll: false });
    },
    [recent, router, pathname],
  );

  const active = open && results.length > 0 ? results[Math.min(cursor, results.length - 1)] : null;

  return (
    <>
      {/* Phones: an icon that opens the palette. */}
      <Button
        variant="ghost"
        size="icon"
        aria-label="Search stocks and pages"
        onClick={() => window.dispatchEvent(new KeyboardEvent('keydown', { key: 'k', ctrlKey: true }))}
        className="sm:hidden"
      >
        <Search className="h-4 w-4" />
      </Button>

      <div className="relative hidden sm:block w-40 md:w-56 lg:w-64">
        <Input
          ref={inputRef}
          label="Search stocks"
          hideLabel
          size="sm"
          type="text"
          role="combobox"
          aria-expanded={open && results.length > 0}
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={active ? `${listId}-${active.ticker}` : undefined}
          autoComplete="off"
          spellCheck={false}
          placeholder="Search stocks   ( / )"
          value={query}
          onFocus={() => {
            setTouched(true);
            setRecent(readRecent());
            setOpen(true);
          }}
          onBlur={() => setOpen(false)}
          onChange={(e) => {
            setQuery(e.target.value);
            setCursor(0);
            setOpen(true);
          }}
          onKeyDown={(e) => {
            if (e.key === 'ArrowDown') {
              e.preventDefault();
              setOpen(true);
              setCursor((c) => Math.min(c + 1, Math.max(results.length - 1, 0)));
            } else if (e.key === 'ArrowUp') {
              e.preventDefault();
              setCursor((c) => Math.max(c - 1, 0));
            } else if (e.key === 'Enter' && active) {
              e.preventDefault();
              choose(active.ticker);
            } else if (e.key === 'Escape') {
              setOpen(false);
              inputRef.current?.blur();
            }
          }}
        />
        {open && results.length > 0 && (
          <ul
            id={listId}
            role="listbox"
            aria-label="Stock suggestions"
            className="absolute left-0 right-0 top-full z-50 mt-1 max-h-80 overflow-auto rounded-lg border border-line-strong bg-inset py-1 shadow-lg"
          >
            {results.map((s, i) => (
              <li
                key={s.ticker}
                id={`${listId}-${s.ticker}`}
                role="option"
                aria-selected={i === cursor}
                // mousedown, not click: the box loses focus first and would close the list under the pointer.
                onMouseDown={(e) => {
                  e.preventDefault();
                  choose(s.ticker);
                }}
                onMouseEnter={() => setCursor(i)}
                className={`flex cursor-pointer items-baseline gap-2 px-3 py-1.5 text-sm ${i === cursor ? 'bg-overlay/60 text-ink' : 'text-ink-dim'}`}
              >
                <span className="font-medium text-ink">{s.ticker}</span>
                {s.name && <span className="min-w-0 flex-1 truncate text-micro text-ink-mute">{s.name}</span>}
                {KIND_LABEL[s.kind] && <span className="ml-auto shrink-0 text-micro text-ink-mute">{KIND_LABEL[s.kind]}</span>}
              </li>
            ))}
          </ul>
        )}
      </div>
    </>
  );
}
