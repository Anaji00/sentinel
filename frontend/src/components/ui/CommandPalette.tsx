'use client';

/**
 * Global command palette (Cmd/Ctrl-K).
 *
 * The platform had no search of any kind. Reaching a ticker meant knowing which
 * of thirteen pages showed it and then finding it in a list, and there was no
 * way at all to jump to an entity by name -- on a system whose whole subject is
 * named entities and correlations between them.
 *
 * Tickers come from recent tradfi events -- not from /watchlists/equities,
 * which is ANALYST-gated on purpose: the tracked symbol set is deployment
 * configuration, and with open signup a VIEWER is any stranger with an email
 * address (test_open_signup_exposure pins that). The palette therefore indexes
 * what the signed-in user can already see.
 *
 * Fetched once on open, not per keystroke.
 */

import React from 'react';
import { useRouter } from 'next/navigation';
import { Search, CornerDownLeft, LineChart, Compass } from 'lucide-react';
import { fetcher } from '../../lib/api';

type Kind = 'page' | 'ticker';

interface Command {
  id: string;
  label: string;
  hint?: string;
  kind: Kind;
  href: string;
}

const PAGES: Command[] = [
  { id: 'p:/', label: 'Command Center', kind: 'page', href: '/' },
  { id: 'p:/map', label: 'Global Map', kind: 'page', href: '/map' },
  { id: 'p:/charts', label: 'Market Charts', kind: 'page', href: '/charts' },
  { id: 'p:/options', label: 'Options Flow', kind: 'page', href: '/options' },
  { id: 'p:/flow', label: 'Dark Pool & Sweeps', kind: 'page', href: '/flow' },
  { id: 'p:/crypto', label: 'Crypto & Perps', kind: 'page', href: '/crypto' },
  { id: 'p:/macro', label: 'Macro Matrix', kind: 'page', href: '/macro' },
  { id: 'p:/filings', label: 'Filings & 13F', kind: 'page', href: '/filings' },
  { id: 'p:/intelligence', label: 'Intelligence Feed', kind: 'page', href: '/intelligence' },
  { id: 'p:/osint', label: 'OSINT Matrix', kind: 'page', href: '/osint' },
  { id: 'p:/agents', label: 'Agent Swarm', kind: 'page', href: '/agents' },
  { id: 'p:/methodology', label: 'Methodology', kind: 'page', href: '/methodology' },
];

const KIND_ICON: Record<Kind, React.ComponentType<{ className?: string }>> = {
  page: Compass,
  ticker: LineChart,
};

const KIND_LABEL: Record<Kind, string> = {
  page: 'Page',
  ticker: 'Ticker',
};

export function CommandPalette() {
  const router = useRouter();
  const [open, setOpen] = React.useState(false);
  const [query, setQuery] = React.useState('');
  const [cursor, setCursor] = React.useState(0);
  const [dynamic, setDynamic] = React.useState<Command[]>([]);
  const inputRef = React.useRef<HTMLInputElement>(null);

  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        setOpen((prev) => !prev);
      }
      if (e.key === 'Escape') setOpen(false);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  // Loaded when the palette opens, not on every keystroke.
  React.useEffect(() => {
    if (!open || dynamic.length > 0) return;
    let cancelled = false;

    (async () => {
      const found: Command[] = [];
      const seenTickers = new Set<string>();
      try {
        const events = await fetcher('/events/tradfi?limit=120');
        for (const ev of Array.isArray(events) ? events : []) {
          const d = ev?.domain_data || ev?.financial_data || {};
          const ticker = String(d.ticker || ev?.primary_entity_id || '').toUpperCase().trim();
          if (!ticker || seenTickers.has(ticker)) continue;
          seenTickers.add(ticker);
          found.push({
            id: `t:${ticker}`,
            label: ticker,
            hint: ev?.primary_entity_name && ev.primary_entity_name !== ticker
              ? String(ev.primary_entity_name)
              : 'recent activity',
            kind: 'ticker',
            href: `/charts?symbol=${encodeURIComponent(ticker)}`,
          });
        }
      } catch {
        /* the palette still navigates pages without it */
      }
      if (!cancelled) setDynamic(found);
    })();

    return () => {
      cancelled = true;
    };
  }, [open, dynamic.length]);

  React.useEffect(() => {
    if (open) {
      setQuery('');
      setCursor(0);
      // The input mounts in the same commit that sets `open`, so a single
      // requestAnimationFrame can land before it is in the document. Try on the
      // next frame and again on the next task, and stop as soon as it takes.
      const focus = () => inputRef.current?.focus();
      const frame = requestAnimationFrame(focus);
      const timer = setTimeout(focus, 50);
      return () => {
        cancelAnimationFrame(frame);
        clearTimeout(timer);
      };
    }
  }, [open]);

  const results = React.useMemo(() => {
    const all = [...PAGES, ...dynamic];
    const q = query.trim().toLowerCase();
    if (!q) return PAGES;
    const scored = all
      .map((c) => {
        const label = c.label.toLowerCase();
        if (label === q) return { c, score: 0 };
        if (label.startsWith(q)) return { c, score: 1 };
        if (label.includes(q)) return { c, score: 2 };
        return null;
      })
      .filter(Boolean) as Array<{ c: Command; score: number }>;
    scored.sort((a, b) => a.score - b.score || a.c.label.length - b.c.label.length);
    return scored.slice(0, 40).map((s) => s.c);
  }, [query, dynamic]);

  React.useEffect(() => setCursor(0), [query]);

  const go = React.useCallback(
    (cmd: Command) => {
      setOpen(false);
      router.push(cmd.href);
    },
    [router],
  );

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[100] flex items-start justify-center bg-black/70 backdrop-blur-sm pt-[12vh] px-4"
      onClick={() => setOpen(false)}
      role="presentation"
    >
      <div
        className="w-full max-w-xl rounded-xl border border-slate-700 bg-[#0d1017] shadow-2xl overflow-hidden"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
      >
        <div className="flex items-center gap-3 px-4 border-b border-slate-800">
          <Search className="h-4 w-4 text-slate-500 shrink-0" />
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'ArrowDown') {
                e.preventDefault();
                setCursor((c) => Math.min(c + 1, results.length - 1));
              } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                setCursor((c) => Math.max(c - 1, 0));
              } else if (e.key === 'Enter' && results[cursor]) {
                e.preventDefault();
                go(results[cursor]);
              }
            }}
            placeholder="Search pages, tickers, entities…"
            className="flex-1 bg-transparent py-3.5 text-sm text-slate-100 placeholder:text-slate-600 outline-none"
            aria-label="Search"
          />
          <kbd className="text-[10px] text-slate-600 border border-slate-700 rounded px-1.5 py-0.5">ESC</kbd>
        </div>

        <ul className="max-h-[52vh] overflow-y-auto py-1">
          {results.length === 0 && (
            <li className="px-4 py-6 text-center text-xs text-slate-500">
              Nothing matches “{query}”.
            </li>
          )}
          {results.map((cmd, i) => {
            const Icon = KIND_ICON[cmd.kind];
            const active = i === cursor;
            return (
              <li key={cmd.id}>
                <button
                  type="button"
                  onMouseEnter={() => setCursor(i)}
                  onClick={() => go(cmd)}
                  className={`w-full flex items-center gap-3 px-4 py-2 text-left text-sm transition-colors ${
                    active ? 'bg-cyan-500/10 text-cyan-100' : 'text-slate-300 hover:bg-slate-800/50'
                  }`}
                >
                  <Icon className={`h-4 w-4 shrink-0 ${active ? 'text-cyan-300' : 'text-slate-500'}`} />
                  <span className="flex-1 truncate">{cmd.label}</span>
                  {cmd.hint && <span className="text-[10px] text-slate-500 truncate">{cmd.hint}</span>}
                  <span className="text-[10px] uppercase tracking-wide text-slate-600">
                    {KIND_LABEL[cmd.kind]}
                  </span>
                  {active && <CornerDownLeft className="h-3.5 w-3.5 text-slate-500" />}
                </button>
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
}
