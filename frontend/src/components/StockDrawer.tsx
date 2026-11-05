'use client';

/**
 * Any stock, without leaving the page: a panel from the right opened by `?t=`.
 *
 * A ticker used to be a link to /ticker, which threw away the page you were
 * reading. The drawer shows the same header and evidence over the current
 * page, keeps its own address (so it can be shared or bookmarked) and closes
 * back to exactly where you were. "Full detail" holds what the Look Up a Stock
 * page used to: candles, fund holders, options and surveillance business.
 */

import React from 'react';
import { Button } from './ui/Button';
import Link from 'next/link';
import useSWR from 'swr';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { fetcher } from '../lib/api';
import { formatNumber } from '../lib/format';
import { shortName, type HoldersResponse } from '../lib/money';
import { TickerEvidence } from './ConvergingEvidence';
import { TickerHero } from './TickerHero';
import dynamic from 'next/dynamic';
import { Disclosure } from './ui/Disclosure';
import { IconClose } from './ui/icons';
import { LoadingLines } from './ui/Skeleton';
import RelatedStocks from './RelatedStocks';
import ThesisPanel from './ThesisPanel';
import WhatChanged from './WhatChanged';
import TickerSurveillance from './TickerSurveillance';
import { useDialog } from './ui/useDialog';

// Loaded when someone asks for it: the drawer is mounted on every page.
const TickerInspector = dynamic(() => import('./TickerInspector'), {
  ssr: false,
  loading: () => <LoadingLines />,
});

/** The `t` search parameter, cleaned: letters, digits, dot and dash, at most ten. */
export function drawerTicker(raw: string | null | undefined): string {
  return (raw || '').toUpperCase().replace(/[^A-Z0-9.-]/g, '').slice(0, 10);
}

function Owners({ ticker }: { ticker: string }) {
  const { data } = useSWR<HoldersResponse>(`/ownership/holders/${encodeURIComponent(ticker)}`, fetcher);
  const top = (data?.holders ?? []).filter((h) => (h.pct_outstanding ?? 0) > 0).slice(0, 4);
  if (!top.length) return null;
  return (
    <section className="space-y-2 rounded-xl border border-line bg-inset p-4">
      <h3 className="text-xs font-semibold uppercase tracking-[0.08em] text-ink-mute">Who owns it</h3>
      <p className="text-sm text-ink-dim">
        {top.map((h, i) => (
          <span key={h.filer_id}>
            {i > 0 && ', '}
            <span className="text-ink">{shortName(h.filer)}</span>{' '}
            <span className="font-mono">{formatNumber(h.pct_outstanding, { decimals: 1 })}%</span>
          </span>
        ))}
        . BlackRock, Vanguard and State Street together:{' '}
        <span className="font-mono text-civic">{formatNumber(data?.big_three_pct, { decimals: 1 })}%</span>.
      </p>
      <Link href="/money#lookup" className="text-xs text-accent hover:underline">
        Follow the money →
      </Link>
    </section>
  );
}

function Drawer({ ticker, onClose }: { ticker: string; onClose: () => void }) {
  const dialog = useDialog(true, onClose, `${ticker} details`);
  const [detail, setDetail] = React.useState(false);
  return (
    <div className="fixed inset-0 z-50 bg-black/60" {...dialog.overlayProps}>
      <div
        className="absolute inset-y-0 right-0 flex w-full max-w-[560px] flex-col gap-4 overflow-y-auto border-l border-line-strong bg-raised p-5 shadow-overlay"
        {...dialog.panelProps}
      >
        <div className="flex items-center justify-between gap-3">
          <Button variant="link"
            type="button"
            onClick={() => setDetail((v) => !v)}
            aria-expanded={detail} className="text-xs"
          >
            {detail ? 'Hide full detail' : 'Full detail: prices, funds, options'}
          </Button>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="flex h-8 w-8 items-center justify-center rounded-md border border-line text-ink-dim hover:text-ink"
          >
            <IconClose />
          </button>
        </div>
        <TickerHero symbol={ticker} />
        <WhatChanged ticker={ticker} />
        <ThesisPanel ticker={ticker} />
        <Disclosure variant="row" label={`More about ${ticker}`} hint="owners, patterns, related stocks" storageKey="drawer:more">
          <Owners ticker={ticker} />
          <TickerEvidence ticker={ticker} />
          <RelatedStocks ticker={ticker} />
        </Disclosure>
        {detail && (
          <>
            <TickerSurveillance symbol={ticker} />
            <TickerInspector symbol={ticker} embedded />
          </>
        )}
      </div>
    </div>
  );
}

/** Mounted once, in the dashboard layout, under a Suspense boundary. */
export function StockDrawer() {
  const params = useSearchParams();
  const pathname = usePathname();
  const router = useRouter();
  const ticker = drawerTicker(params?.get('t'));
  if (!ticker) return null;
  const close = () => {
    const next = new URLSearchParams(params?.toString() ?? '');
    next.delete('t');
    const q = next.toString();
    router.replace(q ? `${pathname}?${q}` : pathname || '/', { scroll: false });
  };
  return <Drawer key={ticker} ticker={ticker} onClose={close} />;
}

/**
 * A ticker that opens the drawer on the current page instead of navigating
 * away. The href is a real link (open in a new tab still works); a plain click
 * keeps the page's other parameters, such as its view.
 */
export function StockLink({
  ticker,
  className,
  title,
  children,
}: {
  ticker: string;
  className?: string;
  title?: string;
  children?: React.ReactNode;
}) {
  const pathname = usePathname() || '/';
  const router = useRouter();
  const t = drawerTicker(ticker);
  return (
    <Link
      href={`${pathname}?t=${encodeURIComponent(t)}`}
      scroll={false}
      className={className}
      title={title}
      onClick={(e) => {
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
        e.preventDefault();
        const next = new URLSearchParams(window.location.search);
        next.set('t', t);
        router.push(`${pathname}?${next.toString()}`, { scroll: false });
      }}
    >
      {children ?? t}
    </Link>
  );
}

export default StockDrawer;
