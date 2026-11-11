'use client';

/**
 * The second stock being compared with the open one, kept in the address as `?c=` beside `?t=`, so a comparison can be
 * shared and survives a reload. Pinning, swapping and clearing change only those two parameters.
 */

import React from 'react';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { cleanTicker } from './openStock';

export function useCompare() {
  const params = useSearchParams();
  const pathname = usePathname() || '/';
  const router = useRouter();
  const t = cleanTicker(params?.get('t'));
  const raw = cleanTicker(params?.get('c'));
  // Comparing a stock with itself says nothing.
  const other = raw && raw !== t ? raw : '';

  const write = React.useCallback(
    (changes: Record<string, string | null>) => {
      const next = new URLSearchParams(params?.toString() ?? '');
      for (const [k, v] of Object.entries(changes)) {
        if (v) next.set(k, v);
        else next.delete(k);
      }
      const q = next.toString();
      router.replace(q ? `${pathname}?${q}` : pathname, { scroll: false });
    },
    [params, pathname, router],
  );

  return {
    open: t,
    other,
    pin: (ticker: string) => write({ c: cleanTicker(ticker) || null }),
    clear: () => write({ c: null }),
    swap: () => other && t && write({ t: other, c: t }),
  };
}
