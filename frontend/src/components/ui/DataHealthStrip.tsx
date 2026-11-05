'use client';

/**
 * A line at the top of the page when information is not updating.
 *
 * Panels that draw from `data ?? []` look the same whether the server had
 * nothing to say or could not be reached. This is the one place that says which,
 * for the whole page, and offers a retry.
 */

import React from 'react';
import { Button } from './Button';
import { useSWRConfig } from 'swr';
import { useFailingSources } from '../../lib/useDataHealth';

export function DataHealthStrip() {
  const failing = useFailingSources();
  const { mutate } = useSWRConfig();
  if (failing === 0) return null;
  return (
    <div
      role="status"
      className="mb-3 flex flex-wrap items-center gap-x-3 gap-y-1 rounded-lg border border-caution/30 bg-caution/10 px-3 py-2 text-xs text-ink-dim"
    >
      <span>
        <span className="font-semibold text-caution">Some information is not updating.</span> What you see here may be
        out of date.
      </span>
      <Button variant="link" type="button" onClick={() => void mutate(() => true)}>
        Try again
      </Button>
    </div>
  );
}

export default DataHealthStrip;
