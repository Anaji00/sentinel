'use client';

/**
 * What a panel shows when its own request failed and it has nothing to draw.
 *
 * Panels that build a list from `data ?? []` used to say "No matching events found"
 * in that case, which is a different claim from "we could not ask". This says the
 * second, names what could not be loaded, and offers a retry. Use it only when
 * `failedWithout(error, data)`: a failed refresh with older data on screen is the
 * page strip's job, not a reason to blank the panel.
 */

import React from 'react';
import { Button } from './Button';

export function PanelError({ what, onRetry, inline = false }: { what: string; onRetry?: () => void; inline?: boolean }) {
  const body = (
    <>
      Could not load {what}.{' '}
      {onRetry && (
        <Button variant="link" type="button" onClick={onRetry} className="underline hover:text-ink">
          Try again
        </Button>
      )}
    </>
  );
  return inline ? (
    <span role="status" className="text-xs text-caution">
      {body}
    </span>
  ) : (
    <div role="status" className="rounded-xl border border-dashed border-caution/30 p-6 text-center text-xs text-caution">
      {body}
    </div>
  );
}

export default PanelError;
