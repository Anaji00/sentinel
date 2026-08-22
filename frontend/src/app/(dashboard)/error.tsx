'use client';

import React, { useEffect } from 'react';
import { AlertTriangle, RotateCw } from 'lucide-react';

/**
 * Route-level error boundary for the dashboard.
 *
 * Must be a client component -- Next.js passes it a reset callback. It contains
 * the failure to the view rather than blanking the shell, and states what
 * happened plainly instead of surfacing a stack trace to an operator.
 *
 * The digest is shown because it is the only handle that ties what the operator
 * saw to what the server logged; the underlying message is deliberately not
 * rendered, since production errors are opaque by design and a raw one is more
 * likely to mislead than to help.
 */
export default function DashboardError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    // Surfaced in the browser console for local diagnosis; the server has the
    // full trace under the same digest.
    console.error('Dashboard view failed to render:', error);
  }, [error]);

  return (
    <div
      role="alert"
      className="flex h-full w-full items-center justify-center p-6"
    >
      <div className="w-full max-w-md rounded-xl border border-rose-500/30 bg-rose-950/20 p-6 font-mono">
        <div className="flex items-center gap-2.5">
          <AlertTriangle className="h-4 w-4 shrink-0 text-rose-400" />
          <h2 className="text-sm font-bold uppercase tracking-wider text-rose-300">
            This view could not be loaded
          </h2>
        </div>

        <p className="mt-3 text-[11px] leading-relaxed text-slate-400">
          The panel failed while rendering. Other views are unaffected. Retrying
          re-runs the request; if it keeps failing, the upstream service is
          likely unavailable.
        </p>

        {error.digest && (
          <p className="mt-3 text-[10px] text-slate-500">
            Reference <code className="text-slate-400">{error.digest}</code>
          </p>
        )}

        <button
          onClick={reset}
          className="mt-4 inline-flex items-center gap-2 rounded-lg border border-rose-500/40 bg-rose-950/40 px-3 py-2 text-[11px] font-bold text-rose-300 transition-colors hover:bg-rose-900/40 focus:outline-none focus:ring-2 focus:ring-rose-400"
        >
          <RotateCw className="h-3.5 w-3.5" />
          RETRY
        </button>
      </div>
    </div>
  );
}
