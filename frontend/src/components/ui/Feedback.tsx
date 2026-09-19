'use client';

/**
 * Confirmation and result feedback for actions that change something.
 *
 * The operations console fired three irreversible writes — replay a dead-letter
 * batch, merge two entities, reject a merge — behind `window.confirm`, and then
 * said nothing at all about whether they worked. `window.confirm` blocks the
 * event loop, cannot be styled, is suppressed by some browsers after repeated
 * use, and is invisible to a screen reader until it steals focus. And a silent
 * POST is the UI version of a swallowed exception: the operator cannot tell a
 * completed replay from one the gateway refused.
 *
 * So: a real dialog that says what it will do, and a toast that says what
 * happened. No dependency — this is a context, a portal-free fixed container
 * and two small components.
 */

import React from 'react';

// ── toasts ──────────────────────────────────────────────────────────────────

type ToastKind = 'success' | 'error' | 'info';

interface Toast {
  id: number;
  kind: ToastKind;
  message: string;
  /** The detail that makes a failure actionable rather than merely announced. */
  detail?: string;
}

interface ConfirmRequest {
  title: string;
  body: string;
  /** Wording on the button that does the thing. Never "OK". */
  confirmLabel: string;
  destructive?: boolean;
}

interface FeedbackState {
  toast: (kind: ToastKind, message: string, detail?: string) => void;
  /** Resolves true if the person confirmed. */
  confirm: (req: ConfirmRequest) => Promise<boolean>;
}

const FeedbackContext = React.createContext<FeedbackState | null>(null);

const TOAST_MS = 6000;

const TOAST_STYLE: Record<ToastKind, string> = {
  success: 'border-positive/40 text-positive',
  error: 'border-negative/40 text-negative',
  info: 'border-line-strong text-ink-dim',
};

export function FeedbackProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = React.useState<Toast[]>([]);
  const [pending, setPending] = React.useState<
    (ConfirmRequest & { resolve: (ok: boolean) => void }) | null
  >(null);
  const nextId = React.useRef(1);

  const toast = React.useCallback((kind: ToastKind, message: string, detail?: string) => {
    const id = nextId.current++;
    setToasts((all) => [...all, { id, kind, message, detail }]);
    setTimeout(() => setToasts((all) => all.filter((t) => t.id !== id)), TOAST_MS);
  }, []);

  const confirm = React.useCallback(
    (req: ConfirmRequest) => new Promise<boolean>((resolve) => setPending({ ...req, resolve })),
    [],
  );

  const settle = React.useCallback(
    (ok: boolean) => {
      pending?.resolve(ok);
      setPending(null);
    },
    [pending],
  );

  // Escape declines. A dialog with no keyboard exit is a trap.
  React.useEffect(() => {
    if (!pending) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') settle(false);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [pending, settle]);

  const value = React.useMemo(() => ({ toast, confirm }), [toast, confirm]);

  return (
    <FeedbackContext.Provider value={value}>
      {children}

      {pending && (
        <div
          className="fixed inset-0 z-[60] flex items-center justify-center bg-black/70 p-4"
          role="dialog"
          aria-modal="true"
          aria-labelledby="confirm-title"
          onClick={(e) => {
            if (e.target === e.currentTarget) settle(false);
          }}
        >
          <div
            className="w-full max-w-md rounded-panel border border-line-strong
                       bg-overlay p-5 shadow-overlay"
          >
            <h2 id="confirm-title" className="text-sm font-semibold text-ink">
              {pending.title}
            </h2>
            <p className="mt-2 text-xs leading-relaxed text-ink-dim">{pending.body}</p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => settle(false)}
                className="rounded-md border border-line px-3 py-1.5 text-xs
                           text-ink-dim transition-colors hover:bg-white/5
                           focus-visible:ring-1 focus-visible:ring-accent outline-none"
              >
                Cancel
              </button>
              <button
                type="button"
                autoFocus
                onClick={() => settle(true)}
                className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors
                            outline-none focus-visible:ring-1 focus-visible:ring-accent ${
                              pending.destructive
                                ? 'bg-negative/15 text-negative border border-negative/40 hover:bg-negative/25'
                                : 'bg-accent-dim text-accent border border-line-accent hover:bg-accent/20'
                            }`}
              >
                {pending.confirmLabel}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Polite, not assertive: a result is worth announcing and not worth
          interrupting whatever the reader is in the middle of. */}
      <div
        className="pointer-events-none fixed bottom-4 right-4 z-[70] flex w-[min(24rem,calc(100vw-2rem))] flex-col gap-2"
        aria-live="polite"
        aria-atomic="false"
      >
        {toasts.map((t) => (
          <div
            key={t.id}
            className={`pointer-events-auto rounded-panel border bg-overlay
                        px-3.5 py-2.5 shadow-overlay ${TOAST_STYLE[t.kind]}`}
          >
            <p className="text-xs font-medium">{t.message}</p>
            {t.detail && (
              <p className="mt-0.5 text-micro leading-relaxed text-ink-mute">{t.detail}</p>
            )}
          </div>
        ))}
      </div>
    </FeedbackContext.Provider>
  );
}

/**
 * Falls back to no-ops rather than throwing.
 *
 * These components render in tests and in isolation, and a missing provider
 * should not take a page down — but the fallback `confirm` returns false, so an
 * irreversible action never proceeds by accident when the dialog cannot be
 * shown.
 */
export function useFeedback(): FeedbackState {
  const ctx = React.useContext(FeedbackContext);
  return ctx ?? { toast: () => {}, confirm: async () => false };
}
