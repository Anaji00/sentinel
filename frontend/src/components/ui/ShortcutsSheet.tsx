'use client';

/**
 * The keyboard shortcuts, one key away: press `?` anywhere that is not a text field. The sheet is also
 * reachable from the sidebar for anyone who does not know the key. It lists only shortcuts that exist.
 */

import React from 'react';
import { Keyboard } from 'lucide-react';
import { Button } from './Button';
import { useDialog } from './useDialog';

export const OPEN_SHORTCUTS_EVENT = 'sentinel:open-shortcuts';

const isMac = () => typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);

function typingIn(el: EventTarget | null): boolean {
  return el instanceof HTMLElement && (el.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName));
}

function Sheet({ onClose }: { onClose: () => void }) {
  const dialog = useDialog(true, onClose, 'Keyboard shortcuts');
  const mod = isMac() ? '⌘' : 'Ctrl';
  const rows: Array<[string, string]> = [
    ['/', 'Search for a stock'],
    [`${mod} K`, 'Open the quick finder for pages and stocks'],
    ['Esc', 'Close whatever is open'],
    ['?', 'Show this list'],
  ];
  return (
    <div className="fade-in fixed inset-0 z-[90] flex items-start justify-center bg-black/60 px-4 pt-[14vh]" {...dialog.overlayProps}>
      <div className="drawer-in w-full max-w-sm rounded-md border border-line-strong bg-raised p-5 shadow-overlay" {...dialog.panelProps}>
        <div className="flex items-center justify-between gap-3">
          <h2 className="text-head font-semibold text-ink">Keyboard shortcuts</h2>
          <Button variant="outline" size="sm" onClick={onClose}>
            Close
          </Button>
        </div>
        <dl className="mt-3 divide-y divide-line">
          {rows.map(([key, what]) => (
            <div key={key} className="flex items-center justify-between gap-4 py-2 text-sm">
              <dt>
                <kbd className="whitespace-nowrap rounded border border-line-strong bg-inset px-1.5 py-0.5 font-mono text-micro text-ink">{key}</kbd>
              </dt>
              <dd className="text-right text-ink-dim">{what}</dd>
            </div>
          ))}
        </dl>
      </div>
    </div>
  );
}

export function ShortcutsSheet() {
  const [open, setOpen] = React.useState(false);
  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== '?' || e.ctrlKey || e.metaKey || e.altKey || typingIn(e.target)) return;
      e.preventDefault();
      setOpen((v) => !v);
    };
    const onOpen = () => setOpen(true);
    window.addEventListener('keydown', onKey);
    window.addEventListener(OPEN_SHORTCUTS_EVENT, onOpen);
    return () => {
      window.removeEventListener('keydown', onKey);
      window.removeEventListener(OPEN_SHORTCUTS_EVENT, onOpen);
    };
  }, []);
  return open ? <Sheet onClose={() => setOpen(false)} /> : null;
}

/** A button for the sidebar or anywhere else that opens the sheet. */
export function ShortcutsButton({ className }: { className?: string }) {
  return (
    <Button variant="ghost" size="sm" onClick={() => window.dispatchEvent(new Event(OPEN_SHORTCUTS_EVENT))} className={className} aria-label="Keyboard shortcuts">
      <Keyboard className="h-4 w-4" aria-hidden />
    </Button>
  );
}

export default ShortcutsSheet;
