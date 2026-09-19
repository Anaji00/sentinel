'use client';

/**
 * What a dialog owes anyone who is not using a mouse.
 *
 * Measured across the application before this existed: fourteen
 * `fixed inset-0` overlays, two of which declared `role="dialog"`, none of
 * which trapped focus, and `tabIndex` appearing zero times in the entire
 * codebase. Three Escape handlers existed and all three were on non-modal
 * surfaces -- the nav drawer, the command palette, the confirm toast.
 *
 * So for twelve dialogs, a keyboard user could tab out of the open dialog into
 * the page behind it -- which is still there, still focusable, and now
 * invisible under an 80%-black overlay -- with no way to close what they had
 * opened and no way to see where their focus had gone. That is not a polish
 * item; it is a surface a person can enter and not leave.
 *
 * Four things, which is what "modal" actually means:
 *
 *   1. Escape closes it.
 *   2. Focus moves into it when it opens, so the next Tab is inside.
 *   3. Tab and Shift+Tab cycle within it rather than escaping behind it.
 *   4. Focus returns to whatever opened it when it closes, so the operator is
 *      not dropped at the top of the document.
 *
 * Clicking the backdrop closes it too, which is conventional and which the
 * overlays were inconsistent about.
 */

import React from 'react';

/** Everything focusable, in document order, excluding anything disabled. */
const FOCUSABLE = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled]):not([type="hidden"])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

export interface DialogProps {
  /** Spread on the full-screen backdrop element. */
  overlayProps: {
    onMouseDown: (e: React.MouseEvent) => void;
  };
  /** Spread on the dialog panel itself. */
  panelProps: {
    ref: React.RefObject<HTMLDivElement | null>;
    role: 'dialog';
    'aria-modal': true;
    'aria-label'?: string;
    tabIndex: -1;
    onMouseDown: (e: React.MouseEvent) => void;
  };
}

/**
 * @param isOpen   Whether the dialog is showing. Taken as an argument rather
 *                 than inferred from mounting, because every one of these
 *                 overlays is rendered inline as `{isOpen && <div .../>}` --
 *                 the hook has to be called unconditionally from the component
 *                 body, which is not the moment the dialog opens.
 * @param onClose  What closing means for this dialog. Called by Escape and by
 *                 a backdrop click.
 * @param label    The dialog's accessible name. Pass one whenever the visible
 *                 heading is not the first thing inside.
 */
export function useDialog(isOpen: boolean, onClose: () => void, label?: string): DialogProps {
  const panelRef = React.useRef<HTMLDivElement | null>(null);
  const openerRef = React.useRef<Element | null>(null);

  // Captured when it opens, restored when it closes.
  React.useEffect(() => {
    if (!isOpen) return;
    openerRef.current = document.activeElement;

    // The panel is not in the DOM until after this render commits in the same
    // tick, so the query runs against a ref that is already attached.
    const panel = panelRef.current;
    if (panel) {
      const first = panel.querySelector<HTMLElement>(FOCUSABLE);
      // The panel itself is focusable (tabIndex -1), so a dialog with no
      // controls still takes focus rather than leaving it behind the overlay.
      (first ?? panel).focus();
    }

    return () => {
      const opener = openerRef.current;
      if (opener instanceof HTMLElement && document.contains(opener)) opener.focus();
    };
  }, [isOpen]);

  React.useEffect(() => {
    if (!isOpen) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        onClose();
        return;
      }
      if (e.key !== 'Tab') return;

      const panel = panelRef.current;
      if (!panel) return;
      const items = Array.from(panel.querySelectorAll<HTMLElement>(FOCUSABLE)).filter(
        // offsetParent is null for anything display:none, so a collapsed
        // section inside the dialog does not become a focus black hole.
        (el) => el.offsetParent !== null || el === document.activeElement,
      );
      if (items.length === 0) {
        // Nothing to cycle between; keep focus on the panel rather than
        // letting Tab walk into the page underneath.
        e.preventDefault();
        panel.focus();
        return;
      }

      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement;

      if (!e.shiftKey && active === last) {
        e.preventDefault();
        first.focus();
      } else if (e.shiftKey && (active === first || active === panel)) {
        e.preventDefault();
        last.focus();
      } else if (!panel.contains(active)) {
        // Focus was outside to begin with -- a click on the backdrop, or a
        // programmatic move. Pull it back in rather than continuing behind.
        e.preventDefault();
        first.focus();
      }
    };

    document.addEventListener('keydown', onKeyDown, true);
    return () => document.removeEventListener('keydown', onKeyDown, true);
  }, [isOpen, onClose]);

  // mousedown rather than click: a click that starts inside the panel and ends
  // on the backdrop -- selecting text and releasing outside -- would otherwise
  // close the dialog and lose whatever was being read.
  const onOverlayMouseDown = React.useCallback(
    (e: React.MouseEvent) => {
      if (e.target === e.currentTarget) onClose();
    },
    [onClose],
  );

  const stop = React.useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
  }, []);

  return {
    overlayProps: { onMouseDown: onOverlayMouseDown },
    panelProps: {
      ref: panelRef,
      role: 'dialog',
      'aria-modal': true,
      'aria-label': label,
      tabIndex: -1,
      onMouseDown: stop,
    },
  };
}
