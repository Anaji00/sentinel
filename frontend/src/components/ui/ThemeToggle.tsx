'use client';

import React, { useSyncExternalStore } from 'react';
import { Moon, Sun } from 'lucide-react';
import { applyTheme, currentTheme, subscribeTheme, type Theme } from '../../lib/theme';

/** Switches between the dark and light themes. Lives in the header. */
export function ThemeToggle() {
  // Null on the server, which cannot read the stored choice; the document's
  // attribute (set by the boot script before paint) on the client.
  const theme = useSyncExternalStore<Theme | null>(subscribeTheme, currentTheme, () => null);

  const next: Theme = theme === 'light' ? 'dark' : 'light';
  return (
    <button
      type="button"
      onClick={() => applyTheme(next)}
      aria-label={`Switch to ${next} theme`}
      title={`Switch to ${next} theme`}
      className="flex h-8 w-8 items-center justify-center rounded-md text-ink-dim transition-colors
                 hover:bg-overlay/60 hover:text-ink outline-none focus-visible:ring-1 focus-visible:ring-accent/60"
    >
      {theme === 'light' ? <Moon className="h-4 w-4" /> : <Sun className="h-4 w-4" />}
    </button>
  );
}
