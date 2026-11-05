/**
 * Whether a folded section was left open, remembered per browser.
 *
 * A convenience only: storage can be missing, full or blocked (private windows,
 * previews), and then every section simply starts at its default.
 */

export type OpenStore = Pick<Storage, 'getItem' | 'setItem'>;

export const OPEN_PREFIX = 'sentinel:open:';

function browserStore(): OpenStore | null {
  try {
    return typeof window === 'undefined' ? null : window.localStorage;
  } catch {
    return null;
  }
}

export function readOpen(key: string | undefined, fallback: boolean, store: OpenStore | null = browserStore()): boolean {
  if (!key || !store) return fallback;
  try {
    const v = store.getItem(OPEN_PREFIX + key);
    return v === '1' ? true : v === '0' ? false : fallback;
  } catch {
    return fallback;
  }
}

export function writeOpen(key: string | undefined, open: boolean, store: OpenStore | null = browserStore()): void {
  if (!key || !store) return;
  try {
    store.setItem(OPEN_PREFIX + key, open ? '1' : '0');
  } catch {
    /* not remembered; the section still works */
  }
}
