/**
 * Whether the stock drawer's "how to read this" card has been seen. It shows the first time someone opens a stock
 * and not again once they close it. Kept in this browser only; with storage missing or blocked it simply shows
 * each time, which is harmless, and nothing else depends on it.
 */

const KEY = 'sentinel:first-look-seen';

type Store = Pick<Storage, 'getItem' | 'setItem'>;

function browserStore(): Store | null {
  try {
    return typeof window === 'undefined' ? null : window.localStorage;
  } catch {
    return null;
  }
}

export function hasSeenFirstLook(store: Store | null = browserStore()): boolean {
  if (!store) return false;
  try {
    return store.getItem(KEY) === '1';
  } catch {
    return false;
  }
}

export function markFirstLookSeen(store: Store | null = browserStore()): void {
  if (!store) return;
  try {
    store.setItem(KEY, '1');
  } catch {
    /* it will show again next time; nothing is lost */
  }
}

/** The steps, in the order the drawer lays its sections out. */
export const FIRST_LOOK_STEPS: ReadonlyArray<{ title: string; text: string }> = [
  { title: 'At the top', text: 'The price, and what the model expects over the next month. "Top 15% of stocks" means the model ranks it above 85% of others.' },
  { title: 'What changed', text: 'Facts that were measured: price moves, large trades, options and short selling. Nothing here is a guess.' },
  { title: 'Bull and bear case', text: 'A small AI model writes the best reasons for and against, using only what the platform knows. It is optional and takes a minute.' },
  { title: 'More about it', text: 'Who owns it, the facts behind the call with their sources, and related stocks.' },
];
