/**
 * Light or dark, per browser.
 *
 * Dark is the default: it is how the platform has always looked, and a
 * dashboard read for hours is easier on the eyes that way. Light is for
 * daylight and for reports someone will export or print. The choice is a
 * `data-theme` attribute on <html>, which `globals.css` keys the light tokens
 * off; nothing else in the application knows which theme is on.
 */

export type Theme = 'dark' | 'light';

export const THEME_KEY = 'sentinel.theme';

/** A stored value as a theme; anything unexpected is the default. */
export function parseTheme(value: string | null | undefined): Theme {
  return value === 'light' ? 'light' : 'dark';
}

/** Sets the theme on the document and remembers it. Storage may be unavailable. */
export function applyTheme(theme: Theme): void {
  const root = document.documentElement;
  root.dataset.theme = theme;
  root.classList.toggle('dark', theme === 'dark');
  try {
    window.localStorage.setItem(THEME_KEY, theme);
  } catch {
    /* private mode: the choice lasts for this page */
  }
  window.dispatchEvent(new Event(THEME_EVENT));
}

/** Fired on window whenever the theme changes, for useSyncExternalStore. */
export const THEME_EVENT = 'sentinel:theme';

export function subscribeTheme(onChange: () => void): () => void {
  window.addEventListener(THEME_EVENT, onChange);
  return () => window.removeEventListener(THEME_EVENT, onChange);
}

/** The theme currently on the document. */
export function currentTheme(): Theme {
  return parseTheme(document.documentElement.dataset.theme);
}

/**
 * Runs in <head> before the first paint, so a light-theme reader never sees a
 * dark frame first. Kept tiny and dependency-free: it is inlined as a string.
 */
export const THEME_BOOT = `(function(){try{var t=localStorage.getItem('${THEME_KEY}')==='light'?'light':'dark';var r=document.documentElement;r.dataset.theme=t;r.classList.toggle('dark',t==='dark');}catch(e){document.documentElement.dataset.theme='dark';}})();`;
