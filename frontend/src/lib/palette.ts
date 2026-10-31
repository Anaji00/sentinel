/**
 * The design tokens, as JavaScript.
 *
 * `globals.css` is the source of truth for colour and it can only be read by
 * CSS. Charts and inline SVG set colour through attributes — `stroke`,
 * `fill`, `stopColor` — which are JS values, so every chart in this app carried
 * its own hex literals: #10b981 for up, #f43f5e for down, #00f2fe for the
 * accent. Measured before this: 222 hex literals against 16 token references.
 *
 * Three different cyans were in use for one meaning. That is the same failure
 * this codebase's backend audit spent four hundred entries on — a value
 * defined in two places drifts — so the fix is the same one: name it once.
 *
 * `PALETTE_HEX` mirrors the dark `:root` in globals.css. `PALETTE` hands out
 * `var(--token, hex)` references instead of the literals: SVG presentation
 * attributes and inline styles both resolve `var()`, so every chart follows the
 * light theme with no edit of its own, and the hex inside each reference still
 * paints if a token were ever missing. `readToken` resolves a computed value
 * for the few places that need a real colour string.
 */

export const PALETTE_HEX = {
  accent: '#8b9dff',
  positive: '#34d399',
  caution: '#fbbf24',
  negative: '#f87171',
  info: '#9cb0c9',
  civic: '#e879f9',

  bgBase: '#0b0f17',
  bgRaised: '#111722',
  bgOverlay: '#182030',
  bgInset: '#0d121b',

  textPrimary: '#eef2f8',
  textSecondary: '#a6b3c6',
  textMuted: '#8391a8',

  borderSubtle: 'rgba(160, 174, 200, 0.13)',
  borderStrong: 'rgba(160, 174, 200, 0.24)',
} as const;

export type PaletteKey = keyof typeof PALETTE_HEX;

const CSS_VAR: Record<PaletteKey, string> = {
  accent: '--accent',
  positive: '--positive',
  caution: '--caution',
  negative: '--negative',
  info: '--info',
  civic: '--civic',
  bgBase: '--bg-base',
  bgRaised: '--bg-raised',
  bgOverlay: '--bg-overlay',
  bgInset: '--bg-inset',
  textPrimary: '--text-primary',
  textSecondary: '--text-secondary',
  textMuted: '--text-muted',
  borderSubtle: '--border-subtle',
  borderStrong: '--border-strong',
};

/** Theme-following colour references, `var(--token, hex)`, for SVG and inline styles. */
export const PALETTE = Object.fromEntries(
  (Object.keys(PALETTE_HEX) as PaletteKey[]).map((k) => [k, `var(${CSS_VAR[k]}, ${PALETTE_HEX[k]})`]),
) as Record<PaletteKey, string>;

/**
 * The live value of a token, or the compiled fallback.
 *
 * Reading the computed style keeps CSS the single source: if `globals.css`
 * changes, charts follow without a second edit. Wrapped because
 * `getComputedStyle` is unavailable during server render and throws in a few
 * sandboxed contexts.
 */
export function readToken(key: PaletteKey): string {
  if (typeof window === 'undefined' || !window.getComputedStyle) return PALETTE_HEX[key];
  try {
    const value = getComputedStyle(document.documentElement).getPropertyValue(CSS_VAR[key]).trim();
    return value || PALETTE_HEX[key];
  } catch {
    return PALETTE_HEX[key];
  }
}

/**
 * Up, down, or neither — the one place a series decides what colour it is.
 *
 * Every chart made this decision inline with its own pair of hexes, so a
 * "positive" green differed by component. Zero is neither: a flat series is
 * not a gain, and colouring it as one is the same overclaim as a warm-up score
 * reported as a measurement.
 */
export function directionColor(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value) || value === 0) {
    return PALETTE.textMuted;
  }
  return value > 0 ? PALETTE.positive : PALETTE.negative;
}
