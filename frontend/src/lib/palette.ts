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
 * These mirror `:root` in globals.css. `readToken` resolves the live computed
 * value where the DOM is available, so a theme change is picked up rather than
 * baked in at build time; the literals are the fallback for server rendering
 * and for tests.
 */

export const PALETTE = {
  accent: '#22d3ee',
  positive: '#34d399',
  caution: '#fbbf24',
  negative: '#f87171',
  info: '#a78bfa',

  bgBase: '#07090e',
  bgRaised: '#0c1018',
  bgOverlay: '#11161f',
  bgInset: '#090c12',

  textPrimary: '#e8edf5',
  textSecondary: '#94a3b8',
  textMuted: '#64748b',

  borderSubtle: 'rgba(148, 163, 184, 0.12)',
  borderStrong: 'rgba(148, 163, 184, 0.22)',
} as const;

export type PaletteKey = keyof typeof PALETTE;

const CSS_VAR: Record<PaletteKey, string> = {
  accent: '--accent',
  positive: '--positive',
  caution: '--caution',
  negative: '--negative',
  info: '--info',
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

/**
 * The live value of a token, or the compiled fallback.
 *
 * Reading the computed style keeps CSS the single source: if `globals.css`
 * changes, charts follow without a second edit. Wrapped because
 * `getComputedStyle` is unavailable during server render and throws in a few
 * sandboxed contexts.
 */
export function readToken(key: PaletteKey): string {
  if (typeof window === 'undefined' || !window.getComputedStyle) return PALETTE[key];
  try {
    const value = getComputedStyle(document.documentElement).getPropertyValue(CSS_VAR[key]).trim();
    return value || PALETTE[key];
  } catch {
    return PALETTE[key];
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
