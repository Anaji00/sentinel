/**
 * Geometry for the small gap gauge: a bar from -4 to +4 standard deviations with the stock's gap as a dot and the
 * trigger marks at +/-2.5. Pure, so the position of every mark is tested; the component only draws.
 */

export const Z_LIMIT = 4;
export const Z_TRIGGER = 2.5;

/** Where a z-score sits along a bar of `width`, centre-zero, clamped to the ends. */
export function zX(z: number, width: number): number {
  const c = Math.max(-Z_LIMIT, Math.min(Z_LIMIT, z));
  return ((c + Z_LIMIT) / (2 * Z_LIMIT)) * width;
}

/** Strong enough to colour: a gap of 2 standard deviations or more. Everything calmer stays in the quiet tone. */
export const isNotable = (z: number | null | undefined): boolean => typeof z === 'number' && Number.isFinite(z) && Math.abs(z) >= 2;
