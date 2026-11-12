/**
 * Keyboard movement through a list of rows: arrows (or j and k) step, Home and End jump. Pure, so the key map is tested
 * apart from any component. Returns null for a key that does nothing, so the caller leaves the event alone.
 */

export function nextRowIndex(key: string, current: number, count: number): number | null {
  if (count <= 0) return null;
  const last = count - 1;
  const at = Math.min(Math.max(current, 0), last);
  switch (key) {
    case 'ArrowDown':
    case 'j':
      return current < 0 ? 0 : Math.min(at + 1, last);
    case 'ArrowUp':
    case 'k':
      return current < 0 ? last : Math.max(at - 1, 0);
    case 'Home':
      return 0;
    case 'End':
      return last;
    default:
      return null;
  }
}
