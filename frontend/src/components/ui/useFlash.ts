import { useEffect, useRef, useState } from 'react';

export type Flash = 'up' | 'down' | null;

/** How long a changed figure stays tinted. */
export const FLASH_MS = 900;

/** Which way a figure moved, if it moved: nothing on first sight or when unknown. */
export function flashDirection(prev: number | null | undefined, next: number | null | undefined): Flash {
  if (prev == null || next == null || !Number.isFinite(prev) || !Number.isFinite(next) || prev === next) {
    return null;
  }
  return next > prev ? 'up' : 'down';
}

/** The class for a flash, from globals.css (it does nothing under reduced motion). */
export function flashClass(flash: Flash): string {
  return flash === 'up' ? 'flash-up' : flash === 'down' ? 'flash-down' : '';
}

/**
 * Tints a live figure briefly when it changes, green up and red down, so a
 * price that just moved can be seen moving. The first value never flashes.
 */
export function useFlash(value: number | null | undefined): Flash {
  const prev = useRef(value);
  const [flash, setFlash] = useState<Flash>(null);

  useEffect(() => {
    const dir = flashDirection(prev.current, value);
    prev.current = value;
    if (!dir) return;
    setFlash(dir);
    const t = setTimeout(() => setFlash(null), FLASH_MS);
    return () => clearTimeout(t);
  }, [value]);

  return flash;
}
