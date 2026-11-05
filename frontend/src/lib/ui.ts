/**
 * Class recipes for the shared Button and Input, kept as plain functions so they
 * can be tested and reused without rendering.
 *
 * They come from what the app already did: 194 buttons in 146 hand-written
 * recipes fell into a few families (text links, small outline chips, filled accent
 * buttons, icon buttons), and 41 inputs in 25. Every colour is a theme token, so
 * both themes work, and the global focus ring applies to all of them.
 */

export type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'ghost' | 'link' | 'danger';
export type ButtonSize = 'sm' | 'md' | 'icon';

const BUTTON_BASE = 'transition-colors disabled:cursor-not-allowed disabled:opacity-50';

const VARIANT: Record<ButtonVariant, string> = {
  primary: 'inline-flex items-center justify-center gap-1.5 rounded-md bg-accent font-semibold text-page hover:brightness-110',
  secondary:
    'inline-flex items-center justify-center gap-1.5 rounded-lg border border-line-accent bg-accent-dim font-semibold text-accent enabled:hover:border-accent',
  outline:
    'inline-flex items-center justify-center gap-1.5 rounded-md border border-line font-medium text-ink-dim enabled:hover:border-line-strong enabled:hover:text-ink',
  ghost: 'inline-flex items-center justify-center rounded text-ink-dim hover:text-ink',
  // Sits inside a sentence or a line of small text, so it keeps that text's size and has no padding.
  link: 'text-accent hover:underline',
  danger:
    'inline-flex items-center justify-center gap-1.5 rounded-lg border border-negative/30 font-medium text-negative enabled:hover:bg-negative/10',
};

const SIZE: Record<ButtonSize, string> = {
  sm: 'px-2 py-0.5 text-micro',
  md: 'px-3 py-1.5 text-sm',
  icon: 'p-1.5',
};

export function buttonClasses(variant: ButtonVariant, size: ButtonSize, extra?: string): string {
  const sizeClass = variant === 'link' ? '' : SIZE[size];
  return [BUTTON_BASE, VARIANT[variant], sizeClass, extra].filter(Boolean).join(' ');
}

/** sm and md for dense panels, lg for sign-in style forms. The corner radius goes with the size. */
export type InputSize = 'sm' | 'md' | 'lg';

const INPUT_BASE =
  'w-full border bg-inset text-ink outline-none transition-colors placeholder:text-ink-mute disabled:cursor-not-allowed disabled:opacity-50';
const INPUT_SIZE: Record<InputSize, string> = {
  sm: 'rounded-lg px-2.5 py-1 text-xs',
  md: 'rounded-lg px-3 py-1.5 text-sm',
  lg: 'rounded-xl px-4 py-3 text-sm',
};

export function inputClasses(size: InputSize, invalid: boolean, extra?: string): string {
  const edge = invalid ? 'border-caution focus-visible:border-caution' : 'border-line-strong focus-visible:border-accent';
  return [INPUT_BASE, INPUT_SIZE[size], edge, extra].filter(Boolean).join(' ');
}
