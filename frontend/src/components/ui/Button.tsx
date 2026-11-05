'use client';

/**
 * The one button. It defaults to type="button", so one inside a form never submits
 * by accident (118 of the app's 189 buttons had no type at all), and it carries the
 * states a button needs: disabled, busy while something is in flight, and pressed
 * for a toggle.
 *
 *   primary    the main action on a screen
 *   secondary  a quieter filled action
 *   outline    most other controls
 *   ghost      an icon or other chrome-free control
 *   link       text that acts, inside a line of text
 *   danger     something that cannot be undone
 */

import React from 'react';
import { buttonClasses, type ButtonSize, type ButtonVariant } from '../../lib/ui';

export interface ButtonProps extends React.ComponentPropsWithRef<'button'> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  /** Something is in flight: disables the button and says so to assistive tech. */
  loading?: boolean;
  /** Makes it a toggle and announces its state. */
  pressed?: boolean;
}

export function Button({
  variant = 'outline',
  size = 'md',
  loading = false,
  pressed,
  className,
  type = 'button',
  disabled,
  children,
  ...rest
}: ButtonProps) {
  return (
    <button
      type={type}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      aria-pressed={pressed}
      className={buttonClasses(variant, size, className)}
      {...rest}
    >
      {children}
    </button>
  );
}

export default Button;
